#!/usr/bin/env python3
"""Convert a slackdump SQLite archive to per-channel Markdown files.

Each output file is a single channel rendered as a linear Markdown transcript
with threads nested as indented bullets underneath their parent message.

Typical use:

    python3 slackdump_to_md.py ./archive/slackdump.sqlite -o ./md

The output directory will contain one ``<channel-name>.md`` file per channel
found in the archive, plus an ``_index.md`` summarising counts.
"""
from __future__ import annotations

import argparse
import json
import pathlib
import re
import sqlite3
import sys
from datetime import datetime, timezone


MENTION_RE = re.compile(r"<@([UW][A-Z0-9]+)>")
CHANNEL_REF_RE = re.compile(r"<#([CGDW][A-Z0-9]+)(?:\|([^>]+))?>")
LINK_RE = re.compile(r"<(https?://[^|>]+)(?:\|([^>]+))?>")
UNSAFE_FILENAME_RE = re.compile(r"[^\w.-]+")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("db", type=pathlib.Path, help="Path to slackdump.sqlite")
    p.add_argument(
        "-o",
        "--output",
        type=pathlib.Path,
        default=pathlib.Path("md"),
        help="Output directory (default: ./md)",
    )
    p.add_argument(
        "--channel",
        action="append",
        default=[],
        help="Limit to this channel name. Repeatable. Omit to export all.",
    )
    p.add_argument(
        "--min-chars",
        type=int,
        default=0,
        help="Skip messages with rendered text shorter than this many chars.",
    )
    return p.parse_args()


def ts_to_iso(ts: str | None) -> str:
    if not ts:
        return ""
    try:
        return datetime.fromtimestamp(float(ts), tz=timezone.utc).isoformat()
    except (TypeError, ValueError):
        return ts


def load_users(con: sqlite3.Connection) -> dict[str, str]:
    users: dict[str, str] = {}
    query = """
        SELECT s.ID, s.USERNAME, s.DATA
        FROM S_USER s
        WHERE s.CHUNK_ID = (
            SELECT MAX(s2.CHUNK_ID) FROM S_USER s2 WHERE s2.ID = s.ID
        )
    """
    for row in con.execute(query):
        uid = row["ID"]
        display = row["USERNAME"]
        data = row["DATA"]
        if data:
            try:
                parsed = json.loads(data)
                profile = parsed.get("profile") or {}
                display = (
                    profile.get("display_name")
                    or profile.get("real_name")
                    or parsed.get("real_name")
                    or parsed.get("name")
                    or display
                    or uid
                )
            except (json.JSONDecodeError, TypeError):
                pass
        users[uid] = (display or uid).strip()
    return users


def load_channels(
    con: sqlite3.Connection, wanted: list[str]
) -> list[tuple[str, str]]:
    # Prefer the dedicated NAME column; fall back to the JSON payload so the
    # script keeps working if a future slackdump version stops populating it
    # or on SQLite builds without the JSON1 extension (json_extract is only
    # used as a fallback inside Python).
    rows = con.execute(
        """
        SELECT c.ID, c.NAME, c.DATA
        FROM CHANNEL c
        WHERE c.CHUNK_ID = (
            SELECT MAX(c2.CHUNK_ID) FROM CHANNEL c2 WHERE c2.ID = c.ID
        )
        ORDER BY c.NAME, c.ID
        """
    )
    chans: list[tuple[str, str]] = []
    for row in rows:
        name = row["NAME"]
        if not name and row["DATA"]:
            try:
                parsed = json.loads(row["DATA"])
                name = parsed.get("name")
            except (json.JSONDecodeError, TypeError):
                name = None
        if not name:
            continue
        chans.append((row["ID"], name))
    if wanted:
        wanted_set = set(wanted)
        chans = [c for c in chans if c[1] in wanted_set]
    return chans


def resolve_mentions(text: str, users: dict[str, str]) -> str:
    def sub_user(match: re.Match[str]) -> str:
        uid = match.group(1)
        return "@" + users.get(uid, uid)

    def sub_channel(match: re.Match[str]) -> str:
        name = match.group(2) or match.group(1)
        return "#" + name

    def sub_link(match: re.Match[str]) -> str:
        url, label = match.group(1), match.group(2)
        if label:
            return f"[{label}]({url})"
        return url

    text = MENTION_RE.sub(sub_user, text)
    text = CHANNEL_REF_RE.sub(sub_channel, text)
    text = LINK_RE.sub(sub_link, text)
    return text


def _format_message(
    row: sqlite3.Row, users: dict[str, str], indent: str
) -> list[str]:
    text = resolve_mentions((row["TXT"] or "").strip(), users)
    when = ts_to_iso(row["TS"])
    uid = row["uid"] or row["bot"] or ""
    who = users.get(uid) or row["uname_override"] or uid or "system"
    text = text.replace("\n", f"\n{indent}  ")
    return [
        f"{indent}- **{who}** ({when})",
        f"{indent}  {text}",
        "",
    ]


def render_channel(
    con: sqlite3.Connection,
    cid: str,
    cname: str,
    users: dict[str, str],
    out_dir: pathlib.Path,
    min_chars: int,
) -> int:
    # MESSAGE has composite PK (ID, CHUNK_ID); the same logical message can
    # exist in multiple chunks after resume / thread-fetch. Dedupe to the
    # latest chunk per (CHANNEL_ID, ID) so we don't emit duplicates.
    #
    # Threads are grouped: we first pull top-level messages (thread parents
    # and standalone posts) in timestamp order, then for each parent we pull
    # its replies (also deduped) and write them immediately beneath. This
    # keeps each conversation intact even when replies arrive days after the
    # parent.
    top_level_sql = """
        SELECT m.ID, m.TS, m.IS_PARENT, m.PARENT_ID, m.THREAD_TS, m.TXT,
               json_extract(m.DATA, '$.user')     AS uid,
               json_extract(m.DATA, '$.bot_id')   AS bot,
               json_extract(m.DATA, '$.username') AS uname_override
        FROM MESSAGE m
        WHERE m.CHANNEL_ID = ?
          AND m.PARENT_ID IS NULL
          AND m.CHUNK_ID = (
              SELECT MAX(m2.CHUNK_ID)
              FROM MESSAGE m2
              WHERE m2.ID = m.ID AND m2.CHANNEL_ID = m.CHANNEL_ID
          )
        ORDER BY CAST(m.TS AS REAL) ASC
    """
    replies_sql = """
        SELECT m.ID, m.TS, m.IS_PARENT, m.PARENT_ID, m.THREAD_TS, m.TXT,
               json_extract(m.DATA, '$.user')     AS uid,
               json_extract(m.DATA, '$.bot_id')   AS bot,
               json_extract(m.DATA, '$.username') AS uname_override
        FROM MESSAGE m
        WHERE m.CHANNEL_ID = ?
          AND m.PARENT_ID = ?
          AND m.CHUNK_ID = (
              SELECT MAX(m2.CHUNK_ID)
              FROM MESSAGE m2
              WHERE m2.ID = m.ID AND m2.CHANNEL_ID = m.CHANNEL_ID
          )
        ORDER BY CAST(m.TS AS REAL) ASC
    """

    lines: list[str] = [f"# #{cname}", ""]
    count = 0
    for parent in con.execute(top_level_sql, (cid,)):
        text = (parent["TXT"] or "").strip()
        render_parent = bool(text) and len(text) >= min_chars
        if render_parent:
            lines.extend(_format_message(parent, users, ""))
            count += 1
        if parent["IS_PARENT"]:
            reply_cur = con.execute(replies_sql, (cid, parent["ID"]))
            for reply in reply_cur:
                rtext = (reply["TXT"] or "").strip()
                if not rtext or len(rtext) < min_chars:
                    continue
                lines.extend(_format_message(reply, users, "  "))
                count += 1

    safe_name = UNSAFE_FILENAME_RE.sub("_", cname) or "channel"
    (out_dir / f"{safe_name}.md").write_text(
        "\n".join(lines), encoding="utf-8"
    )
    return count


def main() -> int:
    args = parse_args()
    if not args.db.exists():
        print(f"Database not found: {args.db}", file=sys.stderr)
        return 1
    args.output.mkdir(parents=True, exist_ok=True)

    con = sqlite3.connect(args.db)
    con.row_factory = sqlite3.Row
    try:
        users = load_users(con)
        chans = load_channels(con, args.channel)
        if not chans:
            print("No matching channels in the archive.", file=sys.stderr)
            return 2
        summary: list[tuple[str, int]] = []
        for cid, cname in chans:
            n = render_channel(
                con, cid, cname, users, args.output, args.min_chars
            )
            summary.append((cname, n))
            print(f"  {cname:30s}  {n:>7d} messages")
        index_lines = [
            "# macadmins archive index",
            "",
            f"Generated: {datetime.now(tz=timezone.utc).isoformat()}",
            "",
            "| Channel | Messages |",
            "| --- | ---: |",
        ]
        for cname, n in summary:
            safe = UNSAFE_FILENAME_RE.sub("_", cname) or "channel"
            index_lines.append(f"| [#{cname}]({safe}.md) | {n} |")
        (args.output / "_index.md").write_text(
            "\n".join(index_lines) + "\n", encoding="utf-8"
        )
        total = sum(n for _, n in summary)
        print(
            f"Wrote {len(summary)} channel file(s) + _index.md "
            f"to {args.output} ({total} messages total)"
        )
    finally:
        con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
