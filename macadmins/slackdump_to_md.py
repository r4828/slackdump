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
    # Prefer the dedicated NAME column; fall back to parsing the JSON payload
    # in Python for this channel lookup if a future slackdump version stops
    # populating NAME consistently. (The message queries below still rely on
    # SQLite's JSON1 extension for uid/bot/username extraction.)
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
    row: sqlite3.Row,
    users: dict[str, str],
    indent: str,
    min_chars: int,
) -> list[str] | None:
    """Render a message row as Markdown lines, or return None if filtered.

    The length check runs against the rendered text (after mention / channel /
    link rewriting) so --min-chars behaves predictably regardless of how
    Slack's raw <@U…> / <#C…> tokens happen to compare in length.
    """
    raw = (row["TXT"] or "").strip()
    if not raw:
        return None
    rendered = resolve_mentions(raw, users)
    if len(rendered) < min_chars:
        return None
    when = ts_to_iso(row["TS"])
    uid = row["uid"] or row["bot"] or ""
    who = users.get(uid) or row["uname_override"] or uid or "system"
    body = rendered.replace("\n", f"\n{indent}  ")
    return [
        f"{indent}- **{who}** ({when})",
        f"{indent}  {body}",
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
    # slackdump stores thread parents with PARENT_ID set to their own
    # thread timestamp (see dbase/repository/dbmessage.go: NewDBMessage
    # populates ParentID whenever msg.ThreadTimestamp != ""). Two subtle
    # shapes fall out of that:
    #
    # - Normal thread parent (has replies): IS_PARENT = 1, PARENT_ID = ID.
    # - Orphan thread lead (no replies yet, or replies deleted):
    #   IS_PARENT = 0 (structures.IsThreadStart excludes empty threads),
    #   PARENT_ID = ID, LATEST_REPLY = '0000000000.000000'.
    #
    # Both are "top-level" from a reading perspective. Expressing that as
    # IS_PARENT = 1 OR PARENT_ID IS NULL would drop the orphan case, so
    # the filter is "PARENT_ID IS NULL OR PARENT_ID = ID". Replies are
    # any row whose PARENT_ID points at the lead but whose own ID is not
    # the lead's (which excludes the parent's self-reference).
    top_level_sql = """
        SELECT m.ID, m.TS, m.IS_PARENT, m.PARENT_ID, m.THREAD_TS, m.TXT,
               json_extract(m.DATA, '$.user')     AS uid,
               json_extract(m.DATA, '$.bot_id')   AS bot,
               json_extract(m.DATA, '$.username') AS uname_override
        FROM MESSAGE m
        WHERE m.CHANNEL_ID = ?
          AND (m.PARENT_ID IS NULL OR m.PARENT_ID = m.ID)
          AND m.CHUNK_ID = (
              SELECT MAX(m2.CHUNK_ID)
              FROM MESSAGE m2
              WHERE m2.ID = m.ID AND m2.CHANNEL_ID = m.CHANNEL_ID
          )
        ORDER BY m.ID ASC
    """
    replies_sql = """
        SELECT m.ID, m.TS, m.IS_PARENT, m.PARENT_ID, m.THREAD_TS, m.TXT,
               json_extract(m.DATA, '$.user')     AS uid,
               json_extract(m.DATA, '$.bot_id')   AS bot,
               json_extract(m.DATA, '$.username') AS uname_override
        FROM MESSAGE m
        WHERE m.CHANNEL_ID = ?
          AND m.PARENT_ID = ?
          AND m.ID != ?
          AND m.CHUNK_ID = (
              SELECT MAX(m2.CHUNK_ID)
              FROM MESSAGE m2
              WHERE m2.ID = m.ID AND m2.CHANNEL_ID = m.CHANNEL_ID
          )
        ORDER BY m.ID ASC
    """

    lines: list[str] = [f"# #{cname}", ""]
    count = 0
    for parent in con.execute(top_level_sql, (cid,)):
        parent_lines = _format_message(parent, users, "", min_chars)
        if parent_lines is not None:
            lines.extend(parent_lines)
            count += 1
        # Query replies whenever the row is a thread lead (PARENT_ID = ID),
        # including orphan leads. For standalone posts (PARENT_ID IS NULL)
        # there is nothing to look up and we skip the round trip.
        if parent["PARENT_ID"] is not None:
            pid = parent["ID"]
            for reply in con.execute(replies_sql, (cid, pid, pid)):
                reply_lines = _format_message(reply, users, "  ", min_chars)
                if reply_lines is None:
                    continue
                lines.extend(reply_lines)
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
