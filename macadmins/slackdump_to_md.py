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
CHANNEL_REF_RE = re.compile(r"<#([CG][A-Z0-9]+)(?:\|([^>]+))?>")
LINK_RE = re.compile(r"<(https?://[^|>]+)(?:\|([^>]+))?>")


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
    rows = con.execute(
        """
        SELECT c.ID, json_extract(c.DATA, '$.name') AS name
        FROM CHANNEL c
        WHERE c.CHUNK_ID = (
            SELECT MAX(c2.CHUNK_ID) FROM CHANNEL c2 WHERE c2.ID = c.ID
        )
          AND json_extract(c.DATA, '$.name') IS NOT NULL
        ORDER BY name
        """
    )
    chans = [(r["ID"], r["name"]) for r in rows]
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


def render_channel(
    con: sqlite3.Connection,
    cid: str,
    cname: str,
    users: dict[str, str],
    out_dir: pathlib.Path,
    min_chars: int,
) -> int:
    rows = con.execute(
        """
        SELECT TS,
               PARENT_ID,
               THREAD_TS,
               IS_PARENT,
               TXT,
               json_extract(DATA, '$.user')     AS uid,
               json_extract(DATA, '$.bot_id')   AS bot,
               json_extract(DATA, '$.username') AS uname_override
        FROM MESSAGE
        WHERE CHANNEL_ID = ?
        ORDER BY CAST(TS AS REAL) ASC
        """,
        (cid,),
    )
    lines: list[str] = [f"# #{cname}", ""]
    count = 0
    for row in rows:
        text = (row["TXT"] or "").strip()
        if len(text) < min_chars:
            continue
        text = resolve_mentions(text, users)
        when = ts_to_iso(row["TS"])
        uid = row["uid"] or row["bot"] or ""
        who = (
            users.get(uid)
            or row["uname_override"]
            or uid
            or "system"
        )
        is_reply = not row["IS_PARENT"] and row["PARENT_ID"] is not None
        indent = "  " if is_reply else ""
        text = text.replace("\n", f"\n{indent}  ")
        lines.append(f"{indent}- **{who}** ({when})")
        lines.append(f"{indent}  {text}")
        lines.append("")
        count += 1
    (out_dir / f"{cname}.md").write_text("\n".join(lines), encoding="utf-8")
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
            index_lines.append(f"| [#{cname}]({cname}.md) | {n} |")
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
