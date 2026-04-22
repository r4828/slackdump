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
from collections import Counter
from datetime import datetime, timezone


MENTION_RE = re.compile(r"<@([UW][A-Z0-9]+)>")
CHANNEL_REF_RE = re.compile(r"<#([CGDW][A-Z0-9]+)(?:\|([^>]+))?>")
LINK_RE = re.compile(r"<(https?://[^|>]+)(?:\|([^>]+))?>")
UNSAFE_FILENAME_RE = re.compile(r"[^\w.-]+")

# Inline Markdown metacharacters that can break rendering if they appear
# verbatim in user-supplied text (Slack display names, etc.). Keeping
# this narrow: we don't touch '#', '-', '+', '.', '!', or '|' which are
# only meaningful at the start of a line or inside tables.
_MD_INLINE_SPECIAL_RE = re.compile(r"([\\`*_{}\[\]()<>])")


def _escape_md(text: str) -> str:
    """Backslash-escape inline Markdown metacharacters in free-form text."""
    return _MD_INLINE_SPECIAL_RE.sub(r"\\\1", text)


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
    """Format a Slack timestamp as UTC ISO-8601.

    Slack timestamps are strings like '1700000000.123456' — 16 significant
    digits, which exceeds IEEE-754 double precision. Going through float()
    would drop sub-microsecond precision and (for very large values) shift
    the microsecond field itself. Parse the seconds and the fractional
    part from the string directly so the output is byte-exact.
    """
    if not ts:
        return ""
    try:
        ts_str = ts.strip()
        if not ts_str:
            return ""
        seconds_str, dot, fraction_str = ts_str.partition(".")
        seconds = int(seconds_str)
        microseconds = 0
        if dot:
            if not fraction_str.isdigit():
                return ts
            microseconds = int(fraction_str[:6].ljust(6, "0"))
        return (
            datetime.fromtimestamp(seconds, tz=timezone.utc)
            .replace(microsecond=microseconds)
            .isoformat()
        )
    except (TypeError, ValueError, OverflowError):
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
        return "@" + _escape_md(users.get(uid, uid))

    def sub_channel(match: re.Match[str]) -> str:
        name = match.group(2) or match.group(1)
        return "#" + _escape_md(name)

    def sub_link(match: re.Match[str]) -> str:
        url, label = match.group(1), match.group(2)
        # Wrap the URL in angle brackets so embedded parens can't close
        # the Markdown link-destination group early (CommonMark's <...>
        # destination form treats () as literal).
        if label:
            return f"[{_escape_md(label)}](<{url}>)"
        return f"<{url}>"

    text = MENTION_RE.sub(sub_user, text)
    text = CHANNEL_REF_RE.sub(sub_channel, text)
    text = LINK_RE.sub(sub_link, text)
    return text


def _pid_to_iso(pid: int) -> str:
    """Reverse fasttime.TS2int (dot-stripped int64) back to ISO-8601."""
    s = str(pid)
    if len(s) > 6:
        ts = f"{s[:-6]}.{s[-6:]}"
    else:
        ts = f"0.{s.zfill(6)}"
    return ts_to_iso(ts)


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
    who = _escape_md(
        users.get(uid) or row["uname_override"] or uid or "system"
    )
    body = rendered.replace("\n", f"\n{indent}  ")
    # Trailing separator carries the list-item indent + 2 spaces so the
    # blank keeps the current list item open in CommonMark. A truly
    # unindented blank would terminate the item, and the next indented
    # reply line ("  - ...") could then be re-parsed as a new top-level
    # list (CommonMark allows up to 3 leading spaces on top-level items).
    return [
        f"{indent}- **{who}** ({when})",
        f"{indent}  {body}",
        f"{indent}  ",
    ]


def render_channel(
    con: sqlite3.Connection,
    cid: str,
    cname: str,
    stem: str,
    users: dict[str, str],
    out_dir: pathlib.Path,
    min_chars: int,
) -> int:
    # Single query pulls every deduped message for the channel; we group
    # in Python to avoid an N+1 roundtrip per thread parent.
    #
    # MESSAGE has composite PK (ID, CHUNK_ID); the same logical message
    # can exist in multiple chunks after resume / thread-fetch, so we
    # filter to MAX(CHUNK_ID) per (CHANNEL_ID, ID).
    #
    # slackdump stores thread parents with PARENT_ID set to their own
    # thread timestamp (see dbase/repository/dbmessage.go: NewDBMessage
    # populates ParentID whenever msg.ThreadTimestamp != ""). Two subtle
    # shapes fall out of that:
    #
    #   - Normal thread parent (has replies): IS_PARENT = 1, PARENT_ID = ID.
    #   - Orphan thread lead (no replies yet, or replies deleted):
    #     IS_PARENT = 0 (structures.IsThreadStart excludes empty threads),
    #     PARENT_ID = ID, LATEST_REPLY = '0000000000.000000'.
    #
    # Both count as "top-level" for rendering. The grouping logic below
    # treats a row as top-level when PARENT_ID IS NULL OR PARENT_ID == ID,
    # and as a reply otherwise (bucketed by PARENT_ID). Replies whose
    # parent is not in the top-level set (deleted root, retention
    # boundary, thread_broadcast without its root) surface in a separate
    # orphan section at the end.
    all_sql = """
        SELECT m.ID, m.TS, m.IS_PARENT, m.PARENT_ID, m.THREAD_TS, m.TXT,
               json_extract(m.DATA, '$.user')     AS uid,
               json_extract(m.DATA, '$.bot_id')   AS bot,
               json_extract(m.DATA, '$.username') AS uname_override
        FROM MESSAGE m
        WHERE m.CHANNEL_ID = ?
          AND m.CHUNK_ID = (
              SELECT MAX(m2.CHUNK_ID)
              FROM MESSAGE m2
              WHERE m2.ID = m.ID AND m2.CHANNEL_ID = m.CHANNEL_ID
          )
        ORDER BY m.ID ASC
    """

    top_level_rows: list[sqlite3.Row] = []
    replies_by_parent: dict[int, list[sqlite3.Row]] = {}
    for row in con.execute(all_sql, (cid,)):
        pid = row["PARENT_ID"]
        rid = row["ID"]
        if pid is None or pid == rid:
            top_level_rows.append(row)
        else:
            replies_by_parent.setdefault(pid, []).append(row)

    lines: list[str] = [f"# #{cname}", ""]
    count = 0
    for parent in top_level_rows:
        parent_lines = _format_message(parent, users, "", min_chars)
        parent_rendered = parent_lines is not None
        if parent_rendered:
            lines.extend(parent_lines)
            count += 1
        reply_rows = replies_by_parent.pop(parent["ID"], [])
        reply_lines_buf: list[str] = []
        for reply in reply_rows:
            formatted = _format_message(reply, users, "  ", min_chars)
            if formatted is None:
                continue
            reply_lines_buf.extend(formatted)
            count += 1
        if not reply_lines_buf:
            continue
        if not parent_rendered:
            # Parent was filtered (empty TXT or below --min-chars) but we
            # have replies worth keeping. Emit a placeholder parent bullet
            # so the indented replies have a valid list ancestor; without
            # it some Markdown renderers treat the replies as orphaned
            # code / continuation blocks.
            when = ts_to_iso(parent["TS"])
            lines.append(
                f"- _parent message ({when}) has no displayable text_"
            )
        lines.extend(reply_lines_buf)

    # Anything left in replies_by_parent has a PARENT_ID that does not
    # map to a top-level row in this archive. Render those under their
    # own heading so they aren't silently dropped.
    orphan_section: list[str] = []
    for orphan_pid in sorted(replies_by_parent):
        group: list[str] = []
        for reply in replies_by_parent[orphan_pid]:
            formatted = _format_message(reply, users, "  ", min_chars)
            if formatted is None:
                continue
            group.extend(formatted)
            count += 1
        if group:
            orphan_section.append(
                f"- _parent message ({_pid_to_iso(orphan_pid)}) "
                "is not in this archive_"
            )
            orphan_section.extend(group)
    if orphan_section:
        lines.append("## Orphan thread replies")
        lines.append("")
        lines.extend(orphan_section)

    (out_dir / f"{stem}.md").write_text(
        "\n".join(lines), encoding="utf-8"
    )
    return count


def _filename_stems(chans: list[tuple[str, str]]) -> dict[str, str]:
    """Map each channel ID to a disambiguated filename stem.

    Slack allows repeated channel names (archived-and-recreated, renames,
    etc.). Naming every output file after the channel name alone would
    let later channels clobber earlier ones at the same path and make
    _index.md point multiple rows at one file. When a name collides we
    suffix each colliding file with '--<channel-id>'; single-occurrence
    names stay clean.
    """
    counts = Counter(name for _, name in chans)
    stems: dict[str, str] = {}
    for cid, cname in chans:
        safe = UNSAFE_FILENAME_RE.sub("_", cname) or "channel"
        if counts[cname] > 1:
            safe = f"{safe}--{cid}"
        stems[cid] = safe
    return stems


def main() -> int:
    args = parse_args()
    if not args.db.exists():
        print(f"Database not found: {args.db}", file=sys.stderr)
        return 1
    try:
        args.output.mkdir(parents=True, exist_ok=True)
    except OSError as err:
        print(
            f"Unable to create output directory {args.output}: {err}",
            file=sys.stderr,
        )
        return 1

    # Open the archive read-only: this script never writes to the DB,
    # and mode=ro prevents SQLite from taking a write lock or creating
    # journal files next to slackdump.sqlite (which could race with a
    # concurrent slackdump mcp / slackdump view process).
    db_uri = f"{args.db.resolve().as_uri()}?mode=ro"
    try:
        con = sqlite3.connect(db_uri, uri=True)
    except sqlite3.OperationalError as err:
        print(
            f"Failed to open database read-only: {args.db} ({err})",
            file=sys.stderr,
        )
        return 1
    con.row_factory = sqlite3.Row
    try:
        users = load_users(con)
        chans = load_channels(con, args.channel)
        if not chans:
            print("No matching channels in the archive.", file=sys.stderr)
            return 2
        stems = _filename_stems(chans)
        summary: list[tuple[str, str, int]] = []
        for cid, cname in chans:
            stem = stems[cid]
            n = render_channel(
                con, cid, cname, stem, users, args.output, args.min_chars
            )
            summary.append((cname, stem, n))
            print(f"  {cname:30s}  {n:>7d} messages")
        index_lines = [
            "# macadmins archive index",
            "",
            f"Generated: {datetime.now(tz=timezone.utc).isoformat()}",
            "",
            "| Channel | Messages |",
            "| --- | ---: |",
        ]
        for cname, stem, n in summary:
            # Escape the display text: cname can contain inline Markdown
            # metacharacters, and an unescaped '|' would break the table
            # row (Slack disallows '|' in names, but guard defensively).
            safe_cname = _escape_md(cname).replace("|", "\\|")
            index_lines.append(
                f"| [#{safe_cname}]({stem}.md) | {n} |"
            )
        (args.output / "_index.md").write_text(
            "\n".join(index_lines) + "\n", encoding="utf-8"
        )
        total = sum(n for _, _, n in summary)
        print(
            f"Wrote {len(summary)} channel file(s) + _index.md "
            f"to {args.output} ({total} messages total)"
        )
    finally:
        con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
