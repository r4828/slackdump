# macadmins RAG pipeline

Pulls selected public channels from `macadmins.slack.com` with
[slackdump](https://github.com/rusq/slackdump), stores them as a local SQLite
archive, and exposes them two ways:

1. Live, read-only MCP tools for Claude Code (via `slackdump mcp`).
2. One Markdown file per channel, suitable for a Claude.ai Project upload or
   an Onyx / Danswer File-connector ingestion.

Everything here is downstream tooling; no slackdump source is modified.

## Prerequisites

- `slackdump` v4.x built or installed and on `PATH`
  (from the repo root: `go build ./cmd/slackdump && mv slackdump /usr/local/bin/`).
- `jq` and `python3` on `PATH`.
- A `macadmins.slack.com` account (community workspace; no admin needed).

## One-time setup

```bash
cd macadmins
slackdump workspace new macadmins    # pick "User Browser" when prompted
slackdump workspace list             # verify it's cached
```

Credentials are written to `~/.cache/slackdump/macadmins/` (Linux), encrypted
against the machine ID.

## Step 1 — resolve channel names to IDs

Edit `channel_names.txt` to taste (one channel name per line, `#` comments
allowed), then:

```bash
./build_channel_list.sh
```

This fetches the full channel listing once a day into `channels_all.json`
and writes resolved URLs to `channels.txt`. If any names fail to resolve,
they land in `missing.txt` and the script exits non-zero so you can fix them.

## Step 2 — archive

Text-only, all history the API will return, one session:

```bash
slackdump archive \
  -workspace macadmins \
  -files=false -avatars=false -channel-users \
  -o ./archive \
  @channels.txt
```

Flags explained:

- `-files=false -avatars=false` — skip binary downloads; 10–50× faster and
  a RAG doesn't need attachments.
- `-channel-users` — only fetch user records for participants in the selected
  channels, instead of the whole workspace.
- `@channels.txt` — read channel URLs from the file.

If it's interrupted:

```bash
slackdump resume ./archive
```

Sanity-check in your browser:

```bash
slackdump view ./archive/slackdump.sqlite
# opens http://127.0.0.1:8080
```

## Step 3a — wire MCP into Claude Code

`.mcp.json` in this directory already points Claude Code at
`./archive/slackdump.sqlite`. Open Claude Code in this folder and the
`slackdump-macadmins` server is available with read-only tools:
`list_channels`, `get_channel`, `list_users`, `get_messages`, `get_thread`,
`get_workspace_info`, `load_source`, `command_help`.

If you prefer HTTP transport (e.g. share the archive with a remote agent):

```bash
slackdump mcp -transport http -listen 127.0.0.1:8483 ./archive/slackdump.sqlite
```

## Step 3b — render Markdown for Project / Onyx

```bash
python3 slackdump_to_md.py ./archive/slackdump.sqlite -o ./md
```

Produces `md/<channel>.md` per channel plus `md/_index.md`. Threads are
nested as indented bullets. User IDs and `<#C…>` channel refs are
resolved to readable display names.

Useful options:

```bash
# subset to just a few channels
python3 slackdump_to_md.py ./archive/slackdump.sqlite \
  -o ./md --channel jamf-pro --channel nudge

# drop single-character "noise" messages
python3 slackdump_to_md.py ./archive/slackdump.sqlite \
  -o ./md --min-chars 10
```

Upload `md/*.md` to a Claude.ai Project, or mount them into an Onyx File
connector once your Onyx instance is running.

## Repo hygiene

`.gitignore` keeps the following out of git:

- `archive/` — the SQLite archive and any downloaded binaries
- `md/` — generated Markdown
- `channels_all.json` — cached channel listing
- `channels.txt`, `missing.txt` — regenerated each run

Only the scripts and `channel_names.txt` are version-controlled.

## Notes & gotchas

- macadmins is on the Free tier, so messages older than ~90 days may not be
  returned by the Slack API regardless of `-time-from`.
- `slackdump` only accepts client tokens (`xoxc-…`); bot tokens don't work.
- The `MESSAGE.TXT` column is slackdump's rendered text; raw blocks,
  reactions, files, etc. live in `MESSAGE.DATA` as JSON if you need them.
- If Claude Code can't find `slackdump`, edit `.mcp.json` and replace
  `"command": "slackdump"` with the absolute path to the binary.
