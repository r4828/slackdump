#!/usr/bin/env bash
# Resolve channel_names.txt -> channels.txt (a list of Slack archive URLs that
# slackdump's archive command accepts as positional args).
#
# Prereqs:
#   1. jq on PATH
#   2. slackdump workspace already authenticated for macadmins, e.g.:
#        slackdump workspace new macadmins
#   3. SLACKDUMP_WORKSPACE env var OR pass workspace as first arg
#
# Output:
#   channels_all.<workspace>.json  - full workspace channel listing (cached 24h)
#   channels.txt                   - URLs for every name that resolved, one per line
#   missing.txt                    - names that didn't match any channel (if any)

set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
workspace="${1:-${SLACKDUMP_WORKSPACE:-macadmins}}"
names_file="${here}/channel_names.txt"
# Scope the cache to the workspace so switching workspaces within the
# 24-hour cache window doesn't reuse the previous catalog.
safe_ws="${workspace//[^[:alnum:]._-]/_}"
cache="${here}/channels_all.${safe_ws}.json"
out="${here}/channels.txt"
missing_file="${here}/missing.txt"

command -v jq >/dev/null || { echo "jq is required" >&2; exit 1; }
command -v slackdump >/dev/null || { echo "slackdump is required on PATH" >&2; exit 1; }
command -v python3 >/dev/null || { echo "python3 is required (portable cache age check)" >&2; exit 1; }
[[ -f "$names_file" ]] || { echo "Missing $names_file" >&2; exit 1; }

# Refresh the channel cache if it's missing, empty, or older than 24
# hours. Uses Python because `find -mmin` / `find -mtime` behave
# differently across BSD (macOS) and GNU builds, and POSIX `find` does
# not specify `-mmin` at all. Exit 0 = stale (re-fetch), 1 = fresh.
cache_is_stale() {
  python3 - "$1" <<'PY'
import os, sys, time
p = sys.argv[1]
if not os.path.exists(p) or os.path.getsize(p) == 0:
    sys.exit(0)
if time.time() - os.path.getmtime(p) > 86400:
    sys.exit(0)
sys.exit(1)
PY
}

if cache_is_stale "$cache"; then
  echo "Fetching channel list from workspace '${workspace}' (this can take a minute)..." >&2
  # -y : auto-accept overwrite prompt (no TTY hang)
  # -q : don't also spew the JSON to stdout
  slackdump list channels -workspace "$workspace" -y -q -format JSON -o "$cache"
fi

# Write to a temp file and rename at the end so interrupted runs don't leave
# a half-populated channels.txt on disk.
out_tmp="${out}.tmp"
missing_tmp="${missing_file}.tmp"
trap 'rm -f "$out_tmp" "$missing_tmp"' EXIT
: > "$out_tmp"
: > "$missing_tmp"
resolved=0
missed=0

while IFS= read -r raw; do
  # strip inline comments and whitespace
  name="${raw%%#*}"
  name="${name//[[:space:]]/}"
  [[ -z "$name" ]] && continue

  matches=$(jq -r --arg n "$name" '.[] | select(.name==$n) | .id' "$cache")
  id=$(printf '%s\n' "$matches" | head -n1)
  if [[ -z "$id" ]]; then
    echo "$name" >> "$missing_tmp"
    missed=$((missed + 1))
    continue
  fi
  if [[ $(printf '%s\n' "$matches" | grep -c .) -gt 1 ]]; then
    echo "Warning: channel name '$name' matched multiple IDs; using $id" >&2
  fi
  # slackdump's @file reader only treats lines that START with '#' as
  # comments (internal/structures/entity_list.go:227). Emit the channel
  # name as a separate comment line above each URL so the file is
  # human-readable without confusing the parser.
  printf '# %s\n' "$name" >> "$out_tmp"
  printf 'https://macadmins.slack.com/archives/%s\n' "$id" >> "$out_tmp"
  resolved=$((resolved + 1))
done < "$names_file"

mv "$out_tmp" "$out"
mv "$missing_tmp" "$missing_file"
trap - EXIT

echo "Resolved ${resolved} channel(s) -> $out" >&2
if (( resolved == 0 )); then
  echo "No channels resolved. Not running slackdump archive with an empty" >&2
  echo "@channels.txt would risk a full-workspace scrape. Check" >&2
  echo "channel_names.txt for typos or uncomment at least one entry." >&2
  exit 1
fi
if (( missed > 0 )); then
  echo "Could not resolve ${missed} name(s). See ${missing_file}:" >&2
  sed 's/^/  - /' "$missing_file" >&2
  echo "Fix names in channel_names.txt (they must match Slack's exact channel name) and re-run." >&2
  exit 1
fi
