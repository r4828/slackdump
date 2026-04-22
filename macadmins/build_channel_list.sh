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
#   channels_all.json  - full workspace channel listing (cached 24h)
#   channels.txt       - URLs for every name that resolved, one per line
#   missing.txt        - names that didn't match any channel (if any)

set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
workspace="${1:-${SLACKDUMP_WORKSPACE:-macadmins}}"
names_file="${here}/channel_names.txt"
cache="${here}/channels_all.json"
out="${here}/channels.txt"
missing_file="${here}/missing.txt"

command -v jq >/dev/null || { echo "jq is required" >&2; exit 1; }
command -v slackdump >/dev/null || { echo "slackdump is required on PATH" >&2; exit 1; }
[[ -f "$names_file" ]] || { echo "Missing $names_file" >&2; exit 1; }

# Refresh the channel cache once a day.
# -y   : auto-accept overwrite prompt (no TTY hang)
# -q   : don't also spew the JSON to stdout
if [[ ! -s "$cache" ]] || [[ -n "$(find "$cache" -mtime +1 -print 2>/dev/null)" ]]; then
  echo "Fetching channel list from workspace '${workspace}' (this can take a minute)..." >&2
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
  printf 'https://macadmins.slack.com/archives/%s  # %s\n' "$id" "$name" >> "$out_tmp"
  resolved=$((resolved + 1))
done < "$names_file"

mv "$out_tmp" "$out"
mv "$missing_tmp" "$missing_file"
trap - EXIT

echo "Resolved ${resolved} channel(s) -> $out" >&2
if (( missed > 0 )); then
  echo "Could not resolve ${missed} name(s). See ${missing_file}:" >&2
  sed 's/^/  - /' "$missing_file" >&2
  echo "Fix names in channel_names.txt (they must match Slack's exact channel name) and re-run." >&2
  exit 1
fi
