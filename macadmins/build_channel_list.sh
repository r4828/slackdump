#!/usr/bin/env bash
# Resolve channel_names.txt -> channels.txt (a list of Slack archive URLs that
# slackdump's archive command accepts as positional args).
#
# channel_names.txt accepts either:
#   - a channel name (e.g. `jamf-pro`), which gets looked up in the
#     cached workspace listing, or
#   - a raw channel / group / DM ID (e.g. `C01234ABCDE`), which is
#     passed through as-is. Useful when a name has multiple historical
#     matches and the script refuses to pick one for you.
#
# Prereqs:
#   1. jq and python3 on PATH
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

# Refresh the channel cache if it's missing, empty, older than 24
# hours, or has malformed / truncated JSON. Uses Python because
# `find -mmin` / `find -mtime` behave differently across BSD (macOS)
# and GNU builds, and POSIX `find` does not specify `-mmin` at all.
# The JSON integrity check repairs caches left corrupt by an
# interrupted `slackdump list channels` call (before the atomic
# temp-file swap was added below).
# Exit 0 = stale (re-fetch), 1 = fresh.
cache_is_stale() {
  python3 - "$1" <<'PY'
import json, os, sys, time
p = sys.argv[1]
try:
    if not os.path.exists(p) or os.path.getsize(p) == 0:
        sys.exit(0)
    if time.time() - os.path.getmtime(p) > 86400:
        sys.exit(0)
    with open(p, encoding="utf-8") as fh:
        data = json.load(fh)
    if not isinstance(data, list):
        sys.exit(0)
except (OSError, ValueError):
    sys.exit(0)
sys.exit(1)
PY
}

if cache_is_stale "$cache"; then
  echo "Fetching channel list from workspace '${workspace}' (this can take a minute)..." >&2
  # Write to a sibling temp path and rename on success so an aborted
  # fetch doesn't leave truncated JSON at the canonical cache path.
  cache_tmp="${cache}.new.$$"
  trap 'rm -f "$cache_tmp"' EXIT
  # `slackdump list channels` has no -o flag (OmitOutputFlag stays set
  # for this subcommand in cmd/slackdump/internal/list/common.go:38),
  # and the positional [filename] from its usage string isn't wired up
  # in runListChannels either. The portable way to capture the JSON at
  # a specific path is:
  #   -no-json : misleadingly named "nosave"; skips the auto-save to
  #              CWD and leaves printing-to-stdout as the sole output
  #   -format JSON : array of slack.Channel encoded via json.Encoder
  #   -y : auto-accept any overwrite/confirm prompt (belt-and-braces)
  # Informational logs go to stderr, so stdout carries only the JSON.
  slackdump list channels -workspace "$workspace" -y -format JSON -no-json > "$cache_tmp"
  mv "$cache_tmp" "$cache"
  trap - EXIT
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

# Slack channel / group / DM IDs. Slack forbids uppercase letters in
# channel names, so a token matching this regex is unambiguously an ID
# the user pasted directly (useful for disambiguating repeated names).
id_regex='^[CGD][A-Z0-9]+$'

# `|| [[ -n "${raw-}" ]]` so a file missing its terminating newline
# still has its last line processed. `read` returns 1 on EOF but has
# already populated $raw with the partial line. The `${raw-}` default
# expansion keeps this safe under `set -u` when the file is empty and
# `read` never assigns $raw at all.
while IFS= read -r raw || [[ -n "${raw-}" ]]; do
  # strip inline comments and whitespace
  name="${raw%%#*}"
  name="${name//[[:space:]]/}"
  [[ -z "$name" ]] && continue

  # Pass raw channel IDs through unchanged.
  if [[ "$name" =~ $id_regex ]]; then
    printf '# %s\n' "$name" >> "$out_tmp"
    printf 'https://macadmins.slack.com/archives/%s\n' "$name" >> "$out_tmp"
    resolved=$((resolved + 1))
    continue
  fi

  matches=$(jq -r --arg n "$name" '.[] | select(.name==$n) | .id' "$cache")
  if [[ -z "$matches" ]]; then
    echo "$name" >> "$missing_tmp"
    missed=$((missed + 1))
    continue
  fi
  # `grep -c .` would exit 1 on no matches and trip `set -e`. By this
  # point `$matches` is guaranteed non-empty, so wc -l is always safe.
  match_count=$(printf '%s\n' "$matches" | wc -l)
  if (( match_count > 1 )); then
    # Refuse instead of guessing: picking head -n1 would depend on the
    # API's list order. Emit each candidate so the user can copy a
    # specific ID into channel_names.txt on the next run.
    echo "Error: channel name '$name' matched ${match_count} IDs:" >&2
    jq -r --arg n "$name" \
      '.[] | select(.name==$n) |
       "  \(.id)\(if .is_archived then " (archived)" else "" end)"' \
      "$cache" >&2
    echo "$name (ambiguous; paste the intended ID into channel_names.txt)" \
      >> "$missing_tmp"
    missed=$((missed + 1))
    continue
  fi
  id="$matches"
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
