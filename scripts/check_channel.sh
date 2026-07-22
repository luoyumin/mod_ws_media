#!/bin/bash
#
# Channel diagnostics for mod_ws_media.
#
# The media bug can only attach to a channel that has a real (non-proxy) media
# path. This script dumps the channel state that most commonly explains a
# "Failed to add media bug" error.
#
# Usage:
#   ./check_channel.sh <uuid>
#   FS_CLI=/path/to/fs_cli ./check_channel.sh <uuid>

set -euo pipefail

UUID="${1:-}"
FS_CLI="${FS_CLI:-fs_cli}"

if [ -z "$UUID" ]; then
    echo "Usage: $0 <uuid>"
    echo "  (override the fs_cli path with FS_CLI=/path/to/fs_cli)"
    exit 1
fi

echo "==============================================="
echo "Channel diagnostics for UUID: $UUID"
echo "==============================================="

for var in channel_state media_ready bypass_media proxy_media \
           current_application read_codec write_codec; do
    printf '%-20s: ' "$var"
    "$FS_CLI" -x "uuid_getvar $UUID $var"
done

echo "-----------------------------------------------"
"$FS_CLI" -x "uuid_display $UUID"
