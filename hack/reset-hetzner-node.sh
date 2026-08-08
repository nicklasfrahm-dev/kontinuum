#!/usr/bin/env bash
# reset-hetzner-node.sh — return a Hetzner Cloud server running Talos back
# to insecure maintenance mode.
#
# A plain reboot isn't enough once Talos has actually installed itself:
# its own halt_if_installed kernel guard refuses to boot the attached ISO
# at all when the local disk still holds a Talos install, so the node
# just halts instead of coming back up in maintenance mode. The clean fix
# — an authenticated `talosctl reset` against the node's own installed
# API — only works while you still hold that cluster's certs (see
# docs/workflows/zone-add.md's own note on issue #49); once its Secret is
# gone, so is the ability to authenticate. This script is the fallback:
# boot Hetzner's own rescue system, wipe the disk's GPT by hand, then
# reboot into the already-attached Talos ISO, which now finds nothing
# installed and proceeds straight to maintenance mode.
#
# Usage: hack/reset-hetzner-node.sh <hcloud-server-name> <server-ip> [ssh-key-name] [-y]
set -euo pipefail

yes=0
args=()
for arg in "$@"; do
  if [ "$arg" = "-y" ] || [ "$arg" = "--yes" ]; then
    yes=1
  else
    args+=("$arg")
  fi
done

server="${args[0]:?usage: $0 <hcloud-server-name> <server-ip> [ssh-key-name] [-y]}"
ip="${args[1]:?usage: $0 <hcloud-server-name> <server-ip> [ssh-key-name] [-y]}"
ssh_key_name="${args[2]:-}"

for bin in hcloud ssh talosctl ssh-keygen blockdev; do
  command -v "$bin" >/dev/null || { echo "missing required binary: $bin" >&2; exit 1; }
done

# The rescue system's own host key is freshly generated on every boot —
# nothing meaningful to pin, so skip persisting it rather than fighting
# stale known_hosts entries on every re-run.
ssh_opts=(-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=5 -o BatchMode=yes -o LogLevel=ERROR)

# resolve_ssh_key finds an hcloud-registered SSH key whose fingerprint
# matches one of our own local public keys, so rescue mode gets
# authorized with a key we actually hold the private half of — passing
# the wrong hcloud key name (e.g. one registered under a different local
# machine) leaves rescue mode unreachable with no error until you try to
# SSH in.
resolve_ssh_key() {
  local pub local_fp name
  for pub in "$HOME"/.ssh/*.pub; do
    [ -f "$pub" ] || continue
    local_fp=$(ssh-keygen -l -E md5 -f "$pub" | awk '{print $2}' | sed 's/^MD5://')
    name=$(hcloud ssh-key list -o json | python3 -c "
import json, sys
fp = '$local_fp'
for k in json.load(sys.stdin):
    if k['fingerprint'] == fp:
        print(k['name'])
        break
")
    if [ -n "$name" ]; then
      echo "$name"
      return 0
    fi
  done
  return 1
}

if [ -z "$ssh_key_name" ]; then
  ssh_key_name=$(resolve_ssh_key) || {
    echo "no local SSH key (~/.ssh/*.pub) matches any hcloud-registered key" >&2
    echo "pass one explicitly: $0 $server $ip <ssh-key-name>" >&2
    exit 1
  }
fi
echo "==> using hcloud SSH key: $ssh_key_name"

if [ "$yes" != 1 ]; then
  read -r -p "This will WIPE the disk on $server ($ip) and reboot it into Talos maintenance mode. Continue? [y/N] " reply
  case "$reply" in
    [yY]|[yY][eE][sS]) ;;
    *) echo "aborted"; exit 1 ;;
  esac
fi

rescue_enabled=0
cleanup() {
  if [ "$rescue_enabled" = 1 ]; then
    echo "==> disabling rescue mode (cleanup)"
    hcloud server disable-rescue "$server" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

echo "==> enabling rescue mode on $server"
hcloud server enable-rescue "$server" --type linux64 --ssh-key "$ssh_key_name" >/dev/null
rescue_enabled=1

echo "==> hard-resetting $server into rescue"
hcloud server reset "$server" >/dev/null

echo "==> waiting for rescue system SSH on $ip"
until ssh "${ssh_opts[@]}" "root@$ip" true 2>/dev/null; do sleep 5; done

echo "==> locating a disk with a Talos install"
disk=$(ssh "${ssh_opts[@]}" "root@$ip" bash -s <<'REMOTE'
for d in /dev/sd? /dev/nvme?n1; do
  [ -b "$d" ] || continue
  if lsblk -no LABEL "$d" 2>/dev/null | grep -qx -e STATE -e EPHEMERAL; then
    echo "$d"
    break
  fi
done
REMOTE
)

if [ -z "$disk" ]; then
  echo "==> no Talos install found on any disk — nothing to wipe"
else
  echo "==> wiping Talos install on $disk"
  ssh "${ssh_opts[@]}" "root@$ip" bash -s -- "$disk" <<'REMOTE'
set -euo pipefail
disk="$1"
for p in "$disk"*[0-9]; do
  [ -b "$p" ] && wipefs -a "$p" || true
done
sgdisk --zap-all "$disk"
size_sectors=$(blockdev --getsz "$disk")
dd if=/dev/zero of="$disk" bs=1M count=10 conv=fsync
dd if=/dev/zero of="$disk" bs=1M seek=$(( size_sectors / 2048 - 10 )) count=10 conv=fsync
sync
REMOTE
fi

echo "==> disabling rescue mode"
hcloud server disable-rescue "$server" >/dev/null
rescue_enabled=0

echo "==> rebooting $server into its attached Talos ISO"
hcloud server reboot "$server" >/dev/null

echo "==> waiting for Talos maintenance mode on $ip"
until talosctl version --insecure --nodes "$ip" --timeout 3s >/dev/null 2>&1; do sleep 5; done

echo "==> $server ($ip) is back in Talos maintenance mode"
