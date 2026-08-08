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
# boot Hetzner's own rescue system, wipe the disk's GPT by hand, attach
# the matching Talos ISO (Hetzner Cloud servers boot from local disk by
# default — nothing keeps one "already attached" between runs, unlike a
# dedicated/Robot server's persistent rescue image), then reboot into it,
# which now finds nothing installed and proceeds straight to maintenance
# mode. The ISO is detached again once maintenance mode answers: Hetzner
# Cloud boots from an attached ISO on every restart for as long as it
# stays attached, which would otherwise make the *next* install's own
# post-install reboot land back in the ISO's maintenance mode instead of
# the disk Talos just installed itself onto.
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

for bin in hcloud ssh talosctl ssh-keygen blockdev python3; do
  command -v "$bin" >/dev/null || { echo "missing required binary: $bin" >&2; exit 1; }
done

# The rescue system's own host key is freshly generated on every boot —
# nothing meaningful to pin, so skip persisting it rather than fighting
# stale known_hosts entries on every re-run.
ssh_opts=(-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=5 -o BatchMode=yes -o LogLevel=ERROR)

# wait_until polls check (a command and its args) every interval_seconds,
# failing fast with a clear message instead of hanging forever once
# timeout_seconds is exceeded — the two things this script waits on
# (rescue SSH, Talos maintenance mode) both depend on real hardware
# actually coming back up, which occasionally just doesn't happen (a bad
# ISO, an unattached ISO — see this script's own history).
wait_until() {
  local description="$1" timeout_seconds="$2" interval_seconds="$3"
  shift 3

  local waited=0

  until "$@" >/dev/null 2>&1; do
    if [ "$waited" -ge "$timeout_seconds" ]; then
      echo "timed out after ${timeout_seconds}s waiting for $description" >&2
      return 1
    fi

    sleep "$interval_seconds"
    waited=$((waited + interval_seconds))
  done
}

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

# resolve_talos_iso finds the newest Hetzner-published Talos ISO
# (hcloud-v<version>.<arch>.iso) matching server's own architecture —
# these are what actually boot into Talos maintenance mode; Hetzner's
# stock rescue system is a plain Linux, not Talos, and only serves as the
# environment the disk gets wiped from.
resolve_talos_iso() {
  local hw_arch iso_arch
  hw_arch=$(hcloud server describe "$server" -o json | python3 -c "import json, sys; print(json.load(sys.stdin)['server_type']['architecture'])")

  case "$hw_arch" in
    x86) iso_arch=amd64 ;;
    arm) iso_arch=arm64 ;;
    *) echo "unsupported server architecture: $hw_arch" >&2; return 1 ;;
  esac

  hcloud iso list -o json | python3 -c "
import json, re, sys
arch = '$iso_arch'
pattern = re.compile(r'^hcloud-v(\d+)-(\d+)-(\d+)\.' + re.escape(arch) + r'\.iso\$')
candidates = []
for i in json.load(sys.stdin):
    m = pattern.match(i['name'])
    if m:
        candidates.append((tuple(int(g) for g in m.groups()), i['name']))
if not candidates:
    sys.exit(1)
candidates.sort()
print(candidates[-1][1])
"
}

if [ -z "$ssh_key_name" ]; then
  ssh_key_name=$(resolve_ssh_key) || {
    echo "no local SSH key (~/.ssh/*.pub) matches any hcloud-registered key" >&2
    echo "pass one explicitly: $0 $server $ip <ssh-key-name>" >&2
    exit 1
  }
fi
echo "==> using hcloud SSH key: $ssh_key_name"

talos_iso=$(resolve_talos_iso) || {
  echo "no Hetzner-published Talos ISO found for $server's architecture" >&2
  exit 1
}
echo "==> using Talos ISO: $talos_iso"

if [ "$yes" != 1 ]; then
  read -r -p "This will WIPE the disk on $server ($ip) and reboot it into Talos maintenance mode. Continue? [y/N] " reply
  case "$reply" in
    [yY]|[yY][eE][sS]) ;;
    *) echo "aborted"; exit 1 ;;
  esac
fi

rescue_enabled=0
iso_attached=0
cleanup() {
  if [ "$rescue_enabled" = 1 ]; then
    echo "==> disabling rescue mode (cleanup)"
    hcloud server disable-rescue "$server" >/dev/null 2>&1 || true
  fi
  if [ "$iso_attached" = 1 ]; then
    echo "==> detaching ISO (cleanup)"
    hcloud server detach-iso "$server" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

echo "==> enabling rescue mode on $server"
hcloud server enable-rescue "$server" --type linux64 --ssh-key "$ssh_key_name" >/dev/null
rescue_enabled=1

echo "==> hard-resetting $server into rescue"
hcloud server reset "$server" >/dev/null

echo "==> waiting for rescue system SSH on $ip"
wait_until "rescue system SSH on $ip" 180 3 ssh "${ssh_opts[@]}" "root@$ip" true

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

echo "==> attaching $talos_iso"
hcloud server attach-iso "$server" "$talos_iso" >/dev/null
iso_attached=1

echo "==> rebooting $server into $talos_iso"
hcloud server reboot "$server" >/dev/null

echo "==> waiting for Talos maintenance mode on $ip"
# get disks, not version: talosctl version's Version RPC is gated behind an
# os:admin role check on recent Talos releases, which no maintenance-mode
# caller (talosctl included) can ever satisfy — see
# pkg/domain/instance/talos.go's Discover doc for the same finding via the
# real client library. get disks answers over the same insecure
# maintenance-mode API and doubles as early confirmation the wipe actually
# took (the target disk comes back with no Talos partitions). Each attempt
# is bounded by `timeout 5` since talosctl itself has no --timeout flag.
wait_until "Talos maintenance mode on $ip" 180 3 timeout 5 talosctl get disks --insecure --nodes "$ip"

# Detach now, not in cleanup: Hetzner Cloud boots from an attached ISO on
# every restart for as long as it stays attached, which would otherwise
# make this node's *next* install (talosctl apply-config, run against the
# maintenance-mode instance this just confirmed) reboot back into the
# ISO's own maintenance mode instead of the disk Talos just installed
# itself onto. Maintenance mode itself is already running from RAM by
# this point and doesn't need the ISO to stay attached to keep doing so.
echo "==> detaching $talos_iso"
hcloud server detach-iso "$server" >/dev/null
iso_attached=0

echo "==> $server ($ip) is back in Talos maintenance mode"
