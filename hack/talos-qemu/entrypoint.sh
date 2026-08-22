#!/bin/bash
# Boots a single Talos node as a real QEMU/KVM VM, with the guest sharing
# this container's own Docker Compose network identity — see the "talos"
# service's own doc in compose.yaml for why (in short: a NATed/SLIRP guest
# address would stop being reachable the moment kontinuum switches from
# dialing the "talos" hostname to dialing the node's self-reported IP).
set -euo pipefail

CPU_COUNT="${CPU_COUNT:-2}"
MEM_SIZE_MB="${MEM_SIZE_MB:-4096}"
DISK_SIZE="${DISK_SIZE:-10G}"
GUEST_MAC="52:54:00:12:34:56"

DATA_DIR=/data
DISK="$DATA_DIR/disk.raw"
UUID_FILE="$DATA_DIR/uuid"

mkdir -p "$DATA_DIR"

# A blank virtio disk, exactly like a freshly racked bare-metal node: Talos
# partitions and installs itself onto it the first time it receives a
# machine config, and Reset genuinely wipes STATE/EPHEMERAL back to blank
# on this same file — unlike Docker-mode Talos, whose ModeContainer Reset
# sequence never wipes or reboots at all, regardless of request parameters.
if [ ! -f "$DISK" ]; then
	truncate -s "$DISK_SIZE" "$DISK"
fi

# A stable SMBIOS UUID across restarts, matching how a real machine (or a
# talosctl-provisioned VM) always reports the same identity.
if [ ! -f "$UUID_FILE" ]; then
	cat /proc/sys/kernel/random/uuid >"$UUID_FILE"
fi
NODE_UUID="$(cat "$UUID_FILE")"

# Capture this container's own Docker-assigned address/prefix/gateway on
# eth0 before touching it, then move that exact address onto a bridge
# (br0) joining eth0 and a tap device (tap0) for the guest.
HOST_IP_CIDR="$(ip -o -4 addr show dev eth0 | awk '{print $4}')"
HOST_IP="${HOST_IP_CIDR%%/*}"
HOST_PREFIX="${HOST_IP_CIDR##*/}"
GATEWAY_IP="$(ip route show dev eth0 | awk '/^default/ {print $3; exit}')"

prefix_to_netmask() {
	local bits="$1" mask="" i
	for i in 0 1 2 3; do
		if [ "$bits" -ge 8 ]; then
			mask="${mask:+$mask.}255"
			bits=$((bits - 8))
		else
			mask="${mask:+$mask.}$((256 - (1 << (8 - bits))))"
			bits=0
		fi
	done
	echo "$mask"
}
HOST_NETMASK="$(prefix_to_netmask "$HOST_PREFIX")"

# Second-to-last address in the subnet (broadcast minus one) — used below
# as br0's own identity, deliberately as far as possible from Docker's
# own low, sequential per-container allocation within the same network.
ip_to_int() {
	local IFS=.
	read -r a b c d <<<"$1"
	echo $((a * 16777216 + b * 65536 + c * 256 + d))
}
int_to_ip() {
	local i="$1"
	echo "$((i >> 24 & 255)).$((i >> 16 & 255)).$((i >> 8 & 255)).$((i & 255))"
}
netmask_int=$((0xFFFFFFFF << (32 - HOST_PREFIX) & 0xFFFFFFFF))
network_int=$(($(ip_to_int "$HOST_IP") & netmask_int))
broadcast_int=$((network_int | (~netmask_int & 0xFFFFFFFF)))
BRIDGE_IDENTITY_IP="$(int_to_ip $((broadcast_int - 1)))"

# br0 itself is given no address: the guest is meant to be the sole
# owner of this container's Docker-assigned identity. Giving br0 the same
# address too (an earlier version of this script did) creates a genuine
# duplicate-IP conflict on the same L2 segment — ARP for that address
# then resolves unpredictably to whichever of the two answers first, so
# connections intermittently land on this container's own (unlistening)
# network stack instead of the guest, surfacing as inbound connections
# refused despite the guest itself being reachable and healthy.
ip tuntap add dev tap0 mode tap
ip link add name br0 type bridge
ip link set eth0 down
ip addr flush dev eth0
ip link set eth0 master br0
ip link set tap0 master br0
ip link set eth0 up
ip link set tap0 up
ip link set br0 up

# dnsmasq refuses to serve a DHCP range that doesn't fall within some
# subnet actually configured on the listening interface — so br0 needs an
# address of its own in that same network, distinct from both the real
# gateway (.1) and the guest's own address (reusing that would recreate
# the ARP conflict above). BRIDGE_IDENTITY_IP, computed above, is about as
# far from Docker's own low sequential per-container allocation as this
# network allows.
ip addr add "${BRIDGE_IDENTITY_IP}/${HOST_PREFIX}" dev br0

echo "talos: bridged guest onto ${HOST_IP_CIDR} via gateway ${GATEWAY_IP}" >&2

# A real DHCP server, not a static kernel `ip=` argument: Talos only
# honors a kernel-cmdline static address on the exact boot it was passed
# on. Once it installs itself to disk, every later boot (including the
# one right after install, and every one after a real Reset) reboots via
# a bootloader-persisted cmdline Talos regenerates itself from machine
# config — which defaults to DHCP unless the machine config says
# otherwise, and kontinuum has no way to inject a static network config
# into what it generates. A real DHCP server on this bridge, handing the
# guest's known MAC this container's own address every time, is what
# actually survives that: no dependence on which cmdline the node
# happens to be running, on any given boot, ever again. Confirmed by
# testing an install → reboot cycle directly: without this, the node
# never comes back after the very first post-install reboot, since
# nothing answers its DHCP request. dnsmasq listens only on br0, and the
# range is exactly one address so only the reserved MAC can ever get a
# lease. --bind-dynamic (not --bind-interfaces) is required since br0
# only gets its own address a few lines up, after tap0/eth0 are already
# enslaved to it.
#
# dns-server hands out this container's own bridge address, not a public
# resolver directly — dnsmasq itself forwards (via --server) to Docker's
# embedded DNS at 127.0.0.11, which only answers from inside this
# container's own network namespace (it's a per-container DNAT rule, not
# reachable from the bridged guest directly). That's not just about the
# guest resolving "talos" or "proxy" itself: once Kubernetes is up, its
# own CoreDNS inherits the node's /etc/resolv.conf as its upstream
# forwarder, and a joined zone's kontinuum pod dials the hub's etcd proxy
# at a plain Compose service name (KONTINUUM_SERVER_GRPC_ENDPOINT, e.g.
# "proxy:8443") — a real public resolver has never heard of it and
# returns nothing ("name resolver error: produced zero addresses" is
# what that surfaces as in kontinuum's own logs), where Docker's embedded
# DNS resolves it correctly, same as every other container in this
# compose stack already relies on. 127.0.0.11 still forwards on to the
# host's own real upstream for internet names, so this doesn't cost the
# guest anything the previous 1.1.1.1/8.8.8.8 setup had.
dnsmasq \
	--no-daemon \
	--bind-dynamic \
	--interface=br0 \
	--dhcp-range="${HOST_IP},${HOST_IP},${HOST_NETMASK},12h" \
	--dhcp-host="${GUEST_MAC},${HOST_IP}" \
	--dhcp-option="option:router,${GATEWAY_IP}" \
	--dhcp-option="option:dns-server,${BRIDGE_IDENTITY_IP}" \
	--server=127.0.0.11 \
	--no-resolv \
	--no-hosts \
	--log-dhcp &

# See pkg/machinery/kernel.DefaultArgs and pkg/provision/providers/qemu/qemu.go
# upstream (siderolabs/talos) for where this list comes from — it's exactly
# what talosctl's own qemu provisioner passes, so this VM behaves like any
# other Talos QEMU node, not a bespoke config that happens to boot.
# talos.platform=metal (not left to auto-detect) is the load-bearing flag:
# it's what makes Reset actually wipe and reboot instead of the no-op
# ModeContainer forces — confirmed it survives reinstalls too, since Talos
# regenerates it itself from its own known platform state. No talos.config=
# is set, so the node comes up in maintenance mode waiting for kontinuum to
# apply one over the network, exactly like the Docker-mode node did.
KERNEL_ARGS="init_on_alloc=1 slab_nomerge= pti=on consoleblank=0 nvme_core.io_timeout=4294967295 printk.devkmsg=on ima_template=ima-ng ima_appraise=fix ima_hash=sha512 console=ttyS0 reboot=k panic=1 talos.shutdown=halt talos.platform=metal"

# -no-reboot makes qemu-system-x86_64 exit (rather than warm-reset
# internally) whenever the guest asks to reboot — e.g. after installing
# itself to disk, or after a real Reset. The loop below relaunches it
# immediately against the same disk, which is what actually delivers the
# reboot: a power-cycle of the "hardware", identical in effect to a real
# node's BMC bouncing it. Note that Talos itself often reboots via kexec
# instead (staying in the same qemu process, chainloading straight from
# the installed disk) whenever it can — a real Reset can't use that path
# since the disk it would chainload from is exactly what got wiped, so it
# still falls through to a real power-cycle here. `if !` (not a bare
# statement) is load-bearing under `set -e`: it keeps a non-zero qemu
# exit from killing this script.
while true; do
	if ! qemu-system-x86_64 \
		-machine q35,accel=kvm \
		-cpu host \
		-smp "cpus=${CPU_COUNT}" \
		-m "${MEM_SIZE_MB}" \
		-nographic \
		-no-reboot \
		-smbios "type=1,uuid=${NODE_UUID}" \
		-drive "id=disk0,format=raw,if=none,file=${DISK},cache=none" \
		-device virtio-blk-pci,drive=disk0 \
		-netdev tap,id=net0,ifname=tap0,script=no,downscript=no \
		-device virtio-net-pci,netdev=net0,mac=${GUEST_MAC} \
		-device virtio-rng-pci \
		-kernel /opt/talos/vmlinuz \
		-initrd /opt/talos/initramfs.xz \
		-append "$KERNEL_ARGS"; then
		echo "talos: qemu exited non-zero, relaunching" >&2
	else
		echo "talos: qemu exited (guest-triggered reboot), relaunching" >&2
	fi
done
