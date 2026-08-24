package fabricmanager

import (
	"fmt"
	"os"
	"strings"

	"github.com/google/nftables"
	"github.com/google/nftables/expr"
)

// natTablePrefix and postroutingChainName name the ip-family table/chain
// this package owns exclusively. The table itself is named
// natTablePrefix+"_"+<sanitized interface name> (see natTableName), not a
// single fixed name shared by every interface — a gateway node terminating
// more than one VLAN (a trunk port with several tagged sub-interfaces,
// each backing a different zone's own Fabric) runs one fabricmanager
// process per interface, and each must only ever touch its own table.
// ensureMasquerade always deletes any stale table by that same
// interface-scoped name before recreating it (see queueDeleteExistingTable)
// — it never lists or deletes any *other* table, whether that's a sibling
// interface's own kontinuum_nat_* table, or an unrelated one entirely (see
// that function's own doc for why this also keeps the CNI's own nftables
// state — Cilium, kube-proxy — untouched).
const (
	natTablePrefix       = "kontinuum_nat"
	postroutingChainName = "postrouting"
)

// ifnameSize is IFNAMSIZ — the kernel's own fixed interface-name buffer
// size, and the length nftables' own meta oifname/iifname comparisons
// expect their operand encoded as (see ifnameBytes).
const ifnameSize = 16

// ipForwardPath is the sysctl this package flips to actually let the
// kernel route packets between interfaces — without it, the masquerade
// rule ensureMasquerade installs would apply to nothing, since no packet
// not addressed to this host would ever reach the postrouting hook in the
// first place.
const ipForwardPath = "/proc/sys/net/ipv4/ip_forward"

// ipForwardFilePerm is os.WriteFile's own mode argument — meaningless for
// an already-existing procfs entry like ipForwardPath (the kernel ignores
// it; only a brand-new file's permissions would ever be set from it), but
// kept at a conservative value regardless rather than a wide-open one.
const ipForwardFilePerm = 0o600

// enableIPForwarding sets net.ipv4.ip_forward=1. This process runs
// hostNetwork (see pkg/domain/fabric.ensureFabricManagerWorkload) so this
// write reaches the node's own real network namespace, not a
// pod-isolated one. A node-wide sysctl, not per-interface: safe to set
// (redundantly) from more than one fabricmanager process on the same node,
// since it's idempotent and there's no per-VLAN equivalent to scope it to.
func enableIPForwarding() error {
	err := os.WriteFile(ipForwardPath, []byte("1\n"), ipForwardFilePerm)
	if err != nil {
		return fmt.Errorf("failed to write %q: %w", ipForwardPath, err)
	}

	return nil
}

// natTableName derives this process's own exclusively-owned table name
// from iface — see natTablePrefix's own doc for why this is scoped per
// interface rather than shared.
func natTableName(iface string) string {
	return natTablePrefix + "_" + sanitizeForTableName(iface)
}

// sanitizeForTableName maps iface onto a valid nftables identifier: a VLAN
// sub-interface's own kernel name (e.g. "eth0.100") contains a "." nft
// table/chain names don't reliably accept, so anything other than an
// ASCII letter, digit, or underscore is replaced with "_".
func sanitizeForTableName(iface string) string {
	var sanitized strings.Builder

	for _, r := range iface {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '_':
			sanitized.WriteRune(r)
		default:
			sanitized.WriteRune('_')
		}
	}

	return sanitized.String()
}

// ensureMasquerade (re)creates iface's own table (see natTableName) from
// scratch with one rule: masquerade every packet leaving iface — the
// standard, simplest possible NAT gateway rule (`nft add rule nat
// postrouting oifname $iface masquerade`), the same shape as the nftables
// Go client's own TestConfigureNAT reference example. Idempotent: any stale
// table left over from a previous run of this same command for this same
// interface (e.g. after a crash) is queued for deletion first (see
// queueDeleteExistingTable) — queued, not flushed, so the delete and the
// replacement table/chain/rule below commit in the same netlink batch.
// nftables' own batch commit is atomic (all or nothing), so this never
// leaves a window with no masquerade rule at all — two separate Flush
// calls would.
//
// Never touches the CNI's own nftables state (Cilium, or kube-proxy's own
// iptables-nft-backed rules): this only ever lists tables to find one
// matching iface's own exact, namespaced table name (see
// queueDeleteExistingTable) — never a ruleset-wide flush — and the new
// table/chain this adds is its own independent nat/postrouting hook
// registration, which the kernel runs alongside any other tool's own
// postrouting chain at that same hook (multiple independent nftables
// tables hooking the same netfilter hook, each at their own priority, is
// standard, supported coexistence — not a conflict). The rule itself only
// ever matches traffic leaving through iface — this node's own uplink/VLAN
// interface — never a CNI's own overlay/virtual interfaces (cilium_host,
// lxc*, ...), so pod-to-pod (east-west) traffic never reaches it at all.
func ensureMasquerade(iface string) error {
	conn, err := nftables.New()
	if err != nil {
		return fmt.Errorf("failed to open nftables connection: %w", err)
	}

	err = queueDeleteExistingTable(conn, iface)
	if err != nil {
		return err
	}

	table := conn.AddTable(&nftables.Table{Family: nftables.TableFamilyIPv4, Name: natTableName(iface)})

	chain := conn.AddChain(&nftables.Chain{
		Name:     postroutingChainName,
		Table:    table,
		Type:     nftables.ChainTypeNAT,
		Hooknum:  nftables.ChainHookPostrouting,
		Priority: nftables.ChainPriorityNATSource,
	})

	conn.AddRule(&nftables.Rule{
		Table: table,
		Chain: chain,
		Exprs: []expr.Any{
			// [ meta load oifname => reg 1 ]
			&expr.Meta{Key: expr.MetaKeyOIFNAME, Register: 1},
			// [ cmp eq reg 1 <iface> ]
			&expr.Cmp{Op: expr.CmpOpEq, Register: 1, Data: ifnameBytes(iface)},
			// [ masq ]
			&expr.Masq{},
		},
	})

	err = conn.Flush()
	if err != nil {
		return fmt.Errorf("failed to apply nftables rules: %w", err)
	}

	return nil
}

// deleteMasquerade removes iface's own table (see natTableName), tolerating
// it already being gone.
func deleteMasquerade(iface string) error {
	conn, err := nftables.New()
	if err != nil {
		return fmt.Errorf("failed to open nftables connection: %w", err)
	}

	err = queueDeleteExistingTable(conn, iface)
	if err != nil {
		return err
	}

	err = conn.Flush()
	if err != nil {
		return fmt.Errorf("failed to remove existing %q table: %w", natTableName(iface), err)
	}

	return nil
}

// queueDeleteExistingTable queues a delete of iface's own table (see
// natTableName), if it currently exists, onto conn — without flushing.
// ensureMasquerade queues its own replacement table/chain/rule right after
// calling this and flushes both together in one atomic transaction (see
// its own doc); deleteMasquerade flushes this alone, as the only thing in
// its own batch. Only ever matches this exact, interface-scoped table
// name — see natTablePrefix's own doc for why that matters both for
// multi-VLAN safety and for never touching the CNI's own tables.
func queueDeleteExistingTable(conn *nftables.Conn, iface string) error {
	tables, err := conn.ListTablesOfFamily(nftables.TableFamilyIPv4)
	if err != nil {
		return fmt.Errorf("failed to list existing ipv4 tables: %w", err)
	}

	name := natTableName(iface)

	for _, table := range tables {
		if table.Name == name {
			conn.DelTable(table)
		}
	}

	return nil
}

// ifnameBytes encodes iface the way nftables' own meta oifname/iifname
// comparisons expect their operand: a fixed ifnameSize-byte, NUL-padded
// buffer — mirrors the nftables Go client's own test helper of the same
// shape.
func ifnameBytes(iface string) []byte {
	buf := make([]byte, ifnameSize)
	copy(buf, iface+"\x00")

	return buf
}
