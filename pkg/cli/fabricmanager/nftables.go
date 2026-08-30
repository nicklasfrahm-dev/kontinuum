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
// natTablePrefix+"_"+<fabric/interface identity> (see NATTableName), not a
// single fixed name shared by every Fabric — a gateway node elected by more
// than one Fabric (see Reconciler's own doc) gets one independent table per
// Fabric, each only ever touching its own. ensureMasquerade always deletes
// any stale table by that same name before recreating it (see
// queueDeleteExistingTable); PruneStaleMasqueradeTables does the same for
// every table this node no longer has any live, NAT-enabled Fabric
// assignment for at all. Neither ever touches any *other* table, whether
// that's a sibling Fabric's own kontinuum_nat_* table, or an unrelated one
// entirely (see queueDeleteExistingTable's own doc for why that also keeps
// the CNI's own nftables state — Cilium, kube-proxy — untouched).
const (
	natTablePrefix       = "kontinuum_nat"
	postroutingChainName = "postrouting"
)

// ifnameSize is IFNAMSIZ — the kernel's own fixed interface-name buffer
// size, and the length nftables' own meta oifname/iifname comparisons
// expect their operand encoded as (see IfnameBytes).
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

// NATTableName derives this process's own exclusively-owned table name
// from fabricID (the owning Fabric's own metadata.name) and iface — see
// natTablePrefix's own doc for why this is scoped per interface, and
// fabricID's own doc (passed down from --id) for why per-interface scoping
// alone isn't enough: nothing stops two different Fabric objects from
// electing the same gateway node for the same interface. The sanitized
// fabricID segment is length-prefixed so its boundary with the sanitized
// interface segment is unambiguous — without that, two different
// (fabricID, iface) pairs could still combine into the identical table
// name (e.g. fabricID "eu" + iface "1_eth0" vs fabricID "eu_1" + iface
// "eth0").
func NATTableName(fabricID, iface string) string {
	sanitizedID := SanitizeForTableName(fabricID)

	return fmt.Sprintf("%s_%d_%s_%s", natTablePrefix, len(sanitizedID), sanitizedID, SanitizeForTableName(iface))
}

// SanitizeForTableName maps iface onto a valid nftables identifier: a VLAN
// sub-interface's own kernel name (e.g. "eth0.100") contains a "." nft
// table/chain names don't reliably accept, so any byte other than an ASCII
// letter or digit is escaped as "_" followed by its two lowercase hex
// digits (e.g. "." becomes "_2e") — including a literal "_" itself
// (escaped as "_5f"), so every "_" in the output unambiguously starts an
// escape sequence. Collapsing every disallowed byte to the same literal
// "_" (as an earlier version of this function did) let two different
// interfaces collide onto the same table name (e.g. "eth0.1" and "eth0_1"
// both became "eth0_1"); this escaping is injective, so distinct inputs
// always produce distinct outputs.
func SanitizeForTableName(iface string) string {
	var sanitized strings.Builder

	for i := range len(iface) {
		charByte := iface[i]

		switch {
		case charByte >= 'a' && charByte <= 'z', charByte >= 'A' && charByte <= 'Z', charByte >= '0' && charByte <= '9':
			sanitized.WriteByte(charByte)
		default:
			fmt.Fprintf(&sanitized, "_%02x", charByte)
		}
	}

	return sanitized.String()
}

// ensureMasquerade (re)creates iface's own table (see NATTableName) from
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
func ensureMasquerade(fabricID, iface string) error {
	conn, err := nftables.New()
	if err != nil {
		return fmt.Errorf("failed to open nftables connection: %w", err)
	}

	err = queueDeleteExistingTable(conn, fabricID, iface)
	if err != nil {
		return err
	}

	table := conn.AddTable(&nftables.Table{Family: nftables.TableFamilyIPv4, Name: NATTableName(fabricID, iface)})

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
			&expr.Cmp{Op: expr.CmpOpEq, Register: 1, Data: IfnameBytes(iface)},
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

// PruneStaleMasqueradeTables deletes every kontinuum_nat_*-owned table on
// this node whose name isn't in keep (the full set of table names — see
// NATTableName — Reconciler currently wants present, across every Fabric
// that elects this node with NAT enabled). This is what actually tears
// down a table once nothing justifies it anymore: NAT disabled, this node
// re-elected away, or the owning Fabric deleted outright — none of which
// individually enumerate "what to remove" the way ensureMasquerade's own
// per-Fabric call already knows what to *add*, so this instead reasons
// from the opposite direction, over the full currently-observed table
// list. Only ever matches names carrying natTablePrefix's own "_"
// separator — never a ruleset-wide flush — so the CNI's own tables
// (Cilium, kube-proxy) stay untouched, the same exact-name-scoped
// guarantee queueDeleteExistingTable's own doc already gives its single
// delete.
func PruneStaleMasqueradeTables(keep map[string]bool) error {
	conn, err := nftables.New()
	if err != nil {
		return fmt.Errorf("failed to open nftables connection: %w", err)
	}

	tables, err := conn.ListTablesOfFamily(nftables.TableFamilyIPv4)
	if err != nil {
		return fmt.Errorf("failed to list existing ipv4 tables: %w", err)
	}

	pruned := false

	for _, table := range tables {
		if !strings.HasPrefix(table.Name, natTablePrefix+"_") || keep[table.Name] {
			continue
		}

		conn.DelTable(table)

		pruned = true
	}

	if !pruned {
		return nil
	}

	err = conn.Flush()
	if err != nil {
		return fmt.Errorf("failed to prune stale nat tables: %w", err)
	}

	return nil
}

// queueDeleteExistingTable queues a delete of iface's own table (see
// NATTableName), if it currently exists, onto conn — without flushing.
// ensureMasquerade queues its own replacement table/chain/rule right after
// calling this and flushes both together in one atomic transaction (see
// its own doc); deleteMasquerade flushes this alone, as the only thing in
// its own batch. Only ever matches this exact, interface-scoped table
// name — see natTablePrefix's own doc for why that matters both for
// multi-VLAN safety and for never touching the CNI's own tables.
func queueDeleteExistingTable(conn *nftables.Conn, fabricID, iface string) error {
	tables, err := conn.ListTablesOfFamily(nftables.TableFamilyIPv4)
	if err != nil {
		return fmt.Errorf("failed to list existing ipv4 tables: %w", err)
	}

	name := NATTableName(fabricID, iface)

	for _, table := range tables {
		if table.Name == name {
			conn.DelTable(table)
		}
	}

	return nil
}

// IfnameBytes encodes iface the way nftables' own meta oifname/iifname
// comparisons expect their operand: a fixed ifnameSize-byte, NUL-padded
// buffer — mirrors the nftables Go client's own test helper of the same
// shape.
func IfnameBytes(iface string) []byte {
	buf := make([]byte, ifnameSize)
	copy(buf, iface+"\x00")

	return buf
}
