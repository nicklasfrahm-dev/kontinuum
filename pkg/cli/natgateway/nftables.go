package natgateway

import (
	"fmt"
	"os"

	"github.com/google/nftables"
	"github.com/google/nftables/expr"
)

// natTableName and postroutingChainName name the ip-family table/chain
// this package owns exclusively — ensureMasquerade always deletes any
// stale table by this exact name before recreating it (see
// deleteExistingTable), rather than touching anything else already present
// in the node's own ruleset.
const (
	natTableName         = "kontinuum_nat"
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
// hostNetwork (see pkg/domain/fabric.ensureNATGatewayWorkload) so this
// write reaches the node's own real network namespace, not a
// pod-isolated one.
func enableIPForwarding() error {
	err := os.WriteFile(ipForwardPath, []byte("1\n"), ipForwardFilePerm)
	if err != nil {
		return fmt.Errorf("failed to write %q: %w", ipForwardPath, err)
	}

	return nil
}

// ensureMasquerade (re)creates natTableName from scratch with one rule:
// masquerade every packet leaving iface — the standard, simplest possible
// NAT gateway rule (`nft add rule nat postrouting oifname $iface
// masquerade`), the same shape as the nftables Go client's own
// TestConfigureNAT reference example. Idempotent: any stale table left
// over from a previous run of this same command (e.g. after a crash) is
// deleted first — see deleteExistingTable.
func ensureMasquerade(iface string) error {
	conn, err := nftables.New()
	if err != nil {
		return fmt.Errorf("failed to open nftables connection: %w", err)
	}

	err = deleteExistingTable(conn)
	if err != nil {
		return err
	}

	table := conn.AddTable(&nftables.Table{Family: nftables.TableFamilyIPv4, Name: natTableName})

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

// deleteMasquerade removes natTableName, tolerating it already being gone.
func deleteMasquerade() error {
	conn, err := nftables.New()
	if err != nil {
		return fmt.Errorf("failed to open nftables connection: %w", err)
	}

	return deleteExistingTable(conn)
}

// deleteExistingTable deletes natTableName if it currently exists —
// ensureMasquerade's own idempotency: rather than trying to diff and patch
// an existing ruleset, every run just recreates it from scratch.
func deleteExistingTable(conn *nftables.Conn) error {
	tables, err := conn.ListTablesOfFamily(nftables.TableFamilyIPv4)
	if err != nil {
		return fmt.Errorf("failed to list existing ipv4 tables: %w", err)
	}

	found := false

	for _, table := range tables {
		if table.Name == natTableName {
			conn.DelTable(table)

			found = true
		}
	}

	if !found {
		return nil
	}

	err = conn.Flush()
	if err != nil {
		return fmt.Errorf("failed to remove existing %q table: %w", natTableName, err)
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
