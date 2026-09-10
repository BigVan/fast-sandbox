package network

import (
	"context"
	"fmt"
	"net/netip"
	"strings"
)

// DetectCIDROverlap reports live IPv4 routes that overlap the Fastlet slot
// CIDR. The slot bridge installs a connected route for the CIDR and the
// sibling-isolation rules reject the whole range, so any pre-existing
// pod/node route inside it (CNI pod CIDR, Docker bridge pool, VPC overlay
// route) would be hijacked or blackholed. Routes via the slot bridge itself
// are expected on restart and ignored. Fastlet refuses to start on a
// conflict instead of silently producing half-broken sandboxes.
func DetectCIDROverlap(ctx context.Context, cidr, bridgeDevice string, runner CommandRunner) error {
	slotPrefix, err := netip.ParsePrefix(cidr)
	if err != nil {
		return fmt.Errorf("parse slot CIDR %q: %w", cidr, err)
	}
	if !slotPrefix.Addr().Is4() {
		return fmt.Errorf("slot CIDR %q is not IPv4", cidr)
	}
	slotPrefix = slotPrefix.Masked()
	output, err := runner.Run(ctx, "ip", "-4", "route", "show")
	if err != nil {
		return fmt.Errorf("read IPv4 routes: %w", err)
	}
	var conflicts []string
	for _, line := range strings.Split(string(output), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 || fields[0] == "default" {
			continue
		}
		if routeDevice(fields) == bridgeDevice {
			continue
		}
		routePrefix, ok := parseRouteDestination(fields[0])
		if !ok {
			continue
		}
		if prefixesOverlap(slotPrefix, routePrefix) {
			conflicts = append(conflicts, strings.TrimSpace(line))
		}
	}
	if len(conflicts) > 0 {
		return fmt.Errorf("slot CIDR %s overlaps live host routes: %s; set FAST_SANDBOX_NETWORK_CIDR to a free range", cidr, strings.Join(conflicts, "; "))
	}
	return nil
}

// routeDevice extracts the egress device of an `ip route` line (empty when
// absent).
func routeDevice(fields []string) string {
	for index := 0; index+1 < len(fields); index++ {
		if fields[index] == "dev" {
			return fields[index+1]
		}
	}
	return ""
}

// parseRouteDestination parses an `ip route` destination column: a prefix
// ("172.30.0.0/24") or a bare address (an implicit host route).
func parseRouteDestination(field string) (netip.Prefix, bool) {
	if prefix, err := netip.ParsePrefix(field); err == nil && prefix.Addr().Is4() {
		return prefix.Masked(), true
	}
	if address, err := netip.ParseAddr(field); err == nil && address.Is4() {
		return netip.PrefixFrom(address, address.BitLen()), true
	}
	return netip.Prefix{}, false
}

// prefixesOverlap reports whether two prefixes contain any common address.
func prefixesOverlap(a, b netip.Prefix) bool {
	return a.Contains(b.Addr()) || b.Contains(a.Addr())
}
