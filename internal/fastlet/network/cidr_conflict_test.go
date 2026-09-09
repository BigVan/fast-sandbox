package network

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type routeRunner struct {
	routes string
}

func (r *routeRunner) Run(_ context.Context, command string, args ...string) ([]byte, error) {
	return []byte(r.routes), nil
}

func TestDetectCIDROverlapClean(t *testing.T) {
	runner := &routeRunner{routes: strings.Join([]string{
		"default via 10.244.0.1 dev eth0",
		"10.244.1.0/24 dev eth0 proto kernel scope link src 10.244.1.5",
		"10.96.0.0/12 via 10.244.0.1 dev eth0 metric 10",
		"172.17.0.0/16 dev docker0 proto kernel scope link src 172.17.0.1",
	}, "\n")}
	require.NoError(t, DetectCIDROverlap(context.Background(), "172.30.0.0/24", "fsb0", runner))
}

func TestDetectCIDROverlapIgnoresOwnBridgeAndDefault(t *testing.T) {
	runner := &routeRunner{routes: strings.Join([]string{
		"default via 172.30.0.254 dev eth0",
		// The slot bridge's own connected route (present on restart).
		"172.30.0.0/24 dev fsb0 proto kernel scope link src 172.30.0.1",
	}, "\n")}
	require.NoError(t, DetectCIDROverlap(context.Background(), "172.30.0.0/24", "fsb0", runner))
}

func TestDetectCIDROverlapReportsConflicts(t *testing.T) {
	// A Docker bridge pool covering the slot CIDR.
	runner := &routeRunner{routes: strings.Join([]string{
		"default via 10.244.0.1 dev eth0",
		"10.244.1.0/24 dev eth0 proto kernel scope link src 10.244.1.5",
		"172.30.0.0/16 dev docker0 proto kernel scope link src 172.30.0.1",
	}, "\n")}
	err := DetectCIDROverlap(context.Background(), "172.30.0.0/24", "fsb0", runner)
	require.Error(t, err)
	require.Contains(t, err.Error(), "172.30.0.0/16 dev docker0")

	// A more specific route inside a widened slot CIDR (CNI per-node pod
	// routes carved out of the same /16).
	runner = &routeRunner{routes: "172.30.4.0/26 via 10.0.0.9 dev tun0"}
	err = DetectCIDROverlap(context.Background(), "172.30.0.0/16", "fsb0", runner)
	require.Error(t, err)
	require.Contains(t, err.Error(), "172.30.4.0/26")

	// A bare host route (implicit /32) inside the slot CIDR.
	runner = &routeRunner{routes: "172.30.0.7 dev eth0"}
	err = DetectCIDROverlap(context.Background(), "172.30.0.0/24", "fsb0", runner)
	require.Error(t, err)
	require.Contains(t, err.Error(), "172.30.0.7")
}

func TestDetectCIDROverlapRejectsInvalidCIDR(t *testing.T) {
	require.Error(t, DetectCIDROverlap(context.Background(), "not-a-cidr", "fsb0", &routeRunner{}))
	require.Error(t, DetectCIDROverlap(context.Background(), "fd00::/64", "fsb0", &routeRunner{}))
}

func TestParseRouteDestination(t *testing.T) {
	prefix, ok := parseRouteDestination("172.30.0.0/16")
	require.True(t, ok)
	require.Equal(t, "172.30.0.0/16", prefix.String())

	prefix, ok = parseRouteDestination("10.1.2.3")
	require.True(t, ok)
	require.Equal(t, "10.1.2.3/32", prefix.String())

	_, ok = parseRouteDestination("unreachable")
	require.False(t, ok)
}
