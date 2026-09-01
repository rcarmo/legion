package cluster

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/grandcat/zeroconf"
)

const (
	ServiceType     = "_durable-fn._udp.local."
	zeroconfService = "_durable-fn._udp"
)

type RaftAdvertisement struct {
	NodeID            uint64
	RaftAddr, APIAddr string
}
type DiscoveredPeer struct {
	EndpointID, Host      string
	APIPort               uint16
	RaftID                *uint64
	RaftAddr, RaftAPIAddr string
}

type BonjourRegistration struct{ server *zeroconf.Server }

func RegisterBonjour(endpointID, hostname, ip string, port uint16, version string, raft RaftAdvertisement) (*BonjourRegistration, error) {
	instance := "legion-" + endpointID[:min(16, len(endpointID))]
	text := []string{"node_id=" + endpointID, "version=" + version}
	if raft.NodeID != 0 {
		text = append(text, "raft_id="+strconv.FormatUint(raft.NodeID, 10))
	}
	if raft.RaftAddr != "" {
		text = append(text, "raft_addr="+raft.RaftAddr)
	}
	if raft.APIAddr != "" {
		text = append(text, "raft_api_addr="+raft.APIAddr)
	}
	server, err := zeroconf.RegisterProxy(instance, zeroconfService, "local.", int(port), hostname, []string{ip}, text, nil)
	if err != nil {
		return nil, err
	}
	return &BonjourRegistration{server: server}, nil
}
func (r *BonjourRegistration) Close() {
	if r != nil && r.server != nil {
		r.server.Shutdown()
	}
}
func BrowseBonjour(ctx context.Context, window time.Duration, selfID string) ([]DiscoveredPeer, error) {
	resolver, err := zeroconf.NewResolver(nil)
	if err != nil {
		return nil, err
	}
	entries := make(chan *zeroconf.ServiceEntry)
	browseCtx, cancel := context.WithTimeout(ctx, window)
	defer cancel()
	if err = resolver.Browse(browseCtx, zeroconfService, "local.", entries); err != nil {
		return nil, err
	}
	byID := map[string]DiscoveredPeer{}
	for {
		select {
		case entry, ok := <-entries:
			if !ok {
				entries = nil
				continue
			}
			props := parseTXT(entry.Text)
			id := props["node_id"]
			if id == "" || id == selfID {
				continue
			}
			peer := DiscoveredPeer{EndpointID: id, Host: strings.TrimSuffix(entry.HostName, "."), APIPort: uint16(entry.Port)}
			if value, parseErr := strconv.ParseUint(props["raft_id"], 10, 64); parseErr == nil {
				peer.RaftID = &value
			}
			ip := firstIP(entry)
			peer.RaftAddr = resolveAdvertised(props["raft_addr"], ip)
			peer.RaftAPIAddr = resolveAdvertised(props["raft_api_addr"], ip)
			byID[id] = peer
		case <-browseCtx.Done():
			out := make([]DiscoveredPeer, 0, len(byID))
			for _, peer := range byID {
				out = append(out, peer)
			}
			return out, nil
		}
	}
}
func parseTXT(values []string) map[string]string {
	out := map[string]string{}
	for _, value := range values {
		if key, v, ok := strings.Cut(value, "="); ok {
			out[key] = v
		}
	}
	return out
}
func firstIP(entry *zeroconf.ServiceEntry) string {
	if len(entry.AddrIPv4) > 0 {
		return entry.AddrIPv4[0].String()
	}
	if len(entry.AddrIPv6) > 0 {
		return entry.AddrIPv6[0].String()
	}
	return ""
}
func resolveAdvertised(value, ip string) string {
	if value == "" {
		return ""
	}
	host, port, err := net.SplitHostPort(value)
	if err != nil {
		return value
	}
	if host == "0.0.0.0" || host == "::" || host == "[::]" {
		return net.JoinHostPort(ip, port)
	}
	return value
}
func LocalIP() string {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		return "127.0.0.1"
	}
	defer conn.Close()
	if addr, ok := conn.LocalAddr().(*net.UDPAddr); ok {
		return addr.IP.String()
	}
	return "127.0.0.1"
}
func Hostname(short string) string { return fmt.Sprintf("legion-%s.local.", short) }
