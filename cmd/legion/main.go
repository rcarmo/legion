package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/rcarmo/legion/internal/cluster"
	"github.com/rcarmo/legion/internal/raftstore"
	"github.com/tmc/go-iroh/key"
	"github.com/tmc/go-iroh/netaddr"
)

var version = "dev"

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
func run() error {
	dataDir := flag.String("data-dir", "./data", "persistent node state")
	irohAddr := flag.String("iroh-addr", "[::]:0", "iroh UDP bind address")
	raftAddr := flag.String("raft-addr", "127.0.0.1:7000", "stable Raft TCP address")
	apiAddr := flag.String("api-addr", "127.0.0.1:8080", "cluster control HTTP address")
	mdns := flag.Bool("mdns", true, "enable LAN discovery")
	relay := flag.Bool("relay", true, "enable iroh relay transport")
	discoveryWindow := flag.Duration("discovery-window", 3*time.Second, "Bonjour discovery window")
	flag.Parse()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	_, apiPortText, err := net.SplitHostPort(*apiAddr)
	if err != nil {
		return fmt.Errorf("api address: %w", err)
	}
	apiPort, err := strconv.ParseUint(apiPortText, 10, 16)
	if err != nil {
		return fmt.Errorf("api port: %w", err)
	}
	node, err := cluster.StartNode(ctx, cluster.NodeConfig{DataDir: *dataDir, BindAddr: *irohAddr, APIPort: uint16(apiPort), MDNS: *mdns, Relay: *relay})
	if err != nil {
		return err
	}
	defer node.Close(context.Background())
	raftID, err := cluster.LoadOrGenerateRaftID(*dataDir)
	if err != nil {
		return err
	}
	bootstrap, err := cluster.RunBootstrap(ctx, node, cluster.RaftAdvertisement{NodeID: raftID, RaftAddr: *raftAddr, APIAddr: *apiAddr}, *discoveryWindow, version)
	if err != nil {
		return err
	}
	if bootstrap.Registration != nil {
		defer bootstrap.Registration.Close()
	}
	store, err := raftstore.Open(raftstore.Config{NodeID: strconv.FormatUint(raftID, 10), DataDir: filepath.Join(*dataDir, "raft"), BindAddr: *raftAddr, Bootstrap: bootstrap.Kind == cluster.Bootstrap})
	if err != nil {
		return err
	}
	defer store.Close()
	directory := cluster.NewDirectory()
	directory.Add(*raftAddr, *apiAddr)
	directory.AddPeers(bootstrap.Peers)
	routed := cluster.NewRoutedStore(store, directory)
	server := &http.Server{Addr: *apiAddr, Handler: cluster.ControlServer{Store: store}.Handler(), ReadHeaderTimeout: 5 * time.Second}
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.ListenAndServe() }()
	if target, joinErr := cluster.RaftJoinTarget(bootstrap); joinErr != nil {
		return joinErr
	} else if target != nil {
		if joinErr = routed.Join(ctx, raftID, *raftAddr, target.RaftAPIAddr); joinErr != nil {
			return joinErr
		}
	}
	bootstrapAddrs := make([]netaddr.EndpointAddr, 0, len(bootstrap.Peers))
	for _, peer := range bootstrap.Peers {
		id, parseErr := key.ParseEndpointID(peer.EndpointID)
		if parseErr == nil {
			bootstrapAddrs = append(bootstrapAddrs, netaddr.NewEndpointAddr(id))
		}
	}
	membership, err := cluster.StartMembership(ctx, node, bootstrapAddrs, 5*time.Second, func(peer cluster.NodePresence) { log.Printf("peer joined: %s", peer.ShortID) }, func(peer string) { log.Printf("peer left: %s", peer) })
	if err != nil {
		return err
	}
	defer membership.Close(context.Background())
	log.Printf("legion %s node=%s raft=%d raft_addr=%s api=%s mode=%s", version, node.ShortID(), raftID, *raftAddr, *apiAddr, bootstrap.Kind)
	select {
	case <-ctx.Done():
		shutdownCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		return server.Shutdown(shutdownCtx)
	case err = <-serveErr:
		if err == http.ErrServerClosed {
			return nil
		}
		return fmt.Errorf("control server: %w", err)
	}
}
