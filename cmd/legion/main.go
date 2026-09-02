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

	"github.com/rcarmo/legion/internal/agent"
	"github.com/rcarmo/legion/internal/api"
	"github.com/rcarmo/legion/internal/cluster"
	"github.com/rcarmo/legion/internal/core"
	"github.com/rcarmo/legion/internal/deploy"
	legionns "github.com/rcarmo/legion/internal/namespace"
	"github.com/rcarmo/legion/internal/raftstore"
	legionruntime "github.com/rcarmo/legion/internal/runtime"
	bunruntime "github.com/rcarmo/legion/internal/runtime/bun"
	"github.com/rcarmo/legion/internal/runtime/joker"
	wasmruntime "github.com/rcarmo/legion/internal/runtime/wasm"
	legiontelemetry "github.com/rcarmo/legion/internal/telemetry"
	"github.com/tmc/go-iroh/blobs"
	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/key"
	"github.com/tmc/go-iroh/netaddr"
)

var version = "dev"

type runtimeNamespaceAdapter struct{ target **legionns.LegionNamespace }

func (a runtimeNamespaceAdapter) Read(ctx context.Context, path string) ([]byte, error) {
	if a.target == nil || *a.target == nil {
		return nil, fmt.Errorf("namespace unavailable")
	}
	return (*a.target).Read(ctx, path)
}
func (a runtimeNamespaceAdapter) Write(ctx context.Context, path string, data []byte) ([]byte, error) {
	if a.target == nil || *a.target == nil {
		return nil, fmt.Errorf("namespace unavailable")
	}
	return (*a.target).Write(ctx, path, data)
}

func firstEnv(names ...string) string {
	for _, name := range names {
		if value := os.Getenv(name); value != "" {
			return value
		}
	}
	return ""
}
func envInt(name string, fallback int) int {
	if value, err := strconv.Atoi(os.Getenv(name)); err == nil {
		return value
	}
	return fallback
}
func envDurationMS(name string, fallback time.Duration) time.Duration {
	if value, err := strconv.ParseInt(os.Getenv(name), 10, 64); err == nil {
		return time.Duration(value) * time.Millisecond
	}
	return fallback
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
func run() error {
	if handled, err := runCLI(os.Args[1:]); handled {
		return err
	}
	dataDir := flag.String("data-dir", "./data", "persistent node state")
	irohAddr := flag.String("iroh-addr", "[::]:0", "iroh UDP bind address")
	raftAddr := flag.String("raft-addr", "127.0.0.1:7000", "stable Raft TCP address")
	apiAddr := flag.String("api-addr", "127.0.0.1:8080", "cluster control and REST HTTP address")
	ninepAddr := flag.String("9p-addr", "127.0.0.1:5640", "loopback 9P TCP address (empty disables)")
	capability := flag.String("9p-capability", firstEnv("LEGION_NAMESPACE_CAPABILITY", "LEGION_9P_CAPABILITY"), "9P attach capability token")
	apiKey := flag.String("api-key", os.Getenv("LEGION_API_KEY"), "REST API bearer key")
	sessionRateMax := flag.Int("session-rate-max", envInt("LEGION_SESSION_MAX_REQUESTS_PER_WINDOW", 30), "maximum execution requests per session window (0 disables)")
	sessionRateWindow := flag.Duration("session-rate-window", envDurationMS("LEGION_SESSION_RATE_WINDOW_MS", time.Minute), "session rate-limit window")
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
	tree := legionns.NewTree()
	agentLoop := agent.New(routed, core.EchoToolRegistry{}, agent.GoAI{})
	registry, err := deploy.Open(filepath.Join(*dataDir, "functions"))
	if err != nil {
		return fmt.Errorf("deployment registry: %w", err)
	}
	telemetryProviders, err := legiontelemetry.Init(ctx, "legion", node.ShortID())
	if err != nil {
		return fmt.Errorf("telemetry: %w", err)
	}
	defer telemetryProviders.Shutdown(context.Background())
	limits := legionruntime.DefaultLimits()
	limits.Timeout = envDurationMS("LEGION_INVOKE_TIMEOUT_MS", limits.Timeout)
	limits.MaxInputBytes = envInt("LEGION_INVOKE_MAX_INPUT_BYTES", limits.MaxInputBytes)
	limits.MaxOutputBytes = envInt("LEGION_INVOKE_MAX_OUTPUT_BYTES", limits.MaxOutputBytes)
	limits.MaxConcurrentPerFunction = envInt("LEGION_INVOKE_MAX_CONCURRENT_PER_FUNCTION", limits.MaxConcurrentPerFunction)
	limits.MaxRequestsPerWindow = envInt("LEGION_INVOKE_MAX_REQUESTS_PER_WINDOW", limits.MaxRequestsPerWindow)
	limits.RateWindow = envDurationMS("LEGION_INVOKE_RATE_WINDOW_MS", limits.RateWindow)
	var runtimeNamespace *legionns.LegionNamespace
	wasmHost := runtimeNamespaceAdapter{target: &runtimeNamespace}
	wasm := wasmruntime.New(registry.CAS(), wasmHost, nil, limits)
	defer wasm.Close(context.Background())
	bun := bunruntime.New("", registry.CAS(), limits)
	defer bun.Close()
	jokerRuntime := joker.New("", registry.CAS(), limits)
	functionMetrics := legiontelemetry.NewFunctionMetrics()
	functions := legionruntime.Functions{Registry: registry, WASM: legionruntime.ObservedInvoker{Inner: legionruntime.NewBoundedInvoker(wasm, limits), Observer: functionMetrics}, Bun: legionruntime.ObservedInvoker{Inner: legionruntime.NewBoundedInvoker(bun, limits), Observer: functionMetrics}, Joker: legionruntime.ObservedInvoker{Inner: legionruntime.NewBoundedInvoker(jokerRuntime, limits), Observer: functionMetrics}}
	deployResources := deploy.Resources{Registry: registry, OnRegister: func(manifest legionruntime.Manifest) {
		tree.EnsureDir("/fn/" + manifest.Name)
		_ = tree.SetJSON("/fn/"+manifest.Name+"/manifest.json", manifest)
	}}
	for _, name := range registry.Names() {
		if manifest, ok := registry.Manifest(name); ok {
			deployResources.OnRegister(manifest)
		}
	}
	namespace := legionns.New(tree).WithResources(legionns.NewSessionResources(routed, agentLoop)).WithDeploy(deployResources).WithFunctions(functions).WithCluster(legionns.LiveCluster{Node: node, Store: store, Tree: tree})
	runtimeNamespace = namespace
	if *capability != "" {
		namespace.WithCapability([]byte(*capability))
	}
	mux := http.NewServeMux()
	sessionLimiter := api.NewSessionLimiter(*sessionRateMax, *sessionRateWindow)
	metrics := legiontelemetry.StoreMetrics{Store: routed, Functions: functionMetrics, SessionRejections: sessionLimiter.Rejections}
	mux.Handle("/", legionns.REST{Namespace: namespace, APIKey: *apiKey, SessionRateLimiter: sessionLimiter, Metrics: metrics}.Handler())
	mux.Handle("/raft/", cluster.ControlServer{Store: store}.Handler())
	mux.Handle("/store", cluster.ControlServer{Store: store}.Handler())
	server := &http.Server{Addr: *apiAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
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
	membership, err := cluster.StartMembershipWithProtocols(ctx, node, bootstrapAddrs, 5*time.Second, func(peer cluster.NodePresence) {
		log.Printf("peer joined: %s", peer.ShortID)
		if endpointID, parseErr := key.ParseEndpointID(peer.EndpointID); parseErr == nil {
			namespace.RegisterPeer(peer.EndpointID, legionns.RemotePeer{Dial: func(callCtx context.Context) (*legionns.Client, error) {
				return legionns.DialIroh(callCtx, node.Endpoint, netaddr.NewEndpointAddr(endpointID), *capability)
			}})
		}
	}, func(peer string) {
		log.Printf("peer left: %s", peer)
		namespace.UnregisterPeer(peer)
	}, map[string]iroh.ProtocolHandler{legionns.ALPN: namespace.Handler(), blobs.ALPN: registry.CAS().Handler()})
	if err != nil {
		return err
	}
	defer membership.Close(context.Background())
	if *ninepAddr != "" {
		listener, listenErr := net.Listen("tcp", *ninepAddr)
		if listenErr != nil {
			return fmt.Errorf("9p listen: %w", listenErr)
		}
		defer listener.Close()
		go func() {
			if serveError := namespace.ServeTCP(ctx, listener); serveError != nil && ctx.Err() == nil {
				log.Printf("9p server: %v", serveError)
			}
		}()
	}
	log.Printf("legion %s node=%s raft=%d raft_addr=%s api=%s 9p=%s mode=%s", version, node.ShortID(), raftID, *raftAddr, *apiAddr, *ninepAddr, bootstrap.Kind)
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
