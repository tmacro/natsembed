package natsembed

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/url"
	"net"
	"sync"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

var (
	ErrServerNotRunning = errors.New("server is not running")
)

type ServerOptions struct {
	natsserver.Options
	Logger natsserver.Logger
}

var defaultNatsServerOptions = ServerOptions{
	Options: natsserver.Options{
		ServerName:             "natsembed",
		Port:                   4222,
		NoSigs:                 true,
		DisableJetStreamBanner: true,
	},
}

type ServerOption func(*ServerOptions)

type Peer struct {
	Host string
	Port int
}

func ServerName(name string) ServerOption {
	return func(opts *ServerOptions) {
		opts.ServerName = name
	}
}

func Host(host string) ServerOption {
	return func(opts *ServerOptions) {
		opts.Host = host
	}
}

func Port(port int) ServerOption {
	return func(opts *ServerOptions) {
		opts.Port = port
	}
}

func ClusterName(name string) ServerOption {
	return func(opts *ServerOptions) {
		opts.Cluster.Name = name
	}
}

func ClusterHost(host string) ServerOption {
	return func(opts *ServerOptions) {
		opts.Cluster.Host = host
	}
}

func ClusterPort(port int) ServerOption {
	return func(opts *ServerOptions) {
		opts.Cluster.Port = port
	}
}

func WithPeer(host string, port int) ServerOption {
	return func(opts *ServerOptions) {
		opts.Routes = append(opts.Routes, &url.URL{
			Scheme: "nats",
			Host:   fmt.Sprintf("%s:%d", host, port),
		})
	}
}

func WithPeers(peers []Peer) ServerOption {
	return func(opts *ServerOptions) {
		for _, peer := range peers {
			opts.Routes = append(opts.Routes, &url.URL{
				Scheme: "nats",
				Host:   fmt.Sprintf("%s:%d", peer.Host, peer.Port),
			})
		}
	}
}

func JetstreamEnabled() ServerOption {
	return func(opts *ServerOptions) {
		opts.JetStream = true
	}
}

func DebugEnabled() ServerOption {
	return func(opts *ServerOptions) {
		opts.Debug = true
	}
}

func TraceEnabled() ServerOption {
	return func(opts *ServerOptions) {
		opts.Trace = true
		opts.TraceVerbose = true
	}
}

func DontListen() ServerOption {
	return func(opts *ServerOptions) {
		opts.DontListen = true
	}
}

func Nkeys(users []*natsserver.NkeyUser) ServerOption {
	return func(opts *ServerOptions) {
		opts.Nkeys = users
	}
}

func Users(users []*natsserver.User) ServerOption {
	return func(opts *ServerOptions) {
		opts.Users = users
	}
}

func TLS(cfg *tls.Config) ServerOption {
	return func(opts *ServerOptions) {
		opts.TLSConfig = cfg
	}
}

func StoreDir(dir string) ServerOption {
	return func(opts *ServerOptions) {
		opts.StoreDir = dir
	}
}

func applyServerOptions(opts []ServerOption) (*ServerOptions, error) {
	options := defaultNatsServerOptions
	for _, opt := range opts {
		opt(&options)
	}
	if options.ServerName == "" {
		return nil, errors.New("a server name is required")
	}
	return &options, nil
}

func (p Peer) String() string {
	return fmt.Sprintf("%s:%d", p.Host, p.Port)
}

func (p Peer) URL() string {
	return fmt.Sprintf("nats://%s:%d", p.Host, p.Port)
}

var (
	server            *natsserver.Server
	serverRunning     bool
	serverConnections []net.Conn
	serverMu          *sync.RWMutex
)

func init() {
	serverMu = &sync.RWMutex{}
	serverConnections = make([]net.Conn, 0)
}

func startServer(opts *ServerOptions) error {
	serverMu.Lock()
	defer serverMu.Unlock()

	if serverRunning {
		return nil
	}

	srv, err := natsserver.NewServer(opts.Options.Clone())
	if err != nil {
		return err
	}

	if opts.Logger != nil {
		srv.SetLoggerV2(opts.Logger, opts.Debug, opts.Trace, opts.TraceVerbose)
	}

	err = natsserver.Run(srv)
	if err != nil {
		return err
	}

	server = srv
	serverRunning = true
	return nil
}

func stopServer() error {
	serverMu.Lock()
	defer serverMu.Unlock()

	if !serverRunning || server == nil {
		return nil
	}

	server.Shutdown()
	server.WaitForShutdown()
	server = nil
	serverRunning = false

	return nil
}

func closeInProcessConnections() error {
	serverMu.Lock()
	defer serverMu.Unlock()

	if serverConnections == nil {
		return nil
	}

	var errs []error
	for _, conn := range serverConnections {
		err := conn.Close()
		errs = append(errs, err)
	}

	serverConnections = nil

	return errors.Join(errs...)
}

func Run(ctx context.Context, options ...ServerOption) error {
	cfg, err := applyServerOptions(options)
	if err != nil {
		return err
	}

	err = startServer(cfg)
	if err != nil {
		return err
	}

	<-ctx.Done()

	return stopServer()
}

func Reconfigure(options ...ServerOption) error {
	cfg, err := applyServerOptions(options)
	if err != nil {
		return err
	}

	err = stopServer()
	if err != nil {
		return err
	}

	err = closeInProcessConnections()
	if err != nil {
		return err
	}

	err = startServer(cfg)
	if err != nil {
		return err
	}

	return nil
}

func Start(options ...ServerOption) error {
	return Reconfigure(options...)
}

func Stop() error {
	err := stopServer()
	if err != nil {
		return err
	}

	return closeInProcessConnections()
}

type connectionGetter struct{}

func (cg *connectionGetter) InProcessConn() (net.Conn, error) {
	serverMu.Lock()
	defer serverMu.Unlock()
	if !serverRunning {
		return nil, ErrServerNotRunning
	}

	conn, err := server.InProcessConn()
	if err != nil {
		return nil, err
	}

	serverConnections = append(serverConnections, conn)
	return conn, nil
}

func InProcessConnection(srv ...nats.InProcessConnProvider) (*nats.Conn, error) {
	options := nats.GetDefaultOptions()
	options.AllowReconnect = true
	options.RetryOnFailedConnect = true
	options.ReconnectWait = time.Second * 2
	options.MaxReconnect = -1

	if len(srv) != 0 {
		options.InProcessServer = srv[0]
	} else {
		options.InProcessServer = &connectionGetter{}
	}

	return options.Connect()
}
