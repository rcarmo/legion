package namespace

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"

	"github.com/hugelgupf/p9/p9"
	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/netaddr"
)

// Handler serves every bidirectional stream on an authenticated iroh 9P connection.
func (n *LegionNamespace) Handler() iroh.ProtocolHandler {
	server := p9.NewServer(n)
	return iroh.ProtocolHandlerFunc(func(ctx context.Context, conn *iroh.Conn) error {
		for {
			stream, err := conn.AcceptStream(ctx)
			if err != nil {
				if ctx.Err() != nil || strings.Contains(err.Error(), "closed") {
					return nil
				}
				return err
			}
			go func() { _ = server.Handle(stream, stream) }()
		}
	})
}

// ServeTCP exposes the same namespace on a loopback listener.
func (n *LegionNamespace) ServeTCP(ctx context.Context, listener net.Listener) error {
	return p9.NewServer(n).ServeContext(ctx, listener)
}

type Client struct {
	p9   *p9.Client
	root p9.File
}

func NewClient(conn io.ReadWriteCloser, capability string) (*Client, error) {
	client, err := p9.NewClient(conn)
	if err != nil {
		return nil, err
	}
	name := ""
	if capability != "" {
		name = "cap=" + capability
	}
	root, err := client.Attach(name)
	if err != nil {
		_ = client.Close()
		return nil, err
	}
	return &Client{p9: client, root: root}, nil
}
func DialIroh(ctx context.Context, endpoint *iroh.Endpoint, addr netaddr.EndpointAddr, capability string) (*Client, error) {
	conn, err := endpoint.Connect(ctx, addr, ALPN)
	if err != nil {
		return nil, err
	}
	stream, err := conn.OpenStreamConn(ctx)
	if err != nil {
		return nil, err
	}
	return NewClient(stream, capability)
}
func (c *Client) Close() error {
	if c.root != nil {
		_ = c.root.Close()
	}
	return c.p9.Close()
}
func (c *Client) walk(p string) (p9.File, error) {
	names := strings.FieldsFunc(p, func(r rune) bool { return r == '/' })
	_, f, err := c.root.Walk(names)
	return f, err
}
func (c *Client) Read(p string) ([]byte, error) {
	f, err := c.walk(p)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	if _, _, err = f.Open(p9.ReadOnly); err != nil {
		return nil, err
	}
	buf := make([]byte, 64<<10)
	n, err := f.ReadAt(buf, 0)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	return append([]byte(nil), buf[:n]...), nil
}
func (c *Client) Write(p string, data []byte) ([]byte, error) {
	f, err := c.walk(p)
	if err != nil {
		return nil, err
	}
	if _, _, err = f.Open(p9.WriteOnly); err != nil {
		_ = f.Close()
		return nil, err
	}
	n, err := f.WriteAt(data, 0)
	_ = f.Close()
	if err != nil {
		return nil, err
	}
	if n != len(data) {
		return nil, io.ErrShortWrite
	}
	return c.Read(p)
}
func (c *Client) List(p string) ([]string, error) {
	f, err := c.walk(p)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	if _, _, err = f.Open(p9.ReadOnly); err != nil {
		return nil, err
	}
	var names []string
	var off uint64
	for {
		ds, e := f.Readdir(off, 128)
		for _, d := range ds {
			names = append(names, d.Name)
			off = d.Offset
		}
		if e != nil {
			if errors.Is(e, io.EOF) {
				return names, nil
			}
			return nil, e
		}
		if len(ds) == 0 {
			return names, nil
		}
	}
}

type RemotePeer struct {
	Dial func(context.Context) (*Client, error)
}

func (r RemotePeer) Read(ctx context.Context, p string) ([]byte, error) {
	c, e := r.Dial(ctx)
	if e != nil {
		return nil, e
	}
	defer c.Close()
	return c.Read(p)
}
func (r RemotePeer) Write(ctx context.Context, p string, b []byte) ([]byte, error) {
	c, e := r.Dial(ctx)
	if e != nil {
		return nil, e
	}
	defer c.Close()
	return c.Write(p, b)
}

var _ Peer = RemotePeer{}
var _ = fmt.Sprintf
