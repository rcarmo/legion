// Package deploy implements persistent content-addressed function deployment.
package deploy

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/tmc/go-iroh/blobs"
	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/netaddr"
)

type CAS struct {
	store *blobs.FSStore
	root  string
}

func OpenCAS(root string) (*CAS, error) {
	s, e := blobs.NewFSStore(root)
	if e != nil {
		return nil, e
	}
	return &CAS{store: s, root: root}, nil
}
func (c *CAS) Put(_ context.Context, b []byte) (string, error) {
	h, e := c.store.Add(b)
	if e != nil {
		return "", e
	}
	if e = c.store.SetTag("artifact/"+h.String(), blobs.RawHash(h)); e != nil {
		return "", e
	}
	return h.String(), nil
}
func (c *CAS) Get(ctx context.Context, cid string) ([]byte, error) {
	h, e := blobs.ParseHash(cid)
	if e != nil {
		return nil, e
	}
	return blobs.ReadBlob(ctx, c.store, h)
}
func (c *CAS) Fetch(ctx context.Context, cid string) ([]byte, error) { return c.Get(ctx, cid) }
func (c *CAS) CachedPath(ctx context.Context, cid, extension string) (string, error) {
	if strings.ContainsAny(extension, `/\\`) {
		return "", fmt.Errorf("invalid extension")
	}
	path := filepath.Join(c.root, ".artifacts", cid, "index."+extension)
	if _, err := os.Stat(path); err == nil {
		return path, nil
	}
	return c.Materialize(ctx, cid, path)
}
func (c *CAS) Materialize(ctx context.Context, cid, path string) (string, error) {
	b, e := c.Get(ctx, cid)
	if e != nil {
		return "", e
	}
	if e = os.MkdirAll(filepath.Dir(path), 0755); e != nil {
		return "", e
	}
	if e = os.WriteFile(path, b, 0644); e != nil {
		return "", e
	}
	return path, nil
}
func (c *CAS) Ticket(cid string, addr netaddr.EndpointAddr) (string, error) {
	h, e := blobs.ParseHash(cid)
	if e != nil {
		return "", e
	}
	return blobs.NewTicket(addr, h, blobs.Raw).String(), nil
}
func (c *CAS) Store() blobs.Store { return c.store }
func (c *CAS) Sink() blobs.Sink   { return c.store }
func (c *CAS) Handler() iroh.ProtocolHandler {
	return iroh.ProtocolHandlerFunc(func(ctx context.Context, conn *iroh.Conn) error {
		return blobs.ServeBlobStreams(ctx, func(streamCtx context.Context) (blobs.BidiStream, error) {
			return conn.AcceptStream(streamCtx)
		}, c.store)
	})
}
func FetchTicket(ctx context.Context, ep *iroh.Endpoint, ticketText string) ([]byte, error) {
	ticket, e := blobs.ParseTicket(ticketText)
	if e != nil {
		return nil, e
	}
	conn, e := ep.Connect(ctx, ticket.Addr(), blobs.ALPN)
	if e != nil {
		return nil, e
	}
	defer conn.CloseWithError(0, "")
	s, e := conn.OpenStreamSync(ctx)
	if e != nil {
		return nil, e
	}
	var out bytes.Buffer
	if e = blobs.DownloadBlob(ctx, s, ticket.Hash(), &out); e != nil {
		return nil, e
	}
	return out.Bytes(), nil
}
