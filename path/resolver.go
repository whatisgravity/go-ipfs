// Package path implements utilities for resolving paths within ipfs.
package path

import (
	"errors"
	"fmt"
	"time"

	mh "gx/ipfs/QmYf7ng2hG5XBtJA3tN34DQ2GUN5HNksEw1rLDkmr6vGku/go-multihash"
	"gx/ipfs/QmZy2y8t9zQH2a1b8q2ZSLKp17ATuJoCNxxyMFG5qFExpt/go-net/context"

	key "github.com/ipfs/go-ipfs/blocks/key"
	dag "github.com/ipfs/go-ipfs/merkledag"
	logging "gx/ipfs/QmNQynaz7qfriSUJkiEZUrm2Wen1u3Kj9goZzWtrPyu7XR/go-log"
)

var log = logging.Logger("path")

// Paths after a protocol must contain at least one component
var ErrNoComponents = errors.New(
	"path must contain at least one component")

// ErrNoLink is returned when a link is not found in a path
type ErrNoLink struct {
	Name string
	Node mh.Multihash
}

func (e ErrNoLink) Error() string {
	return fmt.Sprintf("no link named %q under %s", e.Name, e.Node.B58String())
}

// Resolver provides path resolution to IPFS
// It has a pointer to a DAGService, which is uses to resolve nodes.
// TODO: now that this is more modular, try to unify this code with the
//       the resolvers in namesys
type Resolver struct {
	DAG dag.DAGService

	ResolveOnce func(ctx context.Context, ds dag.DAGService, nd *dag.Node, name string) (*dag.Link, error)
}

func NewBasicResolver(ds dag.DAGService) *Resolver {
	return &Resolver{
		DAG:         ds,
		ResolveOnce: ResolveSingle,
	}
}

// SplitAbsPath clean up and split fpath. It extracts the first component (which
// must be a Multihash) and return it separately.
func SplitAbsPath(fpath Path) (mh.Multihash, []string, error) {

	log.Debugf("Resolve: '%s'", fpath)

	parts := fpath.Segments()
	if parts[0] == "ipfs" {
		parts = parts[1:]
	}

	// if nothing, bail.
	if len(parts) == 0 {
		return nil, nil, ErrNoComponents
	}

	// first element in the path is a b58 hash (for now)
	h, err := mh.FromB58String(parts[0])
	if err != nil {
		log.Debug("given path element is not a base58 string.\n")
		return nil, nil, err
	}

	return h, parts[1:], nil
}

// ResolvePath fetches the node for given path. It returns the last item
// returned by ResolvePathComponents.
func (s *Resolver) ResolvePath(ctx context.Context, fpath Path) (*dag.Node, error) {
	// validate path
	if err := fpath.IsValid(); err != nil {
		return nil, err
	}

	nodes, err := s.ResolvePathComponents(ctx, fpath)
	if err != nil || nodes == nil {
		return nil, err
	}
	return nodes[len(nodes)-1], err
}

func ResolveSingle(ctx context.Context, ds dag.DAGService, nd *dag.Node, name string) (*dag.Link, error) {
	return nd.GetNodeLink(name)
}

// ResolvePathComponents fetches the nodes for each segment of the given path.
// It uses the first path component as a hash (key) of the first node, then
// resolves all other components walking the links, with ResolveLinks.
func (s *Resolver) ResolvePathComponents(ctx context.Context, fpath Path) ([]*dag.Node, error) {
	h, parts, err := SplitAbsPath(fpath)
	if err != nil {
		return nil, err
	}

	log.Debug("resolve dag get")
	nd, err := s.DAG.Get(ctx, key.Key(h))
	if err != nil {
		return nil, err
	}

	return s.ResolveLinks(ctx, nd, parts)
}

// ResolveLinks iteratively resolves names by walking the link hierarchy.
// Every node is fetched from the DAGService, resolving the next name.
// Returns the list of nodes forming the path, starting with ndd. This list is
// guaranteed never to be empty.
//
// ResolveLinks(nd, []string{"foo", "bar", "baz"})
// would retrieve "baz" in ("bar" in ("foo" in nd.Links).Links).Links
func (s *Resolver) ResolveLinks(ctx context.Context, ndd *dag.Node, names []string) ([]*dag.Node, error) {

	result := make([]*dag.Node, 0, len(names)+1)
	result = append(result, ndd)
	nd := ndd // dup arg workaround

	// for each of the path components
	for _, name := range names {

		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Minute)
		defer cancel()

		lnk, err := s.ResolveOnce(ctx, s.DAG, nd, name)
		if err == dag.ErrLinkNotFound {
			n, _ := nd.Multihash()
			return result, ErrNoLink{Name: name, Node: n}
		} else if err != nil {
			return result, err
		}

		nextnode, err := lnk.GetNode(ctx, s.DAG)
		if err != nil {
			return result, err
		}

		nd = nextnode
		result = append(result, nextnode)
	}
	return result, nil
}
