package io

import (
	"gx/ipfs/QmZy2y8t9zQH2a1b8q2ZSLKp17ATuJoCNxxyMFG5qFExpt/go-net/context"

	dag "github.com/ipfs/go-ipfs/merkledag"
	ft "github.com/ipfs/go-ipfs/unixfs"
	hamt "github.com/ipfs/go-ipfs/unixfs/hamt"
)

func ResolveUnixfsOnce(ctx context.Context, ds dag.DAGService, nd *dag.Node, name string) (*dag.Link, error) {
	upb, err := ft.FromBytes(nd.Data())
	if err != nil {
		// Not a unixfs node, use standard object traversal code
		return nd.GetNodeLink(name)
	}

	switch upb.GetType() {
	case ft.THAMTShard:
		s, err := hamt.NewHamtFromDag(ds, nd)
		if err != nil {
			return nil, err
		}

		// TODO: optimized routine on HAMT for returning a dag.Link to avoid extra disk hits
		out, err := s.Find(name)
		if err != nil {
			return nil, err
		}

		return dag.MakeLink(out)
	default:
		return nd.GetNodeLink(name)
	}
}
