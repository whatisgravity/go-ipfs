package io

import (
	"fmt"
	"os"

	"gx/ipfs/QmZy2y8t9zQH2a1b8q2ZSLKp17ATuJoCNxxyMFG5qFExpt/go-net/context"

	key "github.com/ipfs/go-ipfs/blocks/key"
	mdag "github.com/ipfs/go-ipfs/merkledag"
	format "github.com/ipfs/go-ipfs/unixfs"
	hamt "github.com/ipfs/go-ipfs/unixfs/hamt"
)

// ShardSplitThreshold specifies how large of an unsharded directory
// the Directory code will generate. Adding entries over this value will
// result in the node being restructured into a sharded object.
var ShardSplitThreshold = 1000

// DefaultShardWidth is the default value used for hamt sharding width.
var DefaultShardWidth = 256

type Directory struct {
	dserv   mdag.DAGService
	dirnode *mdag.Node

	shard *hamt.HamtShard
}

// NewDirectory returns a Directory. It needs a DAGService to add the Children
func NewDirectory(dserv mdag.DAGService) *Directory {
	db := new(Directory)
	db.dserv = dserv
	db.dirnode = format.EmptyDirNode()
	return db
}

func NewDirectoryFromNode(dserv mdag.DAGService, nd *mdag.Node) (*Directory, error) {
	pbd, err := format.FromBytes(nd.Data())
	if err != nil {
		return nil, err
	}

	switch pbd.GetType() {
	case format.TDirectory:
		return &Directory{
			dserv:   dserv,
			dirnode: nd.Copy(),
		}, nil
	case format.THAMTShard:
		shard, err := hamt.NewHamtFromDag(dserv, nd)
		if err != nil {
			return nil, err
		}

		return &Directory{
			dserv: dserv,
			shard: shard,
		}, nil
	default:
		return nil, fmt.Errorf("merkledag node was not a directory or shard")
	}
}

// AddChild adds a (name, key)-pair to the root node.
func (d *Directory) AddChild(ctx context.Context, name string, nd *mdag.Node) error {
	if d.shard == nil {
		if len(d.dirnode.Links) < ShardSplitThreshold {
			nnd := nd.Copy()
			err := d.dirnode.RemoveNodeLink(name)
			_ = err
			return d.dirnode.AddNodeLinkClean(name, nnd)
		}

		err := d.switchToSharding(ctx)
		if err != nil {
			return err
		}
	}

	return d.shard.Insert(name, nd)
}

func (d *Directory) switchToSharding(ctx context.Context) error {
	d.shard = hamt.NewHamtShard(d.dserv, DefaultShardWidth)
	for _, lnk := range d.dirnode.Links {
		cnd, err := d.dserv.Get(ctx, key.Key(lnk.Hash))
		if err != nil {
			return err
		}

		err = d.shard.Insert(lnk.Name, cnd)
		if err != nil {
			return err
		}
	}

	d.dirnode = nil
	return nil
}

func (d *Directory) Links() ([]*mdag.Link, error) {
	if d.shard == nil {
		return d.dirnode.Links, nil
	}

	return d.shard.EnumLinks()
}

func (d *Directory) Find(ctx context.Context, name string) (*mdag.Node, error) {
	if d.shard == nil {
		lnk, err := d.dirnode.GetNodeLink(name)
		switch err {
		case mdag.ErrLinkNotFound:
			return nil, os.ErrNotExist
		default:
			return nil, err
		case nil:
		}

		return d.dserv.Get(ctx, key.Key(lnk.Hash))
	}

	return d.shard.Find(name)
}

func (d *Directory) RemoveChild(ctx context.Context, name string) error {
	if d.shard == nil {
		return d.dirnode.RemoveNodeLink(name)
	}

	return d.shard.Remove(name)
}

// GetNode returns the root of this Directory
func (d *Directory) GetNode() (*mdag.Node, error) {
	if d.shard == nil {
		return d.dirnode, nil
	}

	return d.shard.Node()
}
