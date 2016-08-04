package hamt

import (
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"gx/ipfs/QmZy2y8t9zQH2a1b8q2ZSLKp17ATuJoCNxxyMFG5qFExpt/go-net/context"

	key "github.com/ipfs/go-ipfs/blocks/key"
	dag "github.com/ipfs/go-ipfs/merkledag"
	mdtest "github.com/ipfs/go-ipfs/merkledag/test"
	ft "github.com/ipfs/go-ipfs/unixfs"
)

func shuffle(arr []string) {
	for i := 0; i < len(arr); i++ {
		a := rand.Intn(len(arr))
		b := rand.Intn(len(arr))
		arr[a], arr[b] = arr[b], arr[a]
	}
}

func makeDir(ds dag.DAGService, size int) ([]string, *HamtShard, error) {
	s := NewHamtShard(ds, 256)

	var dirs []string
	for i := 0; i < size; i++ {
		dirs = append(dirs, fmt.Sprintf("DIRNAME%d", i))
	}

	rand.Seed(time.Now().UnixNano())
	shuffle(dirs)

	for i := 0; i < len(dirs); i++ {
		nd := ft.EmptyDirNode()
		ds.Add(nd)
		err := s.Insert(dirs[i], nd)
		if err != nil {
			return nil, nil, err
		}
	}

	return dirs, s, nil
}
func assertSerializationWorks(ds dag.DAGService, s *HamtShard) error {
	nd, err := s.Node()
	if err != nil {
		return err
	}

	nds, err := NewHamtFromDag(ds, nd)
	if err != nil {
		return err
	}

	linksA, err := s.EnumLinks()
	if err != nil {
		return err
	}

	linksB, err := nds.EnumLinks()
	if err != nil {
		return err
	}

	if len(linksA) != len(linksB) {
		return fmt.Errorf("links arrays are different sizes")
	}

	for i, a := range linksA {
		b := linksB[i]
		if a.Name != b.Name {
			return fmt.Errorf("links names mismatch")
		}

		if a.Hash.B58String() != b.Hash.B58String() {
			return fmt.Errorf("link hashes dont match")
		}

		if a.Size != b.Size {
			return fmt.Errorf("link sizes not the same")
		}
	}

	return nil
}

func TestBasicInsert(t *testing.T) {
	ds := mdtest.Mock()
	names, s, err := makeDir(ds, 1000)
	if err != nil {
		t.Fatal(err)
	}

	for _, d := range names {
		_, err := s.Find(d)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestDirBuilding(t *testing.T) {
	ds := mdtest.Mock()
	s := NewHamtShard(ds, 256)

	_, s, err := makeDir(ds, 200)
	if err != nil {
		t.Fatal(err)
	}

	nd, err := s.Node()
	if err != nil {
		t.Fatal(err)
	}

	//printDag(ds, nd, 0)

	k, err := nd.Key()
	if err != nil {
		t.Fatal(err)
	}

	if k.B58String() != "QmPF1FH8RuW8fQTgL6GUqkkGKpJEkpScnMWfGxaPdegwYS" {
		t.Fatalf("output didnt match what we expected (got %s)", k.B58String())
	}
}

func TestShardReload(t *testing.T) {
	ds := mdtest.Mock()
	s := NewHamtShard(ds, 256)

	_, s, err := makeDir(ds, 200)
	if err != nil {
		t.Fatal(err)
	}

	nd, err := s.Node()
	if err != nil {
		t.Fatal(err)
	}

	nds, err := NewHamtFromDag(ds, nd)
	if err != nil {
		t.Fatal(err)
	}

	lnks, err := nds.EnumLinks()
	if err != nil {
		t.Fatal(err)
	}

	if len(lnks) != 200 {
		t.Fatal("not enough links back")
	}

	_, err = nds.Find("DIRNAME50")
	if err != nil {
		t.Fatal(err)
	}

	// Now test roundtrip marshal with no operations

	nds, err = NewHamtFromDag(ds, nd)
	if err != nil {
		t.Fatal(err)
	}

	ond, err := nds.Node()
	if err != nil {
		t.Fatal(err)
	}

	outk, err := ond.Key()
	if err != nil {
		t.Fatal(err)
	}

	ndk, err := nd.Key()
	if err != nil {
		t.Fatal(err)
	}

	if outk != ndk {
		t.Fatal("roundtrip serialization failed")
	}
}

func TestRemoveElems(t *testing.T) {
	ds := mdtest.Mock()
	dirs, s, err := makeDir(ds, 500)
	if err != nil {
		t.Fatal(err)
	}

	shuffle(dirs)

	for _, d := range dirs {
		err := s.Remove(d)
		if err != nil {
			t.Fatal(err)
		}
	}

	nd, err := s.Node()
	if err != nil {
		t.Fatal(err)
	}

	if len(nd.Links) > 0 {
		t.Fatal("shouldnt have any links here")
	}

	err = s.Remove("doesnt exist")
	if err != os.ErrNotExist {
		t.Fatal("expected error does not exist")
	}
}

func TestInsertAfterMarshal(t *testing.T) {
	ds := mdtest.Mock()
	_, s, err := makeDir(ds, 300)
	if err != nil {
		t.Fatal(err)
	}

	nd, err := s.Node()
	if err != nil {
		t.Fatal(err)
	}

	nds, err := NewHamtFromDag(ds, nd)
	if err != nil {
		t.Fatal(err)
	}

	empty := ft.EmptyDirNode()
	for i := 0; i < 100; i++ {
		err := nds.Insert(fmt.Sprintf("moredirs%d", i), empty)
		if err != nil {
			t.Fatal(err)
		}
	}

	links, err := nds.EnumLinks()
	if err != nil {
		t.Fatal(err)
	}

	if len(links) != 400 {
		t.Fatal("expected 400 links")
	}

	err = assertSerializationWorks(ds, nds)
	if err != nil {
		t.Fatal(err)
	}
}

func TestDuplicateAddShard(t *testing.T) {
	ds := mdtest.Mock()
	dir := NewHamtShard(ds, 256)
	nd := ft.EmptyDirNode()

	err := dir.Insert("test", nd)
	if err != nil {
		t.Fatal(err)
	}

	err = dir.Insert("test", nd)
	if err != nil {
		t.Fatal(err)
	}

	lnks, err := dir.EnumLinks()
	if err != nil {
		t.Fatal(err)
	}

	if len(lnks) != 1 {
		t.Fatal("expected only one link")
	}
}

func TestLoadFailsFromNonShard(t *testing.T) {
	ds := mdtest.Mock()
	nd := ft.EmptyDirNode()

	_, err := NewHamtFromDag(ds, nd)
	if err == nil {
		t.Fatal("expected dir shard creation to fail when given normal directory")
	}

	nd = new(dag.Node)

	_, err = NewHamtFromDag(ds, nd)
	if err == nil {
		t.Fatal("expected dir shard creation to fail when given normal directory")
	}
}

func TestFindNonExisting(t *testing.T) {
	ds := mdtest.Mock()
	_, s, err := makeDir(ds, 100)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 200; i++ {
		_, err := s.Find(fmt.Sprintf("notfound%d", i))
		if err != os.ErrNotExist {
			t.Fatal("expected ErrNotExist")
		}
	}
}

func TestRemoveElemsAfterMarshal(t *testing.T) {
	ds := mdtest.Mock()
	dirs, s, err := makeDir(ds, 30)
	if err != nil {
		t.Fatal(err)
	}

	sort.Strings(dirs)

	err = s.Remove(dirs[0])
	if err != nil {
		t.Fatal(err)
	}

	out, err := s.Find(dirs[0])
	if err == nil {
		t.Fatal("expected error, got: ", out)
	}

	nd, err := s.Node()
	if err != nil {
		t.Fatal(err)
	}

	nds, err := NewHamtFromDag(ds, nd)
	if err != nil {
		t.Fatal(err)
	}

	_, err = nds.Find(dirs[0])
	if err == nil {
		t.Fatal("expected not to find ", dirs[0])
	}

	for _, d := range dirs[1:] {
		_, err := nds.Find(d)
		if err != nil {
			t.Fatal("could not find expected link after unmarshaling")
		}
	}

	for _, d := range dirs[1:] {
		err := nds.Remove(d)
		if err != nil {
			t.Fatal(err)
		}
	}

	links, err := nds.EnumLinks()
	if err != nil {
		t.Fatal(err)
	}

	if len(links) != 0 {
		t.Fatal("expected all links to be removed")
	}

	err = assertSerializationWorks(ds, nds)
	if err != nil {
		t.Fatal(err)
	}
}

func TestBitfieldIndexing(t *testing.T) {
	ds := mdtest.Mock()
	s := NewHamtShard(ds, 256)

	set := func(i int) {
		s.bitfield.SetBit(s.bitfield, i, 1)
	}

	assert := func(i int, val int) {
		if s.indexForBitPos(i) != val {
			t.Fatalf("expected index %d to be %d", i, val)
		}
	}

	assert(50, 0)
	set(4)
	set(5)
	set(60)

	assert(10, 2)
	set(3)
	assert(10, 3)
	assert(1, 0)

	assert(100, 4)
	set(50)
	assert(45, 3)
	set(100)
	assert(100, 5)
}

func printDag(ds dag.DAGService, nd *dag.Node, depth int) {
	padding := strings.Repeat(" ", depth)
	fmt.Println("{")
	for _, l := range nd.Links {
		fmt.Printf("%s%s: %s", padding, l.Name, l.Hash.B58String())
		ch, err := ds.Get(context.Background(), key.Key(l.Hash))
		if err != nil {
			panic(err)
		}

		printDag(ds, ch, depth+1)
	}
	fmt.Println(padding + "}")
}
