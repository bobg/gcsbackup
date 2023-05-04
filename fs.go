package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"cloud.google.com/go/storage"
	"github.com/bobg/gcsobj"
	"github.com/pkg/errors"
	"google.golang.org/api/iterator"
)

func (c maincmd) doFS(ctx context.Context, name, listfile, mountpoint string, _ []string) error {
	start := time.Now()

	log.Print("Building file system, please wait")
	f, err := newFS(ctx, c.bucket, listfile)
	if err != nil {
		return errors.Wrap(err, "building filesystem")
	}

	conn, err := fuse.Mount(
		mountpoint,
		fuse.FSName(name),
		fuse.Subtype("gcsbackup"),
		fuse.ReadOnly(),
	)
	if err != nil {
		return errors.Wrapf(err, "mounting filesystem")
	}
	defer conn.Close()

	log.Printf("File system built in %s, now serving", time.Since(start))
	return fs.Serve(conn, f)
}

type FS struct {
	bucket *storage.BucketHandle
	root   *FSNode

	mu        sync.Mutex // protects nextInode
	nextInode uint64
}

var _ fs.FS = &FS{}

func newFS(ctx context.Context, bucket *storage.BucketHandle, fromfile string) (*FS, error) {
	f := &FS{
		bucket:    bucket,
		nextInode: 2,
	}
	f.root = &FSNode{
		fs:       f,
		inode:    1,
		children: make(map[string]*FSNode),
	}

	if fromfile == "" {
		// Build filesystem from a scan of the bucket.

		var (
			query = &storage.Query{Projection: storage.ProjectionNoACL}
			it    = f.bucket.Objects(ctx, query)
		)
		for {
			attrs, err := it.Next()
			if errors.Is(err, iterator.Done) {
				return f, nil
			}
			if err != nil {
				return nil, errors.Wrap(err, "iterating through bucket objects")
			}
			if len(attrs.Metadata) == 0 {
				fmt.Printf("WARNING: no paths defined for object %s\n", attrs.Name)
				continue
			}
			var paths map[string]int64
			if err = json.Unmarshal([]byte(attrs.Metadata["paths"]), &paths); err != nil {
				fmt.Printf("WARNING: unmarshaling paths in object %s: %s\n", attrs.Name, err)
				continue
			}
			for path, unixtime := range paths {
				f.addPath(attrs.Name, path, unixtime, uint64(attrs.Size))
			}
		}
	}

	// Build filesystem by parsing JSON list output.

	var r io.Reader = os.Stdin
	if fromfile != "-" {
		inp, err := os.Open(fromfile)
		if err != nil {
			return nil, errors.Wrapf(err, "opening %s", fromfile)
		}
		defer inp.Close()
		r = inp
	}

	dec := json.NewDecoder(r)
	for dec.More() {
		var l listType
		if err := dec.Decode(&l); err != nil {
			return nil, errors.Wrap(err, "JSON-decoding prescan input")
		}
		for path, timestamp := range l.Paths {
			if err := f.addPath(l.Hash, path, timestamp.Unix(), uint64(l.Size)); err != nil {
				return nil, errors.Wrap(err, "building prescan tree")
			}
		}
	}

	return f, nil
}

func (f *FS) addPath(hash, path string, unixtime int64, size uint64) error {
	parent, basename, err := f.root.findParent(path, true)
	if err != nil {
		return err
	}
	node := &FSNode{
		fs:        f,
		inode:     f.allocateInode(),
		parent:    parent,
		hash:      hash,
		timestamp: time.Unix(unixtime, 0),
		size:      size,
	}
	parent.children[basename] = node
	return nil
}

func (f *FS) allocateInode() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()

	result := f.nextInode
	f.nextInode++
	return result
}

func (f *FS) Root() (fs.Node, error) {
	return f.root, nil
}

var (
	_ fs.Node               = &FSNode{}
	_ fs.HandleReadAller    = &FSNode{}
	_ fs.HandleReadDirAller = &FSNode{}
	_ fs.HandleReader       = &FSNode{}
)

type FSNode struct {
	fs     *FS
	inode  uint64
	parent *FSNode

	// If this is a dir:
	children map[string]*FSNode

	// If this is a file:
	hash      string // hash != "" means this is a file
	timestamp time.Time
	size      uint64
}

func (n *FSNode) isDir() bool {
	return n.hash == ""
}

func (n *FSNode) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Inode = n.inode
	a.Size = n.size
	a.Mtime = n.timestamp

	if n.hash == "" {
		// dir
		a.Mode = os.ModeDir | 0555
	} else {
		// file
		a.Mode = 0444
	}

	return nil
}

func (n *FSNode) Lookup(_ context.Context, name string) (fs.Node, error) {
	return n.findNode(name, false)
}

func (n *FSNode) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	return n.dirents(), nil
}

func (n *FSNode) dirents() []fuse.Dirent {
	var result []fuse.Dirent
	for name, child := range n.children {
		typ := fuse.DT_File
		if child.hash == "" {
			typ = fuse.DT_Dir
		}
		result = append(result, fuse.Dirent{
			Inode: child.inode,
			Type:  typ,
			Name:  name,
		})
	}
	return result
}

func (n *FSNode) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	if req.Dir {
		var (
			dirents = n.dirents()
			result  []byte
		)
		for _, dirent := range dirents {
			result = fuse.AppendDirent(result, dirent)
		}
		resp.Data = result
		return nil
	}

	obj := n.fs.bucket.Object(n.hash)
	r, err := gcsobj.NewReader(ctx, obj)
	if err != nil {
		return err
	}
	defer r.Close()

	if req.Offset > 0 {
		if _, err = r.Seek(req.Offset, io.SeekStart); err != nil {
			return err
		}
	}

	buf := make([]byte, req.Size)
	nbytes, err := r.Read(buf)
	resp.Data = buf[:nbytes]

	if errors.Is(err, io.EOF) {
		// Not sure this is right.
		err = nil
	}
	return err
}

func (n *FSNode) ReadAll(ctx context.Context) ([]byte, error) {
	obj := n.fs.bucket.Object(n.hash)
	r, err := obj.NewReader(ctx)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return io.ReadAll(r)
}

func (n *FSNode) findNode(name string, create bool) (*FSNode, error) {
	parent, basename, err := n.findParent(name, create)
	if err != nil {
		return nil, err
	}
	if found, ok := parent.children[basename]; ok {
		return found, nil
	}
	return nil, syscall.ENOENT
}

// Given a/b/c, returns the node corresponding to a/b,
// which must be a dir,
// plus the string "c".
func (n *FSNode) findParent(name string, create bool) (*FSNode, string, error) {
	name = strings.TrimPrefix(name, "/")
	parts := strings.Split(name, "/")
	parent := n
	for i := 0; i < len(parts)-1; i++ {
		part := parts[i]
		child, ok := parent.children[part]
		if !ok && create {
			child = &FSNode{
				fs:       n.fs,
				inode:    n.fs.allocateInode(),
				parent:   parent,
				children: make(map[string]*FSNode),
			}
			parent.children[part] = child
			parent = child
			continue
		}
		if !ok {
			return nil, "", syscall.ENOENT
		}
		if child.hash != "" {
			return nil, "", syscall.ENOTDIR
		}
		parent = child
	}
	return parent, parts[len(parts)-1], nil
}
