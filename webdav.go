package main

import (
	"context"
	"crypto/subtle"
	_ "embed"
	"io"
	"io/fs"
	"log"
	"mime"
	"net/http"
	"os"
	"os/signal"
	"path"
	"sort"
	"strings"
	"syscall"
	"time"

	"cloud.google.com/go/storage"
	"github.com/bobg/errors"
	"github.com/bobg/gcsobj"
	"golang.org/x/net/webdav"
)

type webdavType struct {
	bucket             *storage.BucketHandle
	username, password string
	node               *FSNode
}

func (c maincmd) doWebDAV(ctx context.Context, dir, listen, username, password, listfile, certfile, keyfile string, _ []string) error {
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	wd := &webdavType{
		bucket:   c.bucket,
		username: username,
		password: password,
	}

	log.Print("Building file system, please wait")

	f, err := newFS(ctx, c.bucket, listfile, "")
	if err != nil {
		return errors.Wrap(err, "building filesystem")
	}

	node, err := wd.findNode(f.root, dir)
	if err != nil {
		return errors.Wrapf(err, "finding %s", dir)
	}
	if !node.isDir() {
		return errors.Wrapf(syscall.ENOTDIR, "finding %s", dir)
	}

	wd.node = node

	s := &http.Server{
		Addr:    listen,
		Handler: wd.handler(),
	}

	log.Printf("Listening on %s", listen)

	errch := make(chan error, 1)
	go func() {
		var err error
		if certfile != "" && keyfile != "" {
			err = s.ListenAndServeTLS(certfile, keyfile)
		} else {
			err = s.ListenAndServe()
		}
		err = errors.Prune(err, http.ErrServerClosed)
		errch <- err
	}()

	select {
	case err := <-errch:
		// Server exited on its own (e.g. could not bind the listen address).
		return errors.Wrap(err, "serving")

	case <-ctx.Done():
	}

	log.Print("Context canceled, shutting down server")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = s.Shutdown(shutdownCtx)
	err = errors.Prune(err, http.ErrServerClosed)
	if err != nil {
		return errors.Wrap(err, "shutting down server")
	}

	err = <-errch
	return errors.Wrap(err, "serving")
}

func (wd *webdavType) handler() http.Handler {
	h := &webdav.Handler{
		FileSystem: &wdFS{wd: wd},
		LockSystem: &wdLockSystem{wd: wd, mem: webdav.NewMemLS()},
		Logger: func(req *http.Request, err error) {
			if err != nil {
				log.Printf("%s %s: %s", req.Method, req.URL.Path, err)
			}
		},
	}

	if wd.username == "" || wd.password == "" {
		return h
	}

	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		username, password, ok := req.BasicAuth()
		if !ok || !constEq(username, wd.username) || !constEq(password, wd.password) {
			w.Header().Add("WWW-Authenticate", `Basic realm="gcsbackup"`)
			http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
			return
		}
		h.ServeHTTP(w, req)
	})
}

func constEq(a, b string) bool {
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

// findNode finds the node for the given slash-separated name relative to root.
// The empty name and "/" both denote root itself.
// Errors satisfy os.IsNotExist when the node is not found.
func (wd *webdavType) findNode(root *FSNode, name string) (*FSNode, error) {
	name = strings.Trim(path.Clean("/"+name), "/")
	if name == "" {
		return root, nil
	}
	node, err := root.findNode(name, false)
	if errors.Is(err, syscall.ENOENT) || errors.Is(err, syscall.ENOTDIR) {
		return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrNotExist}
	}
	return node, err
}

var (
	_ webdav.FileSystem   = (*wdFS)(nil)
	_ webdav.LockSystem   = (*wdLockSystem)(nil)
	_ webdav.File         = (*wdFile)(nil)
	_ webdav.ContentTyper = (*wdFileInfo)(nil)
)

var errReadOnly = errors.New("read only filesystem")

type wdFS struct {
	wd *webdavType
}

func (fs *wdFS) Mkdir(_ context.Context, _ string, _ os.FileMode) error {
	return errReadOnly
}

func (fs *wdFS) OpenFile(ctx context.Context, name string, flag int, perm os.FileMode) (webdav.File, error) {
	if flag&(os.O_WRONLY|os.O_RDWR|os.O_CREATE|os.O_TRUNC|os.O_APPEND) != 0 {
		return nil, errReadOnly
	}

	node, err := fs.wd.findNode(fs.wd.node, name)
	if err != nil {
		return nil, err
	}

	f := &wdFile{
		info: newFileInfo(name, node),
	}
	if !node.isDir() {
		// gcsobj.Reader is lazy:
		// no request is made to GCS until the first Read.
		obj := fs.wd.bucket.Object(node.hash)
		f.r = gcsobj.NewReaderWithSize(ctx, obj, int64(node.size))
	}

	return f, nil
}

func (fs *wdFS) RemoveAll(ctx context.Context, name string) error {
	return errReadOnly
}

func (fs *wdFS) Rename(ctx context.Context, oldName, newName string) error {
	return errReadOnly
}

func (fs *wdFS) Stat(ctx context.Context, name string) (os.FileInfo, error) {
	node, err := fs.wd.findNode(fs.wd.node, name)
	if err != nil {
		return nil, err
	}
	return newFileInfo(name, node), nil
}

// wdLockSystem delegates to an in-memory lock system.
// The filesystem is read-only so locks protect nothing,
// but some WebDAV clients (e.g. macOS Finder, Windows Explorer)
// expect LOCK/UNLOCK to succeed.
type wdLockSystem struct {
	wd  *webdavType
	mem webdav.LockSystem
}

func (ls *wdLockSystem) Confirm(now time.Time, name0, name1 string, conditions ...webdav.Condition) (func(), error) {
	return ls.mem.Confirm(now, name0, name1, conditions...)
}

func (ls *wdLockSystem) Create(now time.Time, details webdav.LockDetails) (string, error) {
	return ls.mem.Create(now, details)
}

func (ls *wdLockSystem) Refresh(now time.Time, token string, dur time.Duration) (webdav.LockDetails, error) {
	return ls.mem.Refresh(now, token, dur)
}

func (ls *wdLockSystem) Unlock(now time.Time, token string) error {
	return ls.mem.Unlock(now, token)
}

type wdFile struct {
	info *wdFileInfo

	// For files, a reader on the GCS object; nil for dirs.
	r *gcsobj.Reader

	// For dirs, the sorted child names, populated on the first call to Readdir,
	// and the position of the next one to return.
	names  []string
	dirpos int
}

func (f *wdFile) Read(p []byte) (int, error) {
	if f.r == nil {
		return 0, &fs.PathError{Op: "read", Path: f.info.name, Err: syscall.EISDIR}
	}
	return f.r.Read(p)
}

func (f *wdFile) Seek(offset int64, whence int) (int64, error) {
	if f.r == nil {
		return 0, &fs.PathError{Op: "seek", Path: f.info.name, Err: syscall.EISDIR}
	}
	return f.r.Seek(offset, whence)
}

func (f *wdFile) Close() error {
	if f.r == nil {
		return nil
	}
	return f.r.Close()
}

func (f *wdFile) Write(p []byte) (int, error) {
	return 0, errReadOnly
}

// Readdir has the semantics of [os.File.Readdir].
func (f *wdFile) Readdir(count int) ([]fs.FileInfo, error) {
	node := f.info.node
	if !node.isDir() {
		return nil, &fs.PathError{Op: "readdir", Path: f.info.name, Err: syscall.ENOTDIR}
	}

	if f.names == nil {
		f.names = make([]string, 0, len(node.children))
		for name := range node.children {
			f.names = append(f.names, name)
		}
		sort.Strings(f.names)
	}

	remaining := f.names[f.dirpos:]
	if count > 0 {
		if len(remaining) == 0 {
			return nil, io.EOF
		}
		if len(remaining) > count {
			remaining = remaining[:count]
		}
	}

	result := make([]fs.FileInfo, 0, len(remaining))
	for _, name := range remaining {
		result = append(result, newFileInfo(name, node.children[name]))
	}
	f.dirpos += len(remaining)

	return result, nil
}

func (f *wdFile) Stat() (fs.FileInfo, error) {
	return f.info, nil
}

// wdFileInfo implements [fs.FileInfo] for an [FSNode].
type wdFileInfo struct {
	name string // base name
	node *FSNode
}

func newFileInfo(name string, node *FSNode) *wdFileInfo {
	return &wdFileInfo{name: path.Base(path.Clean("/" + name)), node: node}
}

func (fi *wdFileInfo) Name() string       { return fi.name }
func (fi *wdFileInfo) ModTime() time.Time { return fi.node.timestamp }
func (fi *wdFileInfo) IsDir() bool        { return fi.node.isDir() }
func (fi *wdFileInfo) Sys() any           { return nil }

func (fi *wdFileInfo) Size() int64 {
	if fi.node.isDir() {
		return 0
	}
	return int64(fi.node.size)
}

func (fi *wdFileInfo) Mode() fs.FileMode {
	if fi.node.isDir() {
		return fs.ModeDir | 0555
	}
	return 0444
}

// ContentType implements [webdav.ContentTyper].
// Without it, the webdav package would sniff the content type
// by reading the start of each file,
// which is expensive for GCS objects.
func (fi *wdFileInfo) ContentType(_ context.Context) (string, error) {
	if fi.node.isDir() {
		return "", webdav.ErrNotImplemented
	}
	if t := mime.TypeByExtension(path.Ext(fi.name)); t != "" {
		return t, nil
	}
	return "application/octet-stream", nil
}
