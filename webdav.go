package main

import (
	"context"
	_ "embed"
	"io/fs"
	"log"
	"net/http"
	"os"
	"os/signal"
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

	node, err := f.root.findNode(dir, false)
	if err != nil {
		return errors.Wrapf(err, "finding %s", dir)
	}

	wd.node = node

	s := &http.Server{
		Addr:    listen,
		Handler: wd.handler(),
	}

	log.Printf("Listening on %s", listen)

	// xxx
}

func (wd *webdavType) handler() http.Handler {
	return &webdav.Handler{
		FileSystem: &wdFS{wd: wd},
		LockSystem: &wdLockSystem{wd: wd},
	}
}

var (
	_ webdav.FileSystem = (*wdFS)(nil)
	_ webdav.LockSystem = (*wdLockSystem)(nil)
	_ webdav.File       = (*wdFile)(nil)
)

var errReadOnly = errors.New("read only filesystem")

type wdFS struct {
	wd *webdavType
}

func (fs *wdFS) Mkdir(_ context.Context, _ string, _ os.FileMode) error {
	return errReadOnly
}

func (fs *wdFS) OpenFile(ctx context.Context, name string, flag int, perm os.FileMode) (webdav.File, error) {
	// xxx
}

func (fs *wdFS) RemoveAll(ctx context.Context, name string) error {
	return errReadOnly
}

func (fs *wdFS) Rename(ctx context.Context, oldName, newName string) error {
	return errReadOnly
}

func (fs *wdFS) Stat(ctx context.Context, name string) (os.FileInfo, error) {
	// xxx
}

type wdLockSystem struct {
	wd *webdavType
}

func (ls *wdLockSystem) Confirm(now time.Time, name0, name1 string, conditions ...webdav.Condition) (func(), error) {
	// xxx
}

func (ls *wdLockSystem) Create(now time.Time, details webdav.LockDetails) (string, error) {
	// xxx
}

func (ls *wdLockSystem) Refresh(now time.Time, token string, dur time.Duration) (webdav.LockDetails, error) {
	// xxx
}

func (ls *wdLockSystem) Unlock(now time.Time, token string) error {
	// xxx
}

type wdFile struct {
	*gcsobj.Reader
}

func (f *wdFile) Write(p []byte) (int, error) {
	return 0, errReadOnly
}

func (f *wdFile) Readdir(count int) ([]fs.FileInfo, error) {
	// xxx
}

func (f *wdFile) Stat() (fs.FileInfo, error) {
	// xxx
}
