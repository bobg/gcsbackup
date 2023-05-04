package main

import (
	"context"
	"io/fs"
	"log"
	"net/http"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/bobg/ctrlc"
	"github.com/bobg/gcsobj"
	"github.com/bobg/mid"
	"github.com/pkg/errors"
)

type kodi struct {
	bucket                  *storage.BucketHandle
	dir, username, password string
	f                       *FS
}

func (c maincmd) doKodi(ctx context.Context, dir, listen, username, password, listfile string, _ []string) error {
	return ctrlc.Run(ctx, func(ctx context.Context) error {
		k := &kodi{
			bucket:   c.bucket,
			dir:      dir,
			username: username,
			password: password,
		}

		log.Print("Building file system, please wait")
		f, err := newFS(ctx, c.bucket, listfile)
		if err != nil {
			return errors.Wrap(err, "building filesystem")
		}
		k.f = f

		s := &http.Server{
			Addr:    listen,
			Handler: mid.Err(k.handle),
		}

		log.Printf("Listening on %s", listenAddr)

		if certFile != "" && keyFile != "" {
			err = s.ListenAndServeTLS(certFile, keyFile)
		} else {
			err = s.ListenAndServe()
		}
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return errors.Wrap(err, "in ListenAndServe")
	})
}

func (k *kodi) handle(w http.ResponseWriter, req *http.Request) error {
	if k.username != "" && k.password != "" {
		username, password, ok := req.BasicAuth()
		if !ok || username != s.username || password != s.password {
			w.Header().Add("WWW-Authenticate", `Basic realm="Access to list and stream titles"`)
			return mid.CodeErr{C: http.StatusUnauthorized}
		}
	}

	path := strings.Trim(req.URL.Path, "/")
	if path == "" {
		return k.handleDir(w, req, k.f.root)
	}

	ctx := req.Context()

	node, err := k.f.root.findNode(path, false)
	if errors.Is(err, fs.ErrNotExist) {
		return mid.CodeErr{C: http.StatusNotFound}
	}
	if err != nil {
		return errors.Wrap(err, "getting %s", path)
	}
	if node.isDir() {
		return k.handleDir(w, req, node)
	}

	obj := k.bucket.Object(node.hash)
	r, err := gcsobj.NewReader(ctx, obj)
	if err != nil {
		return errors.Wrapf(err, "creating reader for object %s", objname)
	}
	defer r.Close()

	wrapper := &mid.ResponseWrapper{W: w}
	http.ServeContent(wrapper, req, path, time.Time{}, r)
	if wrapper.Code < 200 || wrapper.Code >= 400 {
		return mid.CodeErr{C: wrapper.Code}
	}
	return nil
}
