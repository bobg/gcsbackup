package main

import (
	"context"
	_ "embed"
	"html/template"
	"io/fs"
	"log"
	"net/http"
	"sort"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/bobg/ctrlc"
	"github.com/bobg/gcsobj"
	"github.com/bobg/go-generics/v2/maps"
	"github.com/bobg/mid"
	"github.com/pkg/errors"
)

type kodi struct {
	bucket             *storage.BucketHandle
	username, password string
	node               *FSNode
}

func (c maincmd) doKodi(outerCtx context.Context, dir, listen, username, password, listfile, certfile, keyfile string, _ []string) error {
	return ctrlc.Run(outerCtx, func(ctx context.Context) error {
		k := &kodi{
			bucket:   c.bucket,
			username: username,
			password: password,
		}

		f, err := newFS(ctx, c.bucket, listfile)
		if err != nil {
			return errors.Wrap(err, "building filesystem")
		}
		node, err := f.root.findNode(dir, false)
		if err != nil {
			return errors.Wrapf(err, "finding %s", dir)
		}

		k.node = node

		s := &http.Server{
			Addr:    listen,
			Handler: mid.Err(k.handle),
		}

		log.Printf("Listening on %s", listen)

		if certfile != "" && keyfile != "" {
			go s.ListenAndServeTLS(certfile, keyfile)
		} else {
			go s.ListenAndServe()
		}

		<-ctx.Done()
		err = s.Shutdown(outerCtx)

		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	})
}

func (k *kodi) handle(w http.ResponseWriter, req *http.Request) error {
	if k.username != "" && k.password != "" {
		username, password, ok := req.BasicAuth()
		if !ok || username != k.username || password != k.password {
			w.Header().Add("WWW-Authenticate", `Basic realm="Access to list and stream titles"`)
			return mid.CodeErr{C: http.StatusUnauthorized}
		}
	}

	ctx := req.Context()

	log.Printf("Handling %s", req.URL.Path)

	path := strings.Trim(req.URL.Path, "/")
	if path == "" {
		return k.handleDir(ctx, w, k.node)
	}

	node, err := k.node.findNode(path, false)
	if errors.Is(err, fs.ErrNotExist) {
		return mid.CodeErr{C: http.StatusNotFound}
	}
	if err != nil {
		return errors.Wrapf(err, "getting %s", path)
	}
	if node.isDir() {
		return k.handleDir(ctx, w, node)
	}

	obj := k.bucket.Object(node.hash)
	r, err := gcsobj.NewReader(ctx, obj)
	if err != nil {
		return errors.Wrapf(err, "creating reader for object %s", node.hash)
	}
	defer r.Close()

	wrapper := &mid.ResponseWrapper{W: w}
	http.ServeContent(wrapper, req, path, time.Time{}, r)
	if wrapper.Code < 200 || wrapper.Code >= 400 {
		return mid.CodeErr{C: wrapper.Code}
	}
	return nil
}

func (k *kodi) handleDir(ctx context.Context, w http.ResponseWriter, node *FSNode) error {
	var items []template.URL

	keys := maps.Keys(node.children)
	sort.Strings(keys)
	for _, key := range keys {
		child := node.children[key]
		if child.isDir() {
			items = append(items, template.URL(key+"/"))
		} else {
			items = append(items, template.URL(key))
		}
	}

	return dirtmpl.Execute(w, items)
}

//go:embed dir.html.tmpl
var dirtmplstr string

var dirtmpl = template.Must(template.New("").Parse(dirtmplstr))
