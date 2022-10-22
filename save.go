package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"time"

	"cloud.google.com/go/storage"
	"github.com/bobg/atime/v2"
	"github.com/cenkalti/backoff/v4"
	"github.com/pkg/errors"
	"golang.org/x/time/rate"
)

func (c maincmd) doSave(ctx context.Context, excludeFrom string, listfile string, args []string) error {
	var excludePatterns []*regexp.Regexp
	if excludeFrom != "" {
		f, err := os.Open(excludeFrom)
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			regex, err := regexp.Compile(sc.Text())
			if err != nil {
				log.Fatalf("Compiling exclude pattern %s: %s", sc.Text(), err)
			}
			excludePatterns = append(excludePatterns, regex)
		}
		if err := sc.Err(); err != nil {
			log.Fatal(err)
		}
	}

	expBkoff := backoff.NewExponentialBackOff()
	expBkoff.InitialInterval = 10 * time.Second
	bkoff := backoff.WithMaxRetries(expBkoff, 3)
	bkoff = backoff.WithContext(bkoff, ctx)

	f, err := newFS(ctx, c.bucket, listfile)
	if err != nil {
		return errors.Wrap(err, "in prescan")
	}

	for _, root := range args {
		err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if info.IsDir() {
				return nil
			}
			if (info.Mode() & fs.ModeSymlink) == fs.ModeSymlink {
				log.Printf("Skipping symlink %s", path)
				return nil
			}
			if info.Size() == 0 {
				log.Printf("Skipping empty file %s", path)
				return nil
			}
			for _, regex := range excludePatterns {
				if regex.MatchString(path) {
					log.Printf("Skipping excluded file %s", path)
					return nil
				}
			}

			node, err := f.root.findNode(path, false)
			if err != nil {
				// Ignore errors.
				node = nil
			} else if node.hash == "" {
				node = nil
			}

			if node != nil {
				if uint64(info.Size()) == node.size && !info.ModTime().After(node.timestamp) {
					log.Printf("Found a prescan size/modtime match for %s", path)
					return nil
				}
			}

			var hash []byte
			err = atime.WithTimesRestored(path, func(r io.ReadSeeker) error {
				hasher := sha256.New()
				_, err = io.Copy(hasher, r)
				if err != nil {
					return errors.Wrapf(err, "hashing %s", path)
				}
				hash = hasher.Sum(nil)
				return nil
			})
			if err != nil {
				return err
			}

			name := "sha256-" + hex.EncodeToString(hash)

			if node != nil && node.hash == name {
				log.Printf("Found a prescan hash match for %s", path)
				return nil
			}

			obj := c.bucket.Object(name)
			attrs, err := obj.Attrs(ctx)
			if err != nil && !errors.Is(err, storage.ErrObjectNotExist) {
				return errors.Wrapf(err, "getting attrs for %s (path %s)", name, path)
			}

			if errors.Is(err, storage.ErrObjectNotExist) {
				paths := map[string]int64{
					path: time.Now().Unix(),
				}
				j, err := json.Marshal(paths)
				if err != nil {
					return errors.Wrapf(err, "encoding new paths attr for %s (path %s)", name, path)
				}
				metadata := map[string]string{
					"paths": string(j),
				}

				log.Printf("uploading %s, %d bytes, hash %s", path, info.Size(), name)

				err = withRetries(bkoff, func() error {
					var w io.WriteCloser = obj.NewWriter(ctx)

					if c.limiter != nil {
						w = &limitingWriter{ctx: ctx, limiter: c.limiter, w: w}
					}

					err := atime.WithTimesRestored(path, func(r io.ReadSeeker) error {
						_, err := io.Copy(w, r)
						return err
					})
					if err != nil {
						return errors.Wrapf(err, "uploading content for %s (path %s)", name, path)
					}
					err = w.Close()
					return errors.Wrapf(err, "closing upload channel for %s (path %s)", name, path)
				})
				if err != nil {
					return err
				}

				return withRetries(bkoff, func() error {
					_, err := obj.Update(ctx, storage.ObjectAttrsToUpdate{
						Metadata: metadata,
					})
					return errors.Wrapf(err, "storing attrs for %s (path %s)", name, path)
				})
			}

			var paths map[string]int64
			if len(attrs.Metadata) == 0 {
				paths = make(map[string]int64)
			} else {
				if err = json.Unmarshal([]byte(attrs.Metadata["paths"]), &paths); err != nil {
					return errors.Wrapf(err, "decoding paths attr for %s (path %s)", name, path)
				}
			}

			if _, ok := paths[path]; ok {
				log.Printf("%s already present (hash %s)", path, name)
				return nil
			}

			var oldpaths []string
			for k := range paths {
				oldpaths = append(oldpaths, k)
			}
			log.Printf("%s already present as %v (hash %s), adding new path", path, oldpaths, name)

			paths[path] = time.Now().Unix()
			j, err := json.Marshal(paths)
			if err != nil {
				return errors.Wrapf(err, "encoding updated paths attr for %s (path %s)", name, path)
			}
			metadata := map[string]string{
				"paths": string(j),
			}

			return withRetries(bkoff, func() error {
				_, err := obj.Update(ctx, storage.ObjectAttrsToUpdate{
					Metadata: metadata,
				})
				return errors.Wrapf(err, "updating attrs for %s (path %s)", name, path)
			})
		})
		if err != nil {
			return errors.Wrapf(err, "in walk of %s", root)
		}
	}

	return nil
}

func withRetries(bkoff backoff.BackOff, f func() error) error {
	bkoff.Reset()
	return backoff.Retry(f, bkoff) // The backoff API gets the order of these arguments wrong.
}

type limitingWriter struct {
	ctx     context.Context
	limiter *rate.Limiter
	w       io.WriteCloser
}

func (w *limitingWriter) Write(buf []byte) (int, error) {
	if err := w.limiter.WaitN(w.ctx, len(buf)); err != nil {
		return 0, err
	}
	return w.w.Write(buf)
}

func (w *limitingWriter) Close() error {
	return w.w.Close()
}
