// Command gcsbackup walks a directory tree,
// copying files to a google cloud storage bucket
// based on their MD5 hash.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"

	"cloud.google.com/go/storage"
	"github.com/bobg/atime"
	"github.com/pkg/errors"
	"google.golang.org/api/option"
)

func main() {
	var (
		credsFile  = flag.String("creds", "", "filename for JSON-encoded credentials")
		bucketName = flag.String("bucket", "", "bucket name")
	)

	flag.Parse()

	ctx := context.Background()

	client, err := storage.NewClient(
		ctx,
		option.WithScopes(storage.ScopeFullControl),
		option.WithCredentialsFile(*credsFile),
	)
	if err != nil {
		log.Fatal(err)
	}
	bucket := client.Bucket(*bucketName)

	for _, root := range flag.Args() {
		err = filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			if info.IsDir() {
				return nil
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
			obj := bucket.Object(name)
			attrs, err := obj.Attrs(ctx)
			switch err {
			case storage.ErrObjectNotExist:
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

				log.Printf("uploading %s, hash %s", path, name)

				err = withRetries(3, time.Minute, func() error {
					w := obj.NewWriter(ctx)
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

				err = withRetries(3, time.Minute, func() error {
					_, err := obj.Update(ctx, storage.ObjectAttrsToUpdate{
						Metadata: metadata,
					})
					return errors.Wrapf(err, "storing attrs for %s (path %s)", name, path)
				})
				if err != nil {
					return err
				}

			case nil:
				var paths map[string]int64

				if len(attrs.Metadata) == 0 {
					paths = make(map[string]int64)
				} else {
					err = json.Unmarshal([]byte(attrs.Metadata["paths"]), &paths)
					if err != nil {
						return errors.Wrapf(err, "decoding paths attr for %s (path %s)", name, path)
					}
				}

				if _, ok := paths[path]; ok {
					log.Printf("%s already present (hash %s)", path, name)
				} else {
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

					err = withRetries(3, time.Minute, func() error {
						_, err := obj.Update(ctx, storage.ObjectAttrsToUpdate{
							Metadata: metadata,
						})
						return errors.Wrapf(err, "updating attrs for %s (path %s)", name, path)
					})
				}

			default:
				return errors.Wrapf(err, "getting attrs for %s (path %s)", name, path)
			}

			return nil
		})
		if err != nil {
			log.Fatal(err)
		}
	}
}

func withRetries(num int, interval time.Duration, f func() error) error {
	var try int
	for {
		try++
		err := f()
		if err == nil || try >= num {
			return err
		}
		log.Printf("error %s, try %d of %d, will retry in %s", err, try, num, interval)
		time.Sleep(interval)
	}
}
