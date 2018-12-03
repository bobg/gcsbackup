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
	"fmt"
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
		root       = flag.String("root", "", "root dir")
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

	err = filepath.Walk(*root, func(path string, info os.FileInfo, err error) error {
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

			w := obj.NewWriter(ctx)
			err = atime.WithTimesRestored(path, func(r io.ReadSeeker) error {
				_, err := io.Copy(w, r)
				return err
			})
			if err != nil {
				return errors.Wrapf(err, "uploading content for %s (path %s)", name, path)
			}
			err = w.Close()
			if err != nil {
				return errors.Wrapf(err, "closing upload channel for %s (path %s)", name, path)
			}
			_, err = obj.Update(ctx, storage.ObjectAttrsToUpdate{
				Metadata: metadata,
			})
			if err != nil {
				return errors.Wrapf(err, "storing attrs for %s (path %s)", name, path)
			}

		case nil:
			if len(attrs.Metadata) == 0 {
				return fmt.Errorf("no metadata on %s (path %s)", name, path)
			}

			var paths map[string]int64
			err = json.Unmarshal([]byte(attrs.Metadata["paths"]), &paths)
			if err != nil {
				return errors.Wrapf(err, "decoding paths attr for %s (path %s)", name, path)
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
				_, err = obj.Update(ctx, storage.ObjectAttrsToUpdate{
					Metadata: metadata,
				})
				if err != nil {
					return errors.Wrapf(err, "updating attrs for %s (path %s)", name, path)
				}
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
