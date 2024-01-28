// Command gcsbackup walks a directory tree,
// copying files to a google cloud storage bucket
// based on their SHA256 hash.
package main

import (
	"context"
	"flag"
	"log"
	"math"

	"cloud.google.com/go/storage"
	"github.com/bobg/subcmd/v2"
	"golang.org/x/time/rate"
	"google.golang.org/api/option"
)

func main() {
	var (
		credsFile  = flag.String("creds", "creds.json", "filename for JSON-encoded credentials")
		bucketName = flag.String("bucket", "", "bucket name")
		throttle   = flag.Int("throttle", 0, "upload bytes per second (default 0 is unlimited)")
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

	var limiter *rate.Limiter
	if *throttle > 0 {
		limiter = rate.NewLimiter(rate.Limit(*throttle), math.MaxInt)
	}

	c := maincmd{
		bucketname: *bucketName,
		bucket:     bucket,
		limiter:    limiter,
	}

	if err := subcmd.Run(ctx, c, flag.Args()); err != nil {
		log.Fatal(err)
	}
}

type maincmd struct {
	bucketname string
	bucket     *storage.BucketHandle
	limiter    *rate.Limiter
}

func (c maincmd) Subcmds() subcmd.Map {
	return subcmd.Commands(
		"save", c.doSave, "save files to GCS", subcmd.Params(
			"-exclude-from", subcmd.String, "", "file of exclude patterns (unanchored regexes)",
			"-list", subcmd.String, "", "prescan from a file of list output; use - to read from stdin",
		),
		"list", c.doList, "list bucket objects", nil,
		"fs", c.doFS, "serve a FUSE filesystem", subcmd.Params(
			"-name", subcmd.String, c.bucketname, "file system name",
			"-list", subcmd.String, "", "build file system from list output; use - to read from stdin",
			"-at", subcmd.String, "", "build file system as of this time",
			"mount", subcmd.String, "", "mount point",
		),
		"kodi", c.doKodi, "serve a gcsbackup file tree to Kodi", subcmd.Params(
			"-dir", subcmd.String, "", "directory to serve",
			"-listen", subcmd.String, ":1549", "listen address",
			"-username", subcmd.String, "", "HTTP Basic Auth username",
			"-password", subcmd.String, "", "HTTP Basic Auth password", // TODO: move this to an env var
			"-list", subcmd.String, "", "build file system from list output; use - to read from stdin",
			"-cert", subcmd.String, "", "path to cert file",
			"-key", subcmd.String, "", "path to key file",
			"-at", subcmd.String, "", "build file system as of this time",
		),
	)
}
