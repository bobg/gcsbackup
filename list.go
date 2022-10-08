package main

import (
	"context"
	"encoding/json"
	"os"
	"time"

	"cloud.google.com/go/storage"
	"github.com/pkg/errors"
	"google.golang.org/api/iterator"
)

func (c maincmd) doList(ctx context.Context, _ []string) error {
	var (
		enc   = json.NewEncoder(os.Stdout)
		query = new(storage.Query)
		it    = c.bucket.Objects(ctx, query)
	)
	enc.SetIndent("", "  ")
	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			return nil
		}
		if err != nil {
			return err
		}

		var paths map[string]int64
		if len(attrs.Metadata) == 0 {
			paths = make(map[string]int64)
		} else {
			if err = json.Unmarshal([]byte(attrs.Metadata["paths"]), &paths); err != nil {
				return errors.Wrapf(err, "decoding paths attr for %s", attrs.Name)
			}
		}

		for path, unixtime := range paths {
			out := listType{
				Path:      path,
				Timestamp: time.Unix(unixtime, 0),
				Hash:      attrs.Name,
				Link:      attrs.MediaLink,
			}
			if err = enc.Encode(out); err != nil {
				return errors.Wrapf(err, "JSON-encoding output for %s, path %s", attrs.Name, path)
			}
		}
	}
}

type listType struct {
	Path      string    `json:"path"`
	Timestamp time.Time `json:"timestamp"`
	Hash      string    `json:"hash"`
	Link      string    `json:"link,omitempty"`
}
