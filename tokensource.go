package main

import (
	"context"
	"fmt"

	"cloud.google.com/go/storage"
	"github.com/bobg/oauther"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

type tokenSource struct {
	ctx    context.Context
	t      oauther.TokenSrc
	config *oauth2.Config
}

func newTokenSource(ctx context.Context, creds []byte) (*tokenSource, error) {
	config, err := google.ConfigFromJSON(creds, storage.ScopeFullControl)
	if err != nil {
		return nil, err
	}

	w := oauther.NewWebTokenSrc(interact)

	return &tokenSource{
		ctx:    ctx,
		t:      oauther.NewFileCache(w, "tokencache"),
		config: config,
	}, nil
}

// Implements oauth2.TokenSource.
func (t *tokenSource) Token() (*oauth2.Token, error) {
	return t.t.Get(t.ctx, t.config)
}

func interact(url string) (string, error) {
	fmt.Printf("Get an auth code from the following URL, then enter it here:\n%s\n", url)
	var code string
	_, err := fmt.Scan(&code)
	return code, err
}
