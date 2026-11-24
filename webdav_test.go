package main

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/bobg/errors"
	"golang.org/x/net/webdav"
	"google.golang.org/api/option"
)

// fakeGCS is an in-process stand-in for the GCS object-read API.
// It is installed as the transport of the storage client's HTTP client,
// so no network listener is needed.
type fakeGCS struct {
	bucket  string
	objects map[string][]byte

	mu       sync.Mutex
	requests []string // object names requested, in order
}

func (g *fakeGCS) RoundTrip(req *http.Request) (*http.Response, error) {
	// Reads may use either the XML API (/BUCKET/OBJECT)
	// or the JSON API (/storage/v1/b/BUCKET/o/OBJECT?alt=media).
	p := req.URL.EscapedPath()
	var escaped string
	if _, after, ok0 := strings.Cut(p, "/o/"); strings.Contains(p, "/storage/v1/b/") && ok0 {
		escaped = after
	} else {
		escaped = strings.TrimPrefix(p, "/"+g.bucket+"/")
	}
	name, err := url.PathUnescape(escaped)
	if err != nil {
		return nil, err
	}

	g.mu.Lock()
	g.requests = append(g.requests, name)
	g.mu.Unlock()

	rec := httptest.NewRecorder()
	data, ok := g.objects[name]
	if !ok {
		http.Error(rec, "not found", http.StatusNotFound)
	} else {
		rec.Header().Set("Content-Type", "application/octet-stream")
		rec.Header().Set("X-Goog-Generation", "1")
		rec.Header().Set("X-Goog-Metageneration", "1")
		http.ServeContent(rec, req, name, time.Time{}, bytes.NewReader(data))
	}
	resp := rec.Result()
	resp.Request = req
	return resp, nil
}

func (g *fakeGCS) requestCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.requests)
}

// testFile describes one file in the test tree.
type testFile struct {
	path, hash string
	content    string
	mtime      int64
}

var testFiles = []testFile{
	{path: "movies/action/die-hard.mp4", hash: "h-diehard", content: "yippee-ki-yay", mtime: 1000},
	{path: "movies/action/speed.mkv", hash: "h-speed", content: "pop quiz, hotshot", mtime: 2000},
	{path: "movies/comedy/airplane.mp4", hash: "h-airplane", content: "surely you can't be serious", mtime: 3000},
	{path: "movies/readme", hash: "h-readme", content: "no extension here", mtime: 4000},
	{path: "music/song.mp3", hash: "h-song", content: "la la la", mtime: 5000},
	{path: "top.txt", hash: "h-top", content: "top-level file", mtime: 6000},
}

const testBucket = "test-bucket"

// newTestWebDAV builds an FS from testFiles, backed by a fakeGCS,
// and returns a webdavType rooted at dir.
func newTestWebDAV(t *testing.T, dir, username, password string) (*webdavType, *fakeGCS) {
	t.Helper()

	gcs := &fakeGCS{bucket: testBucket, objects: make(map[string][]byte)}
	for _, tf := range testFiles {
		gcs.objects[tf.hash] = []byte(tf.content)
	}

	client, err := storage.NewClient(
		context.Background(),
		option.WithHTTPClient(&http.Client{Transport: gcs}),
		option.WithEndpoint("http://fake-gcs.invalid/storage/v1/"),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })

	bucket := client.Bucket(testBucket)

	f := &FS{bucket: bucket, nextInode: 2}
	f.root = &FSNode{fs: f, inode: 1, children: make(map[string]*FSNode)}
	for _, tf := range testFiles {
		if err := f.addPath(tf.hash, tf.path, tf.mtime, uint64(len(tf.content))); err != nil {
			t.Fatal(err)
		}
	}

	wd := &webdavType{bucket: bucket, username: username, password: password}
	node, err := wd.findNode(f.root, dir)
	if err != nil {
		t.Fatal(err)
	}
	wd.node = node

	return wd, gcs
}

func TestFindNode(t *testing.T) {
	wd, _ := newTestWebDAV(t, "", "", "")
	root := wd.node

	cases := []struct {
		name    string
		wantDir bool
		wantErr bool
	}{
		{name: "", wantDir: true},
		{name: "/", wantDir: true},
		{name: ".", wantDir: true},
		{name: "movies", wantDir: true},
		{name: "/movies", wantDir: true},
		{name: "/movies/", wantDir: true},
		{name: "movies/action", wantDir: true},
		{name: "movies/./action/../action", wantDir: true},
		{name: "movies/action/die-hard.mp4"},
		{name: "/top.txt"},
		{name: "nope", wantErr: true},
		{name: "movies/nope", wantErr: true},
		{name: "movies/nope/deeper", wantErr: true},
		{name: "top.txt/child", wantErr: true}, // path through a file
		{name: "../../etc/passwd", wantErr: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			node, err := wd.findNode(root, tc.name)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("got node %+v, want error", node)
				}
				if !os.IsNotExist(err) {
					t.Errorf("got error %v, want one satisfying os.IsNotExist", err)
				}
				if !errors.Is(err, fs.ErrNotExist) {
					t.Errorf("got error %v, want one matching fs.ErrNotExist", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if node.isDir() != tc.wantDir {
				t.Errorf("got isDir %v, want %v", node.isDir(), tc.wantDir)
			}
		})
	}
}

func TestReadOnlyFS(t *testing.T) {
	wd, _ := newTestWebDAV(t, "", "", "")
	wfs := &wdFS{wd: wd}
	ctx := context.Background()

	if err := wfs.Mkdir(ctx, "/newdir", 0755); !errors.Is(err, errReadOnly) {
		t.Errorf("Mkdir: got %v, want errReadOnly", err)
	}
	if err := wfs.RemoveAll(ctx, "/top.txt"); !errors.Is(err, errReadOnly) {
		t.Errorf("RemoveAll: got %v, want errReadOnly", err)
	}
	if err := wfs.Rename(ctx, "/top.txt", "/bottom.txt"); !errors.Is(err, errReadOnly) {
		t.Errorf("Rename: got %v, want errReadOnly", err)
	}

	for _, flag := range []int{
		os.O_WRONLY,
		os.O_RDWR,
		os.O_RDONLY | os.O_CREATE,
		os.O_RDONLY | os.O_TRUNC,
		os.O_RDONLY | os.O_APPEND,
		os.O_RDWR | os.O_CREATE | os.O_TRUNC,
	} {
		t.Run(fmt.Sprintf("flag=%#x", flag), func(t *testing.T) {
			for _, name := range []string{"/top.txt", "/brand-new.txt", "/movies"} {
				f, err := wfs.OpenFile(ctx, name, flag, 0644)
				if !errors.Is(err, errReadOnly) {
					if f != nil {
						f.Close()
					}
					t.Errorf("OpenFile(%s): got %v, want errReadOnly", name, err)
				}
			}
		})
	}

	f, err := wfs.OpenFile(ctx, "/top.txt", os.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.Write([]byte("x")); !errors.Is(err, errReadOnly) {
		t.Errorf("Write: got %v, want errReadOnly", err)
	}
}

func TestStat(t *testing.T) {
	wd, gcs := newTestWebDAV(t, "", "", "")
	wfs := &wdFS{wd: wd}
	ctx := context.Background()

	for _, tf := range testFiles {
		t.Run(tf.path, func(t *testing.T) {
			fi, err := wfs.Stat(ctx, "/"+tf.path)
			if err != nil {
				t.Fatal(err)
			}
			if got, want := fi.Name(), filepath.Base(tf.path); got != want {
				t.Errorf("Name: got %q, want %q", got, want)
			}
			if got, want := fi.Size(), int64(len(tf.content)); got != want {
				t.Errorf("Size: got %d, want %d", got, want)
			}
			if fi.IsDir() {
				t.Error("IsDir: got true, want false")
			}
			if got, want := fi.Mode(), fs.FileMode(0444); got != want {
				t.Errorf("Mode: got %v, want %v", got, want)
			}
			if got, want := fi.ModTime(), time.Unix(tf.mtime, 0); !got.Equal(want) {
				t.Errorf("ModTime: got %v, want %v", got, want)
			}
			if fi.Sys() != nil {
				t.Errorf("Sys: got %v, want nil", fi.Sys())
			}
		})
	}

	for _, dir := range []string{"/", "/movies", "/movies/action", "/music"} {
		t.Run(dir, func(t *testing.T) {
			fi, err := wfs.Stat(ctx, dir)
			if err != nil {
				t.Fatal(err)
			}
			if !fi.IsDir() {
				t.Error("IsDir: got false, want true")
			}
			if got, want := fi.Mode(), fs.ModeDir|0555; got != want {
				t.Errorf("Mode: got %v, want %v", got, want)
			}
			if fi.Size() != 0 {
				t.Errorf("Size: got %d, want 0", fi.Size())
			}
			wantName := filepath.Base(dir)
			if got := fi.Name(); got != wantName {
				t.Errorf("Name: got %q, want %q", got, wantName)
			}
		})
	}

	_, err := wfs.Stat(ctx, "/no/such/file")
	if !os.IsNotExist(err) {
		t.Errorf("Stat of missing file: got %v, want not-exist error", err)
	}

	if n := gcs.requestCount(); n != 0 {
		t.Errorf("Stat made %d GCS requests, want 0", n)
	}
}

func TestContentType(t *testing.T) {
	wd, gcs := newTestWebDAV(t, "", "", "")
	wfs := &wdFS{wd: wd}
	ctx := context.Background()

	cases := []struct {
		name, want string
		wantErr    error
	}{
		{name: "/movies/action/die-hard.mp4", want: "video/mp4"},
		{name: "/music/song.mp3", want: "audio/mpeg"},
		{name: "/top.txt", want: "text/plain; charset=utf-8"},
		{name: "/movies/readme", want: "application/octet-stream"},
		{name: "/movies", wantErr: webdav.ErrNotImplemented},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fi, err := wfs.Stat(ctx, tc.name)
			if err != nil {
				t.Fatal(err)
			}
			ct, ok := fi.(webdav.ContentTyper)
			if !ok {
				t.Fatalf("%T does not implement webdav.ContentTyper", fi)
			}
			got, err := ct.ContentType(ctx)
			if tc.wantErr != nil {
				if err != tc.wantErr { // the webdav package compares with ==
					t.Fatalf("got error %v, want %v", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}

	if n := gcs.requestCount(); n != 0 {
		t.Errorf("ContentType made %d GCS requests, want 0", n)
	}
}

func TestReaddir(t *testing.T) {
	wd, _ := newTestWebDAV(t, "", "", "")
	wfs := &wdFS{wd: wd}
	ctx := context.Background()

	open := func(t *testing.T, name string) webdav.File {
		t.Helper()
		f, err := wfs.OpenFile(ctx, name, os.O_RDONLY, 0)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { f.Close() })
		return f
	}

	names := func(infos []fs.FileInfo) []string {
		var result []string
		for _, info := range infos {
			result = append(result, info.Name())
		}
		return result
	}

	t.Run("all", func(t *testing.T) {
		for _, count := range []int{0, -1} {
			f := open(t, "/movies")
			infos, err := f.Readdir(count)
			if err != nil {
				t.Fatal(err)
			}
			want := []string{"action", "comedy", "readme"}
			if got := names(infos); !reflect.DeepEqual(got, want) {
				t.Errorf("Readdir(%d): got %v, want %v", count, got, want)
			}
			for _, info := range infos {
				wantDir := info.Name() != "readme"
				if info.IsDir() != wantDir {
					t.Errorf("%s: got IsDir %v, want %v", info.Name(), info.IsDir(), wantDir)
				}
			}

			// Subsequent call returns nothing, without error.
			infos, err = f.Readdir(count)
			if err != nil {
				t.Errorf("second Readdir(%d): got error %v, want nil", count, err)
			}
			if len(infos) != 0 {
				t.Errorf("second Readdir(%d): got %v, want nothing", count, names(infos))
			}
		}
	})

	t.Run("paged", func(t *testing.T) {
		f := open(t, "/")

		var got [][]string
		for {
			infos, err := f.Readdir(2)
			if errors.Is(err, io.EOF) {
				if len(infos) != 0 {
					t.Errorf("got %v along with io.EOF, want nothing", names(infos))
				}
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(infos) == 0 {
				t.Fatal("got empty page without io.EOF")
			}
			got = append(got, names(infos))
		}

		want := [][]string{{"movies", "music"}, {"top.txt"}}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}

		// Still at EOF.
		if _, err := f.Readdir(1); !errors.Is(err, io.EOF) {
			t.Errorf("Readdir after EOF: got %v, want io.EOF", err)
		}
	})

	t.Run("mixed", func(t *testing.T) {
		f := open(t, "/movies")
		infos, err := f.Readdir(1)
		if err != nil {
			t.Fatal(err)
		}
		if got, want := names(infos), []string{"action"}; !reflect.DeepEqual(got, want) {
			t.Errorf("Readdir(1): got %v, want %v", got, want)
		}
		infos, err = f.Readdir(0)
		if err != nil {
			t.Fatal(err)
		}
		if got, want := names(infos), []string{"comedy", "readme"}; !reflect.DeepEqual(got, want) {
			t.Errorf("Readdir(0) after Readdir(1): got %v, want %v", got, want)
		}
	})

	t.Run("child info", func(t *testing.T) {
		f := open(t, "/movies/action")
		infos, err := f.Readdir(0)
		if err != nil {
			t.Fatal(err)
		}
		if len(infos) != 2 {
			t.Fatalf("got %d entries, want 2", len(infos))
		}
		fi := infos[0]
		if fi.Name() != "die-hard.mp4" || fi.Size() != int64(len("yippee-ki-yay")) || !fi.ModTime().Equal(time.Unix(1000, 0)) {
			t.Errorf("got name=%q size=%d mtime=%v", fi.Name(), fi.Size(), fi.ModTime())
		}
	})

	t.Run("file", func(t *testing.T) {
		f := open(t, "/top.txt")
		_, err := f.Readdir(0)
		if !errors.Is(err, syscall.ENOTDIR) {
			t.Errorf("Readdir on a file: got %v, want ENOTDIR", err)
		}
	})
}

func TestDirFile(t *testing.T) {
	wd, _ := newTestWebDAV(t, "", "", "")
	wfs := &wdFS{wd: wd}
	ctx := context.Background()

	f, err := wfs.OpenFile(ctx, "/movies", os.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := f.Read(make([]byte, 10)); !errors.Is(err, syscall.EISDIR) {
		t.Errorf("Read on dir: got %v, want EISDIR", err)
	}
	if _, err := f.Seek(0, io.SeekStart); !errors.Is(err, syscall.EISDIR) {
		t.Errorf("Seek on dir: got %v, want EISDIR", err)
	}

	fi, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if !fi.IsDir() || fi.Name() != "movies" {
		t.Errorf("Stat: got name=%q isDir=%v", fi.Name(), fi.IsDir())
	}

	if err := f.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}

	if _, err := wfs.OpenFile(ctx, "/no/such/dir", os.O_RDONLY, 0); !os.IsNotExist(err) {
		t.Errorf("OpenFile of missing path: got %v, want not-exist error", err)
	}
}

func TestReadFile(t *testing.T) {
	wd, gcs := newTestWebDAV(t, "", "", "")
	wfs := &wdFS{wd: wd}
	ctx := context.Background()

	const name = "/movies/comedy/airplane.mp4"
	const content = "surely you can't be serious"

	f, err := wfs.OpenFile(ctx, name, os.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	if n := gcs.requestCount(); n != 0 {
		t.Errorf("OpenFile made %d GCS requests, want 0 (reads should be lazy)", n)
	}

	fi, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if fi.Name() != "airplane.mp4" || fi.Size() != int64(len(content)) {
		t.Errorf("Stat: got name=%q size=%d", fi.Name(), fi.Size())
	}

	got, err := io.ReadAll(f)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != content {
		t.Errorf("ReadAll: got %q, want %q", got, content)
	}

	pos, err := f.Seek(7, io.SeekStart)
	if err != nil {
		t.Fatal(err)
	}
	if pos != 7 {
		t.Errorf("Seek: got position %d, want 7", pos)
	}
	got, err = io.ReadAll(f)
	if err != nil {
		t.Fatal(err)
	}
	if want := content[7:]; string(got) != want {
		t.Errorf("ReadAll after Seek: got %q, want %q", got, want)
	}

	pos, err = f.Seek(-7, io.SeekEnd)
	if err != nil {
		t.Fatal(err)
	}
	if want := int64(len(content) - 7); pos != want {
		t.Errorf("Seek from end: got position %d, want %d", pos, want)
	}
	got, err = io.ReadAll(f)
	if err != nil {
		t.Fatal(err)
	}
	if want := content[len(content)-7:]; string(got) != want {
		t.Errorf("ReadAll after Seek from end: got %q, want %q", got, want)
	}

	gcs.mu.Lock()
	reqs := append([]string(nil), gcs.requests...)
	gcs.mu.Unlock()
	for _, r := range reqs {
		if r != "h-airplane" {
			t.Errorf("unexpected GCS request for object %q", r)
		}
	}
}

func TestSubtreeRoot(t *testing.T) {
	wd, _ := newTestWebDAV(t, "movies", "", "")
	wfs := &wdFS{wd: wd}
	ctx := context.Background()

	fi, err := wfs.Stat(ctx, "/action/die-hard.mp4")
	if err != nil {
		t.Fatal(err)
	}
	if fi.Name() != "die-hard.mp4" {
		t.Errorf("got name %q, want die-hard.mp4", fi.Name())
	}

	// Things outside the subtree are invisible.
	for _, name := range []string{"/movies", "/music", "/top.txt", "/../top.txt", "/../music/song.mp3"} {
		if _, err := wfs.Stat(ctx, name); !os.IsNotExist(err) {
			t.Errorf("Stat(%s): got %v, want not-exist error", name, err)
		}
	}

	f, err := wfs.OpenFile(ctx, "/", os.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	infos, err := f.Readdir(0)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, info := range infos {
		got = append(got, info.Name())
	}
	if want := []string{"action", "comedy", "readme"}; !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestLockSystem(t *testing.T) {
	wd, _ := newTestWebDAV(t, "", "", "")
	ls := &wdLockSystem{wd: wd, mem: webdav.NewMemLS()}
	now := time.Now()

	token, err := ls.Create(now, webdav.LockDetails{
		Root:      "/top.txt",
		Duration:  time.Minute,
		OwnerXML:  "<D:owner>me</D:owner>",
		ZeroDepth: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	// A conflicting lock is refused.
	if _, err := ls.Create(now, webdav.LockDetails{Root: "/top.txt", Duration: time.Minute, ZeroDepth: true}); !errors.Is(err, webdav.ErrLocked) {
		t.Errorf("conflicting Create: got %v, want ErrLocked", err)
	}

	// Confirm fails without the token, succeeds with it.
	if _, err := ls.Confirm(now, "/top.txt", ""); !errors.Is(err, webdav.ErrConfirmationFailed) {
		t.Errorf("Confirm without token: got %v, want ErrConfirmationFailed", err)
	}
	release, err := ls.Confirm(now, "/top.txt", "", webdav.Condition{Token: token})
	if err != nil {
		t.Fatal(err)
	}
	release()

	details, err := ls.Refresh(now, token, 2*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if details.Root != "/top.txt" || details.Duration != 2*time.Minute {
		t.Errorf("Refresh: got %+v", details)
	}

	if err := ls.Unlock(now, token); err != nil {
		t.Fatal(err)
	}
	if err := ls.Unlock(now, token); !errors.Is(err, webdav.ErrNoSuchLock) {
		t.Errorf("second Unlock: got %v, want ErrNoSuchLock", err)
	}
	if _, err := ls.Refresh(now, token, time.Minute); !errors.Is(err, webdav.ErrNoSuchLock) {
		t.Errorf("Refresh after Unlock: got %v, want ErrNoSuchLock", err)
	}
}

// HTTP-level tests, exercising the handler the way a WebDAV client would.

// serve sends req through h and returns the response.
func serve(h http.Handler, req *http.Request) *http.Response {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec.Result()
}

func newRequest(t *testing.T, method, path, body string, hdr ...string) *http.Request {
	t.Helper()
	req := httptest.NewRequest(method, "http://example.com"+path, strings.NewReader(body))
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	return req
}

type multistatus struct {
	Responses []struct {
		Href     string `xml:"href"`
		Propstat []struct {
			Prop struct {
				DisplayName   string `xml:"displayname"`
				ContentLength string `xml:"getcontentlength"`
				ContentType   string `xml:"getcontenttype"`
				LastModified  string `xml:"getlastmodified"`
				ResourceType  struct {
					Collection *struct{} `xml:"collection"`
				} `xml:"resourcetype"`
			} `xml:"prop"`
			Status string `xml:"status"`
		} `xml:"propstat"`
	} `xml:"response"`
}

func propfind(t *testing.T, h http.Handler, path, depth string) (int, *multistatus) {
	t.Helper()
	resp := serve(h, newRequest(t, "PROPFIND", path, "", "Depth", depth))
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMultiStatus {
		return resp.StatusCode, nil
	}
	var ms multistatus
	if err := xml.NewDecoder(resp.Body).Decode(&ms); err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, &ms
}

func TestHTTPPropfind(t *testing.T) {
	wd, gcs := newTestWebDAV(t, "", "", "")
	h := wd.handler()

	t.Run("depth 1 root", func(t *testing.T) {
		code, ms := propfind(t, h, "/", "1")
		if code != http.StatusMultiStatus {
			t.Fatalf("got status %d, want %d", code, http.StatusMultiStatus)
		}
		var hrefs []string
		for _, r := range ms.Responses {
			hrefs = append(hrefs, r.Href)
		}
		sort.Strings(hrefs)
		want := []string{"/", "/movies/", "/music/", "/top.txt"}
		if !reflect.DeepEqual(hrefs, want) {
			t.Errorf("got hrefs %v, want %v", hrefs, want)
		}
	})

	t.Run("depth 0 file", func(t *testing.T) {
		code, ms := propfind(t, h, "/movies/action/die-hard.mp4", "0")
		if code != http.StatusMultiStatus {
			t.Fatalf("got status %d, want %d", code, http.StatusMultiStatus)
		}
		if len(ms.Responses) != 1 {
			t.Fatalf("got %d responses, want 1", len(ms.Responses))
		}
		r := ms.Responses[0]
		if r.Href != "/movies/action/die-hard.mp4" {
			t.Errorf("got href %q", r.Href)
		}
		var found bool
		for _, ps := range r.Propstat {
			if !strings.Contains(ps.Status, "200") {
				continue
			}
			found = true
			p := ps.Prop
			if p.DisplayName != "die-hard.mp4" {
				t.Errorf("displayname: got %q", p.DisplayName)
			}
			if p.ContentLength != fmt.Sprint(len("yippee-ki-yay")) {
				t.Errorf("getcontentlength: got %q", p.ContentLength)
			}
			if p.ContentType != "video/mp4" {
				t.Errorf("getcontenttype: got %q", p.ContentType)
			}
			if want := time.Unix(1000, 0).UTC().Format(http.TimeFormat); p.LastModified != want {
				t.Errorf("getlastmodified: got %q, want %q", p.LastModified, want)
			}
			if p.ResourceType.Collection != nil {
				t.Error("resourcetype: got collection, want none")
			}
		}
		if !found {
			t.Error("no 200 propstat in response")
		}
	})

	t.Run("depth 0 dir", func(t *testing.T) {
		code, ms := propfind(t, h, "/movies", "0")
		if code != http.StatusMultiStatus {
			t.Fatalf("got status %d, want %d", code, http.StatusMultiStatus)
		}
		if len(ms.Responses) != 1 {
			t.Fatalf("got %d responses, want 1", len(ms.Responses))
		}
		var isCollection bool
		for _, ps := range ms.Responses[0].Propstat {
			if ps.Prop.ResourceType.Collection != nil {
				isCollection = true
			}
		}
		if !isCollection {
			t.Error("resourcetype: want collection")
		}
	})

	t.Run("depth 1 dir", func(t *testing.T) {
		_, ms := propfind(t, h, "/movies/action/", "1")
		var hrefs []string
		for _, r := range ms.Responses {
			hrefs = append(hrefs, r.Href)
		}
		sort.Strings(hrefs)
		want := []string{"/movies/action/", "/movies/action/die-hard.mp4", "/movies/action/speed.mkv"}
		if !reflect.DeepEqual(hrefs, want) {
			t.Errorf("got hrefs %v, want %v", hrefs, want)
		}
	})

	t.Run("missing", func(t *testing.T) {
		code, _ := propfind(t, h, "/nope", "0")
		if code != http.StatusNotFound {
			t.Errorf("got status %d, want %d", code, http.StatusNotFound)
		}
	})

	// Directory listings must not read file contents from GCS
	// (e.g. to sniff content types).
	if n := gcs.requestCount(); n != 0 {
		t.Errorf("PROPFIND made %d GCS requests, want 0", n)
	}
}

func TestHTTPGet(t *testing.T) {
	wd, _ := newTestWebDAV(t, "", "", "")
	h := wd.handler()

	t.Run("full", func(t *testing.T) {
		resp := serve(h, newRequest(t, "GET", "/movies/action/speed.mkv", ""))
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("got status %d, want 200", resp.StatusCode)
		}
		body, _ := io.ReadAll(resp.Body)
		if string(body) != "pop quiz, hotshot" {
			t.Errorf("got body %q", body)
		}
		if got := resp.Header.Get("Last-Modified"); got != time.Unix(2000, 0).UTC().Format(http.TimeFormat) {
			t.Errorf("got Last-Modified %q", got)
		}
		if resp.Header.Get("ETag") == "" {
			t.Error("missing ETag")
		}
	})

	t.Run("range", func(t *testing.T) {
		resp := serve(h, newRequest(t, "GET", "/movies/action/speed.mkv", "", "Range", "bytes=4-7"))
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusPartialContent {
			t.Fatalf("got status %d, want 206", resp.StatusCode)
		}
		body, _ := io.ReadAll(resp.Body)
		if string(body) != "quiz" {
			t.Errorf("got body %q, want %q", body, "quiz")
		}
	})

	t.Run("head", func(t *testing.T) {
		resp := serve(h, newRequest(t, "HEAD", "/top.txt", ""))
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("got status %d, want 200", resp.StatusCode)
		}
		if got := resp.Header.Get("Content-Length"); got != fmt.Sprint(len("top-level file")) {
			t.Errorf("got Content-Length %q", got)
		}
	})

	t.Run("missing", func(t *testing.T) {
		resp := serve(h, newRequest(t, "GET", "/nope.txt", ""))
		resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("got status %d, want 404", resp.StatusCode)
		}
	})

	t.Run("dir", func(t *testing.T) {
		resp := serve(h, newRequest(t, "GET", "/movies/", ""))
		resp.Body.Close()
		if resp.StatusCode != http.StatusMethodNotAllowed {
			t.Errorf("got status %d, want 405", resp.StatusCode)
		}
	})
}

func TestHTTPWritesRejected(t *testing.T) {
	wd, _ := newTestWebDAV(t, "", "", "")
	h := wd.handler()

	cases := []struct {
		method, path, body string
		hdr                []string
	}{
		{method: "PUT", path: "/new.txt", body: "hello"},
		{method: "PUT", path: "/top.txt", body: "overwrite"},
		{method: "DELETE", path: "/top.txt"},
		{method: "DELETE", path: "/movies/"},
		{method: "MKCOL", path: "/newdir"},
		{method: "MOVE", path: "/top.txt", hdr: []string{"Destination", "http://example.com/moved.txt"}},
		{method: "COPY", path: "/top.txt", hdr: []string{"Destination", "http://example.com/copied.txt"}},
		{method: "PROPPATCH", path: "/top.txt", body: `<?xml version="1.0"?><D:propertyupdate xmlns:D="DAV:"><D:set><D:prop><D:displayname>x</D:displayname></D:prop></D:set></D:propertyupdate>`},
	}

	for _, tc := range cases {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			resp := serve(h, newRequest(t, tc.method, tc.path, tc.body, tc.hdr...))
			resp.Body.Close()
			if resp.StatusCode < 400 && resp.StatusCode != http.StatusMultiStatus {
				t.Errorf("got status %d, want an error", resp.StatusCode)
			}
		})
	}

	// Nothing changed.
	_, ms := propfind(t, h, "/", "infinity")
	var hrefs []string
	for _, r := range ms.Responses {
		hrefs = append(hrefs, r.Href)
	}
	sort.Strings(hrefs)
	want := []string{
		"/",
		"/movies/",
		"/movies/action/",
		"/movies/action/die-hard.mp4",
		"/movies/action/speed.mkv",
		"/movies/comedy/",
		"/movies/comedy/airplane.mp4",
		"/movies/readme",
		"/music/",
		"/music/song.mp3",
		"/top.txt",
	}
	if !reflect.DeepEqual(hrefs, want) {
		t.Errorf("after write attempts, got tree %v, want %v", hrefs, want)
	}
}

func TestHTTPLock(t *testing.T) {
	wd, _ := newTestWebDAV(t, "", "", "")
	h := wd.handler()

	const lockBody = `<?xml version="1.0" encoding="utf-8"?>
<D:lockinfo xmlns:D="DAV:">
  <D:lockscope><D:exclusive/></D:lockscope>
  <D:locktype><D:write/></D:locktype>
  <D:owner>tester</D:owner>
</D:lockinfo>`

	resp := serve(h, newRequest(t, "LOCK", "/top.txt", lockBody, "Timeout", "Second-60", "Depth", "0"))
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("LOCK: got status %d, want 200", resp.StatusCode)
	}
	token := resp.Header.Get("Lock-Token")
	if token == "" {
		t.Fatal("LOCK: no Lock-Token header")
	}

	// A second exclusive lock conflicts.
	resp = serve(h, newRequest(t, "LOCK", "/top.txt", lockBody, "Timeout", "Second-60", "Depth", "0"))
	resp.Body.Close()
	if resp.StatusCode != http.StatusLocked {
		t.Errorf("second LOCK: got status %d, want %d", resp.StatusCode, http.StatusLocked)
	}

	resp = serve(h, newRequest(t, "UNLOCK", "/top.txt", "", "Lock-Token", token))
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("UNLOCK: got status %d, want %d", resp.StatusCode, http.StatusNoContent)
	}

	// Can lock again after unlocking.
	resp = serve(h, newRequest(t, "LOCK", "/top.txt", lockBody, "Timeout", "Second-60", "Depth", "0"))
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("LOCK after UNLOCK: got status %d, want 200", resp.StatusCode)
	}

	// Locking a nonexistent resource would normally create it,
	// which must fail on a read-only filesystem.
	resp = serve(h, newRequest(t, "LOCK", "/brand-new.txt", lockBody, "Timeout", "Second-60", "Depth", "0"))
	resp.Body.Close()
	if resp.StatusCode < 400 {
		t.Errorf("LOCK of nonexistent resource: got status %d, want an error", resp.StatusCode)
	}
	if _, err := (&wdFS{wd: wd}).Stat(context.Background(), "/brand-new.txt"); !os.IsNotExist(err) {
		t.Errorf("after LOCK of nonexistent resource, Stat: got %v, want not-exist error", err)
	}
}

func TestHTTPOptions(t *testing.T) {
	wd, _ := newTestWebDAV(t, "", "", "")
	h := wd.handler()

	resp := serve(h, newRequest(t, "OPTIONS", "/movies/", ""))
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("got status %d, want 200", resp.StatusCode)
	}
	if dav := resp.Header.Get("DAV"); !strings.Contains(dav, "1") {
		t.Errorf("got DAV header %q", dav)
	}
	if allow := resp.Header.Get("Allow"); !strings.Contains(allow, "PROPFIND") {
		t.Errorf("got Allow header %q, want it to include PROPFIND", allow)
	}
}

func TestHTTPAuth(t *testing.T) {
	t.Run("required", func(t *testing.T) {
		wd, gcs := newTestWebDAV(t, "", "alice", "s3cret")
		h := wd.handler()

		cases := []struct {
			name     string
			setAuth  bool
			user, pw string
			want     int
		}{
			{name: "none", want: http.StatusUnauthorized},
			{name: "wrong password", setAuth: true, user: "alice", pw: "wrong", want: http.StatusUnauthorized},
			{name: "wrong user", setAuth: true, user: "bob", pw: "s3cret", want: http.StatusUnauthorized},
			{name: "empty", setAuth: true, want: http.StatusUnauthorized},
			{name: "prefix", setAuth: true, user: "alice", pw: "s3cre", want: http.StatusUnauthorized},
			{name: "correct", setAuth: true, user: "alice", pw: "s3cret", want: http.StatusOK},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				req := newRequest(t, "GET", "/top.txt", "")
				if tc.setAuth {
					req.SetBasicAuth(tc.user, tc.pw)
				}
				resp := serve(h, req)
				resp.Body.Close()
				if resp.StatusCode != tc.want {
					t.Errorf("got status %d, want %d", resp.StatusCode, tc.want)
				}
				if tc.want == http.StatusUnauthorized {
					if got := resp.Header.Get("WWW-Authenticate"); !strings.HasPrefix(got, "Basic ") {
						t.Errorf("got WWW-Authenticate %q, want Basic challenge", got)
					}
				}
			})
		}

		// Unauthorized requests of every method are refused before touching the filesystem.
		before := gcs.requestCount()
		for _, method := range []string{"PROPFIND", "GET", "LOCK", "PUT", "DELETE"} {
			resp := serve(h, newRequest(t, method, "/top.txt", ""))
			resp.Body.Close()
			if resp.StatusCode != http.StatusUnauthorized {
				t.Errorf("unauthenticated %s: got status %d, want 401", method, resp.StatusCode)
			}
		}
		if n := gcs.requestCount() - before; n != 0 {
			t.Errorf("unauthenticated requests made %d GCS requests, want 0", n)
		}
	})

	// Auth is enforced only when both username and password are set.
	for _, creds := range [][2]string{{"", ""}, {"alice", ""}, {"", "s3cret"}} {
		t.Run(fmt.Sprintf("disabled user=%q pw=%q", creds[0], creds[1]), func(t *testing.T) {
			wd, _ := newTestWebDAV(t, "", creds[0], creds[1])
			resp := serve(wd.handler(), newRequest(t, "GET", "/top.txt", ""))
			resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Errorf("got status %d, want 200", resp.StatusCode)
			}
		})
	}
}

// doWebDAV tests.

// writeListFile writes testFiles in the format of "gcsbackup list" output
// and returns the file's path.
func writeListFile(t *testing.T) string {
	t.Helper()

	listfile := filepath.Join(t.TempDir(), "list.json")
	f, err := os.Create(listfile)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	for _, tf := range testFiles {
		l := listType{
			Paths: map[string]time.Time{tf.path: time.Unix(tf.mtime, 0)},
			Size:  int64(len(tf.content)),
			Hash:  tf.hash,
		}
		if err := enc.Encode(l); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	return listfile
}

func TestDoWebDAVErrors(t *testing.T) {
	wd, _ := newTestWebDAV(t, "", "", "")
	c := maincmd{bucket: wd.bucket}
	listfile := writeListFile(t)

	cases := []struct {
		name, dir, listen, listfile string
		wantErr                     error
	}{
		{name: "missing dir", dir: "no/such/dir", listen: "127.0.0.1:0", listfile: listfile, wantErr: fs.ErrNotExist},
		{name: "dir is a file", dir: "top.txt", listen: "127.0.0.1:0", listfile: listfile, wantErr: syscall.ENOTDIR},
		{name: "missing list file", dir: "", listen: "127.0.0.1:0", listfile: filepath.Join(t.TempDir(), "nope.json"), wantErr: fs.ErrNotExist},
		{name: "bad listen address", dir: "movies", listen: "not a valid address", listfile: listfile},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			err := c.doWebDAV(ctx, tc.dir, tc.listen, "", "", tc.listfile, "", "", nil)
			if err == nil {
				t.Fatal("got nil error")
			}
			if ctx.Err() != nil {
				t.Fatalf("doWebDAV did not return promptly: %v", err)
			}
			if tc.wantErr != nil && !errors.Is(err, tc.wantErr) {
				t.Errorf("got error %v, want one matching %v", err, tc.wantErr)
			}
		})
	}
}

func TestDoWebDAVShutdown(t *testing.T) {
	wd, _ := newTestWebDAV(t, "", "", "")
	c := maincmd{bucket: wd.bucket}
	listfile := writeListFile(t)

	ctx, cancel := context.WithCancel(context.Background())

	errch := make(chan error, 1)
	go func() {
		errch <- c.doWebDAV(ctx, "movies", "127.0.0.1:0", "", "", listfile, "", "", nil)
	}()

	// Give the server a moment to start (or fail to start).
	select {
	case err := <-errch:
		if err != nil && strings.Contains(err.Error(), "operation not permitted") {
			t.Skipf("cannot listen in this environment: %v", err)
		}
		t.Fatalf("doWebDAV exited before cancellation: %v", err)
	case <-time.After(200 * time.Millisecond):
	}

	cancel()

	select {
	case err := <-errch:
		if err != nil {
			t.Errorf("got error %v, want nil after clean shutdown", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("doWebDAV did not return after context cancellation")
	}
}
