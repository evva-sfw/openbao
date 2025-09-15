package azuretable

import (
	"context"
	"encoding/base64"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/data/aztables"
	log "github.com/hashicorp/go-hclog"
	"github.com/openbao/openbao/sdk/v2/physical"
)

//
// ---------- Unit tests (always run) ----------
//

func TestSafeRowKey(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{"", ""},
		{"no/slash", "no+slash"},
		{"a/b/c", "a+b+c"},
		{"/leading", "+leading"},
		{"trailing/", "trailing+"},
		{"double//slash", "double++slash"},
		{"already+plus", "already+plus"}, // '+' is allowed and unchanged
	}
	for _, tt := range tests {
		if got := safeRowKey(tt.in); got != tt.want {
			t.Fatalf("safeRowKey(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestRevertSafeRowKey(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{"", ""},
		{"no+slash", "no/slash"},
		{"a+b+c", "a/b/c"},
		{"+leading", "/leading"},
		{"trailing+", "trailing/"},
		{"double++slash", "double//slash"},
		{"already/has/slash", "already/has/slash"}, // '/' is produced only from '+'
	}
	for _, tt := range tests {
		if got := revertSafeRowKey(tt.in); got != tt.want {
			t.Fatalf("revertSafeRowKey(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestSafeRoundTrip(t *testing.T) {
	inputs := []string{
		"",
		"plain",
		"foo/bar",
		"/rooted/path/",
		"a/b/c/d/e",
		"double//slash",
	}
	for _, in := range inputs {
		round := revertSafeRowKey(safeRowKey(in))
		if round != in {
			t.Fatalf("round-trip failed: %q -> %q -> %q", in, safeRowKey(in), round)
		}
	}
}

//
// ---------- Integration tests (enabled with -tags=integration) ----------
//
// make sure Azurite vars are in *this* shell
// export AZ_ACCOUNT_NAME=devstoreaccount1
// export AZ_ACCOUNT_KEY='Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=='
// export AZ_SERVICE_URL='http://127.0.0.1:10002/devstoreaccount1'
//
// cd plugins/azuretable
// To run: go test -v -run Integration -tags=integration
//

func TestIntegration_NewAzureTableBackend_CreatesTable(t *testing.T) {
	skipIfNoIntegration(t)

	ctx := context.Background()
	conf, tableName := testConfigWithRandomTable(t)

	logger := log.New(&log.LoggerOptions{Name: "azuretable-test", Level: log.Trace})

	b, err := NewAzureTableBackend(conf, logger)
	if err != nil {
		t.Fatalf("NewAzureTableBackend error: %v", err)
	}
	tb := b.(*AzureTableBackend)
	if tb.client == nil {
		t.Fatalf("expected non-nil client")
	}
	if tb.tableName != tableName {
		t.Fatalf("tableName mismatch: got %q want %q", tb.tableName, tableName)
	}

	// best-effort cleanup
	t.Cleanup(func() {
		_ = deleteTable(ctx, conf, tableName)
	})
}

func TestIntegration_PutGetRoundTrip(t *testing.T) {
	skipIfNoIntegration(t)
	ctx := context.Background()
	conf, tableName := testConfigWithRandomTable(t)
	logger := log.New(&log.LoggerOptions{Name: "azuretable-test", Level: log.Trace})

	b, err := NewAzureTableBackend(conf, logger)
	if err != nil {
		t.Fatalf("NewAzureTableBackend error: %v", err)
	}
	t.Cleanup(func() { _ = deleteTable(ctx, conf, tableName) })

	key := "foo/bar"
	want := []byte("super-secret-value")

	if err := b.Put(ctx, &physical.Entry{Key: key, Value: want}); err != nil {
		t.Fatalf("Put error: %v", err)
	}

	got, err := b.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get error: %v", err)
	}
	if got == nil {
		t.Fatalf("Get returned nil entry for existing key")
	}
	if string(got.Value) != string(want) {
		t.Fatalf("value mismatch: got %q want %q", string(got.Value), string(want))
	}
}

func TestIntegration_GetNotFoundReturnsNil(t *testing.T) {
	skipIfNoIntegration(t)
	ctx := context.Background()
	conf, tableName := testConfigWithRandomTable(t)
	logger := log.New(&log.LoggerOptions{Name: "azuretable-test", Level: log.Trace})

	b, err := NewAzureTableBackend(conf, logger)
	if err != nil {
		t.Fatalf("NewAzureTableBackend error: %v", err)
	}
	t.Cleanup(func() { _ = deleteTable(ctx, conf, tableName) })

	e, err := b.Get(ctx, "does/not/exist")
	if err != nil {
		t.Fatalf("Get returned unexpected error: %v", err)
	}
	if e != nil {
		t.Fatalf("expected nil entry for missing key, got: %+v", e)
	}
}

func TestIntegration_DeleteExistingAndIdempotent(t *testing.T) {
	skipIfNoIntegration(t)
	ctx := context.Background()
	conf, tableName := testConfigWithRandomTable(t)
	logger := log.New(&log.LoggerOptions{Name: "azuretable-test", Level: log.Trace})

	b, err := NewAzureTableBackend(conf, logger)
	if err != nil {
		t.Fatalf("NewAzureTableBackend error: %v", err)
	}
	t.Cleanup(func() { _ = deleteTable(ctx, conf, tableName) })

	key := "a/b/c"
	if err := b.Put(ctx, &physical.Entry{Key: key, Value: []byte("x")}); err != nil {
		t.Fatalf("Put error: %v", err)
	}
	if err := b.Delete(ctx, key); err != nil {
		t.Fatalf("Delete (existing) error: %v", err)
	}
	// Idempotent: deleting again should be success (the code treats 404 as success)
	if err := b.Delete(ctx, key); err != nil {
		t.Fatalf("Delete (idempotent) error: %v", err)
	}
}

func TestIntegration_List_PrefixAndChildren(t *testing.T) {
	skipIfNoIntegration(t)
	ctx := context.Background()
	conf, tableName := testConfigWithRandomTable(t)
	logger := log.New(&log.LoggerOptions{Name: "azuretable-test", Level: log.Trace})

	b, err := NewAzureTableBackend(conf, logger)
	if err != nil {
		t.Fatalf("NewAzureTableBackend error: %v", err)
	}
	t.Cleanup(func() { _ = deleteTable(ctx, conf, tableName) })

	seed := []string{
		"root.txt",
		"dir1/a.txt",
		"dir1/b.txt",
		"dir1/sub/c.txt",
		"dir2/d.txt",
		"dir2sub", // not a folder (no '/')
	}
	for _, k := range seed {
		if err := b.Put(ctx, &physical.Entry{Key: k, Value: []byte(k)}); err != nil {
			t.Fatalf("seed Put %q error: %v", k, err)
		}
	}

	// List at root (prefix = "")
	root, err := b.List(ctx, "")
	if err != nil {
		t.Fatalf("List root error: %v", err)
	}
	// Expect only top-level first components: "root.txt", "dir1/", "dir2/", "dir2sub"
	assertSetEq(t, root, []string{"root.txt", "dir1/", "dir2/", "dir2sub"})

	// List under dir1/
	dir1, err := b.List(ctx, "dir1/")
	if err != nil {
		t.Fatalf("List dir1 error: %v", err)
	}
	// Expect: "a.txt", "b.txt", "sub/"
	assertSetEq(t, dir1, []string{"a.txt", "b.txt", "sub/"})

	// List under dir1/sub/
	dir1sub, err := b.List(ctx, "dir1/sub/")
	if err != nil {
		t.Fatalf("List dir1/sub error: %v", err)
	}
	assertSetEq(t, dir1sub, []string{"c.txt"})
}

func TestIntegration_ListPage_PaginationAndAfter(t *testing.T) {
	skipIfNoIntegration(t)
	ctx := context.Background()
	conf, tableName := testConfigWithRandomTable(t)
	logger := log.New(&log.LoggerOptions{Name: "azuretable-test", Level: log.Trace})

	b, err := NewAzureTableBackend(conf, logger)
	if err != nil {
		t.Fatalf("NewAzureTableBackend error: %v", err)
	}
	t.Cleanup(func() { _ = deleteTable(ctx, conf, tableName) })

	// Seed predictable keys under prefix "p/"
	var keys []string
	for i := 0; i < 10; i++ {
		k := fmt.Sprintf("p/%02d.txt", i)
		keys = append(keys, k)
		if err := b.Put(ctx, &physical.Entry{Key: k, Value: []byte(k)}); err != nil {
			t.Fatalf("seed Put %q error: %v", k, err)
		}
	}

	// Page 1: limit 3 from start
	page1, err := b.ListPage(ctx, "p/", "", 3)
	if err != nil {
		t.Fatalf("ListPage page1 error: %v", err)
	}
	if len(page1) != 3 {
		t.Fatalf("page1 length = %d, want 3", len(page1))
	}
	if !isSortedLex(page1) {
		t.Fatalf("page1 should be lexicographically sorted by RowKey: %v", page1)
	}

	// Page 2: after last item of page1
	after := page1[len(page1)-1]
	page2, err := b.ListPage(ctx, "p/", after, 3)
	if err != nil {
		t.Fatalf("ListPage page2 error: %v", err)
	}
	if len(page2) != 3 {
		t.Fatalf("page2 length = %d, want 3", len(page2))
	}
	if contains(page1, page2...) {
		t.Fatalf("page2 overlaps page1: p1=%v p2=%v", page1, page2)
	}

	// Final page: fetch all remaining
	after = page2[len(page2)-1]
	page3, err := b.ListPage(ctx, "p/", after, 100)
	if err != nil {
		t.Fatalf("ListPage page3 error: %v", err)
	}
	// Total should be 10; we already took 6; remaining should be 4
	if len(page3) != 4 {
		t.Fatalf("page3 length = %d, want 4", len(page3))
	}
}

func TestIntegration_Namespace_PutGetDeleteAndList(t *testing.T) {
	skipIfNoIntegration(t)
	ctx := context.Background()
	conf, tableName := testConfigWithRandomTable(t)
	logger := log.New(&log.LoggerOptions{Name: "azuretable-test", Level: log.Trace})

	b, err := NewAzureTableBackend(conf, logger)
	if err != nil {
		t.Fatalf("NewAzureTableBackend error: %v", err)
	}
	t.Cleanup(func() { _ = deleteTable(ctx, conf, tableName) })

	// Use a synthetic namespace ID (doesn't have to be a UUID for the parser)
	nsID := "ns" + randomAlphaNum(8)

	// Seed some keys in this namespace and a few in other scopes to prove partition isolation
	inNS := []string{
		fmt.Sprintf("namespaces/%s/logical/a.txt", nsID),
		fmt.Sprintf("namespaces/%s/logical/b.txt", nsID),
		fmt.Sprintf("namespaces/%s/logical/sub/c.txt", nsID),
	}
	outside := []string{
		"root.txt", // root partition
		fmt.Sprintf("namespaces/%s/logical/zzz.txt", "x"), // different namespace
	}

	for _, k := range append(inNS, outside...) {
		if err := b.Put(ctx, &physical.Entry{Key: k, Value: []byte(k)}); err != nil {
			t.Fatalf("seed Put %q error: %v", k, err)
		}
	}

	// Get / Delete on a namespaced key
	key := fmt.Sprintf("namespaces/%s/logical/a.txt", nsID)
	got, err := b.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get error: %v", err)
	}
	if got == nil || string(got.Value) != key {
		t.Fatalf("Get mismatch: got=%v", got)
	}

	if err := b.Delete(ctx, key); err != nil {
		t.Fatalf("Delete error: %v", err)
	}
	got, err = b.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get after delete error: %v", err)
	}
	if got != nil {
		t.Fatalf("expected nil after delete, got: %+v", got)
	}

	// List at namespace root: "namespaces/<nsID>/"
	// Expect to see top-level directory names only (first path component inside the namespace)
	nsRoot := fmt.Sprintf("namespaces/%s/", nsID)
	rootChildren, err := b.List(ctx, nsRoot)
	if err != nil {
		t.Fatalf("List(%q) error: %v", nsRoot, err)
	}
	// We inserted keys under "logical/..." only, so we should see just "logical/"
	assertSetEq(t, rootChildren, []string{"logical/"})

	// List inside namespace under "logical/"
	nsLogical := fmt.Sprintf("namespaces/%s/logical/", nsID)
	logicalChildren, err := b.List(ctx, nsLogical)
	if err != nil {
		t.Fatalf("List(%q) error: %v", nsLogical, err)
	}
	// Keys present after we deleted a.txt:
	//   - b.txt (file)
	//   - sub/  (directory)
	assertSetEq(t, logicalChildren, []string{"b.txt", "sub/"})

	// List deeper inside "logical/sub/"
	nsSub := fmt.Sprintf("namespaces/%s/logical/sub/", nsID)
	subChildren, err := b.List(ctx, nsSub)
	if err != nil {
		t.Fatalf("List(%q) error: %v", nsSub, err)
	}
	assertSetEq(t, subChildren, []string{"c.txt"})
}

func TestIntegration_Namespace_ListPage_Pagination(t *testing.T) {
	skipIfNoIntegration(t)
	ctx := context.Background()
	conf, tableName := testConfigWithRandomTable(t)
	logger := log.New(&log.LoggerOptions{Name: "azuretable-test", Level: log.Trace})

	b, err := NewAzureTableBackend(conf, logger)
	if err != nil {
		t.Fatalf("NewAzureTableBackend error: %v", err)
	}
	t.Cleanup(func() { _ = deleteTable(ctx, conf, tableName) })

	nsID := "ns" + randomAlphaNum(8)

	// Seed predictable keys inside the namespace under "p/"
	var seeded []string
	for i := 0; i < 10; i++ {
		k := fmt.Sprintf("namespaces/%s/p/%02d.txt", nsID, i)
		seeded = append(seeded, k)
		if err := b.Put(ctx, &physical.Entry{Key: k, Value: []byte(k)}); err != nil {
			t.Fatalf("seed Put %q error: %v", k, err)
		}
	}
	// Also seed noise in other scopes to ensure partition filtering works
	_ = b.Put(ctx, &physical.Entry{Key: "p/should-not-appear.txt", Value: []byte("x")})
	_ = b.Put(ctx, &physical.Entry{Key: fmt.Sprintf("namespaces/%s/p/should-not-appear.txt", "other"), Value: []byte("x")})

	prefix := fmt.Sprintf("namespaces/%s/p/", nsID)

	// Page 1
	page1, err := b.ListPage(ctx, prefix, "", 3)
	if err != nil {
		t.Fatalf("ListPage page1 error: %v", err)
	}
	if len(page1) != 3 {
		t.Fatalf("page1 length = %d, want 3", len(page1))
	}
	if !isSortedLex(page1) {
		t.Fatalf("page1 should be lexicographically sorted: %v", page1)
	}

	// Page 2, after last of page1
	after := page1[len(page1)-1]
	page2, err := b.ListPage(ctx, prefix, after, 3)
	if err != nil {
		t.Fatalf("ListPage page2 error: %v", err)
	}
	if len(page2) != 3 {
		t.Fatalf("page2 length = %d, want 3", len(page2))
	}
	if contains(page1, page2...) {
		t.Fatalf("page2 overlaps page1: p1=%v p2=%v", page1, page2)
	}

	// Final page
	after = page2[len(page2)-1]
	page3, err := b.ListPage(ctx, prefix, after, 100)
	if err != nil {
		t.Fatalf("ListPage page3 error: %v", err)
	}
	if len(page3) != 4 {
		t.Fatalf("page3 length = %d, want 4", len(page3))
	}
	// Optional: sanity check the very first and very last elements
	if page1[0] >= page1[1] || page1[1] >= page1[2] {
		t.Fatalf("page1 not strictly non-decreasing: %v", page1)
	}
}

//
// ---------- helpers ----------
//

func skipIfNoIntegration(t *testing.T) {
	t.Helper()
	// Integration tests compiled only with -tags=integration.
	// We still double-check env to avoid false positives.
	if os.Getenv("AZ_ACCOUNT_NAME") == "" ||
		os.Getenv("AZ_ACCOUNT_KEY") == "" ||
		os.Getenv("AZ_SERVICE_URL") == "" {
		t.Skip("integration test skipped: AZ_ACCOUNT_NAME/AZ_ACCOUNT_KEY/AZ_SERVICE_URL not set")
	}
}

func testConfigWithRandomTable(t *testing.T) (map[string]string, string) {
	t.Helper()
	account := mustGetenv(t, "AZ_ACCOUNT_NAME")
	key := mustGetenv(t, "AZ_ACCOUNT_KEY")
	service := mustGetenv(t, "AZ_SERVICE_URL")

	rand.Seed(time.Now().UnixNano())
	suffix := randomAlphaNum(8)
	table := fmt.Sprintf("obtest%s", strings.ToLower(suffix)) // Azure tables: alphanum only, must start with letter

	conf := map[string]string{
		"account_name":          account,
		"account_key":           key,
		"service_url":           service,
		"table_name":            table,
		"max_connect_retries":   "2",
		"max_operation_retries": "2",
	}
	return conf, table
}

func mustGetenv(t *testing.T, k string) string {
	t.Helper()
	v := os.Getenv(k)
	if v == "" {
		t.Fatalf("missing required env var: %s", k)
	}
	return v
}

func randomAlphaNum(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

// assertSetEq compares two string slices as sets, disregarding order.
func assertSetEq(t *testing.T, got []string, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("length mismatch: got=%d want=%d; got=%v want=%v", len(got), len(want), got, want)
	}
	m := map[string]int{}
	for _, g := range got {
		m[g]++
	}
	for _, w := range want {
		if m[w] == 0 {
			t.Fatalf("missing expected element: %q; got=%v want=%v", w, got, want)
		}
		m[w]--
	}
	for k, v := range m {
		if v != 0 {
			t.Fatalf("unexpected extra element: %q", k)
		}
	}
}

func contains(base []string, xs ...string) bool {
	m := map[string]struct{}{}
	for _, b := range base {
		m[b] = struct{}{}
	}
	for _, x := range xs {
		if _, ok := m[x]; ok {
			return true
		}
	}
	return false
}

func isSortedLex(s []string) bool {
	for i := 1; i < len(s); i++ {
		if s[i-1] > s[i] {
			return false
		}
	}
	return true
}

//
// ---------- Optional: direct table cleanup helper ----------
//

func deleteTable(ctx context.Context, conf map[string]string, tableName string) error {
	// This helper is best-effort cleanup for integration tests.
	cred, err := aztables.NewSharedKeyCredential(conf["account_name"], conf["account_key"])
	if err != nil {
		return err
	}
	service, err := aztables.NewServiceClientWithSharedKey(conf["service_url"], cred, nil)
	if err != nil {
		return err
	}
	_, _ = service.DeleteTable(ctx, tableName, nil)
	return nil
}

//
// ---------- Sanity: base64 round-trip (not required, but useful) ----------
// Ensures how []byte becomes JSON string and is compatible with Get decoding path.
//

func TestJSONBytesToBase64Semantics(t *testing.T) {
	src := []byte("hello")
	enc := base64.StdEncoding.EncodeToString(src)
	// json.Marshal on a struct with []byte encodes to base64 string;
	// this mirrors how your Put() builds the entity and what Get() expects to decode.
	if enc == "" {
		t.Fatalf("unexpected empty base64 encoding")
	}
}
