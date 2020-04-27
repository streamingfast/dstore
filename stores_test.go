package dstore

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// gs://blah/indexes    "shards-200/0000"

var bctx = context.Background()

func TestWalkLocalIgnoreNotFound(t *testing.T) {
	dir, err := ioutil.TempDir("", "")
	require.NoError(t, err)

	defer os.RemoveAll(dir)

	s, err := NewStore("file://"+dir, "jsonl.gz", "gz", false)
	assert.NoError(t, err)

	err = s.Walk(bctx, "bubblicious/0000", "", func(f string) error { return nil })
	require.NoError(t, err)
}

func TestWalkLocalPathPrefix(t *testing.T) {
	expected := []string{"0001", "0002", "0003"}

	dir, err := ioutil.TempDir("", "")
	require.NoError(t, err)

	require.NoError(t, os.Mkdir(filepath.Join(dir, "0000"), 0755))

	defer os.RemoveAll(dir)

	s, err := NewStore("file://"+dir, "jsonl.gz", "gz", false)
	assert.NoError(t, err)

	for _, f := range expected {
		s.WriteObject(bctx, filepath.Join("0000", f), strings.NewReader("."))
	}

	seen := []string{}
	err = s.Walk(bctx, "0000", "", func(f string) error {
		seen = append(seen, f)
		exists, err := s.FileExists(bctx, filepath.Join("0000", f))
		assert.NoError(t, err)
		assert.True(t, exists)
		return nil
	})
	require.NoError(t, err)
	assert.EqualValues(t, expected, seen)
}

func TestWalkLocalFilePrefix(t *testing.T) {
	expected := []string{"00000001", "00000002", "00000003"}

	dir, err := ioutil.TempDir("", "")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	s, err := NewStore("file://"+dir, "jsonl.gz", "gz", false)
	assert.NoError(t, err)
	for _, f := range expected {
		s.WriteObject(bctx, f, strings.NewReader("."))
	}

	seen := []string{}
	err = s.Walk(bctx, "0000", "", func(f string) error {
		seen = append(seen, f)
		exists, err := s.FileExists(bctx, f)
		assert.NoError(t, err)
		assert.True(t, exists)
		return nil
	})
	require.NoError(t, err)
	assert.EqualValues(t, expected, seen)
}

func TestConcurrentOverwrite(t *testing.T) {
	t.Skip() // need GS access to test this, do it on your PC

	path := "gs://example/dev"
	s, err := NewStore(fmt.Sprintf("%s/tmp-nooverwrite-%012d", path, time.Now().UnixNano()), "jsonl.gz", "gz", true)
	require.NoError(t, err)

	e1 := make(chan error)
	e2 := make(chan error)

	// Write the same file simultaneously
	go func() {
		err := s.WriteObject(bctx, "samefile", strings.NewReader("abcdefghijklmnopqrstuvwxyz"))
		e1 <- err
	}()
	go func() {
		err := s.WriteObject(bctx, "samefile", strings.NewReader("abcdefghijklmnopqrstuvwxyz"))
		e2 <- err
	}()
	err1 := <-e1
	require.NoError(t, err1)
	err2 := <-e2
	require.NoError(t, err2)

	// Write the same file afterwards
	err = s.WriteObject(bctx, "samefile", strings.NewReader("pleasewriteme"))
	require.NoError(t, err)

	o, err := s.OpenObject(bctx, "samefile")
	require.NoError(t, err)

	assert.Equal(t, "pleasewriteme", readFile(t, o), "overwrite should be true")
}

func TestConcurrentNoOverwrite(t *testing.T) {
	t.Skip() // need GS access to test this, do it on your PC

	path := "gs://example/dev"
	s, err := NewStore(fmt.Sprintf("%s/tmp-nooverwrite-%012d", path, time.Now().UnixNano()), "jsonl.gz", "gz", false)
	require.NoError(t, err)

	e1 := make(chan error)
	e2 := make(chan error)

	// Write the same file simultaneously
	go func() {
		err := s.WriteObject(bctx, "samefile", strings.NewReader("abcdefghijklmnopqrstuvwxyz"))
		e1 <- err
	}()
	go func() {
		err := s.WriteObject(bctx, "samefile", strings.NewReader("abcdefghijklmnopqrstuvwxyz"))
		e2 <- err
	}()
	err1 := <-e1
	require.NoError(t, err1)
	err2 := <-e2
	require.NoError(t, err2)

	// Write the same file afterwards
	err = s.WriteObject(bctx, "samefile", strings.NewReader("nowriteme"))
	require.NoError(t, err)

	o, err := s.OpenObject(bctx, "samefile")
	require.NoError(t, err)

	assert.Equal(t, "abcdefghijklmnopqrstuvwxyz", readFile(t, o), "overwrite should be false")
}

func readFile(t *testing.T, o io.ReadCloser) string {
	t.Helper()
	gzObj, err := NewGZipReadCloser(o)
	require.NoError(t, err)
	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(gzObj)
	require.NoError(t, err)
	return buf.String()
}

func TestWalkRemote(t *testing.T) {
	t.Skip() // need GS access to test this, do it on your PC
	expected := []string{"00000001", "00000002", "00000003"}

	path := "gs://example/dev"
	s, err := NewStore(fmt.Sprintf("%s/tmp-%012d", path, time.Now().UnixNano()), "jsonl.gz", "gz", false)
	assert.NoError(t, err)
	for _, f := range expected {
		s.WriteObject(bctx, f, strings.NewReader("."))
	}

	seen := []string{}
	s.Walk(bctx, "0000", "", func(f string) error {
		seen = append(seen, f)
		exists, err := s.FileExists(bctx, f)
		assert.NoError(t, err)
		assert.True(t, exists)
		return nil
	})
	assert.EqualValues(t, expected, seen)

}
