package dstore

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// s3MockPage is one ListObjectsV2 response the mocked bucket answers with, in order.
type s3MockPage struct {
	commonPrefixes []string
	keys           []string
	nextToken      string
}

// s3MockTransport answers every ListObjectsV2 call with the next canned page, recording the
// query of each request so a test can assert on the bounds the store asked for.
type s3MockTransport struct {
	pages    []s3MockPage
	requests []url.Values
}

func (t *s3MockTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	t.requests = append(t.requests, request.URL.Query())

	if len(t.pages) == 0 {
		return nil, fmt.Errorf("unexpected request %q, no page left to serve", request.URL)
	}

	page := t.pages[0]
	t.pages = t.pages[1:]

	body := &strings.Builder{}
	body.WriteString(`<?xml version="1.0" encoding="UTF-8"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>bucket</Name>`)
	for _, commonPrefix := range page.commonPrefixes {
		fmt.Fprintf(body, "<CommonPrefixes><Prefix>%s</Prefix></CommonPrefixes>", commonPrefix)
	}
	for _, key := range page.keys {
		fmt.Fprintf(body, "<Contents><Key>%s</Key><Size>1</Size></Contents>", key)
	}
	if page.nextToken != "" {
		fmt.Fprintf(body, "<IsTruncated>true</IsTruncated><NextContinuationToken>%s</NextContinuationToken>", page.nextToken)
	} else {
		body.WriteString("<IsTruncated>false</IsTruncated>")
	}
	body.WriteString("</ListBucketResult>")

	return &http.Response{
		StatusCode: 200,
		Header:     http.Header{"Content-Type": []string{"application/xml"}},
		Body:       io.NopCloser(strings.NewReader(body.String())),
		Request:    request,
	}, nil
}

func newMockedS3Store(t *testing.T, path string, pages ...s3MockPage) (*S3Store, *s3MockTransport) {
	t.Helper()

	transport := &s3MockTransport{pages: pages}
	client := s3.New(s3.Options{
		Region:       "none",
		BaseEndpoint: aws.String("https://mock.example.com"),
		Credentials:  credentials.NewStaticCredentialsProvider("key", "secret", ""),
		HTTPClient:   &http.Client{Transport: transport},
		UsePathStyle: true,
	})

	baseURL, err := url.Parse("s3://mock.example.com/bucket/" + path + "?region=none")
	require.NoError(t, err)

	return &S3Store{
		baseURL:     baseURL,
		bucket:      "bucket",
		path:        path,
		client:      client,
		commonStore: &commonStore{},
	}, transport
}

func TestS3StoreListFoldersFromTo_StartAfter(t *testing.T) {
	tests := []struct {
		name               string
		path               string
		prefix             string
		inclusiveFrom      string
		expectedPrefix     string
		expectedStartAfter string
	}{
		{
			name:           "no lower bound sends no start-after",
			path:           "root",
			prefix:         "chain/",
			expectedPrefix: "root/chain/",
		},
		{
			name:               "one character bound is pushed down whole",
			path:               "root",
			prefix:             "chain/",
			inclusiveFrom:      "chain/b",
			expectedPrefix:     "root/chain/",
			expectedStartAfter: "root/chain/b",
		},
		{
			name:               "bound keeps everything but its trailing slash",
			path:               "root",
			prefix:             "chain/",
			inclusiveFrom:      "chain/bc/",
			expectedPrefix:     "root/chain/",
			expectedStartAfter: "root/chain/bc",
		},
		{
			name:               "bound at the root of a pathless store",
			prefix:             "",
			inclusiveFrom:      "b/",
			expectedStartAfter: "b",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store, transport := newMockedS3Store(t, test.path, s3MockPage{})

			_, err := store.ListFoldersFromTo(context.Background(), test.prefix, test.inclusiveFrom, "", -1)
			require.NoError(t, err)

			require.Len(t, transport.requests, 1)
			query := transport.requests[0]
			assert.Equal(t, test.expectedPrefix, query.Get("prefix"), "prefix not equals")
			assert.Equal(t, "/", query.Get("delimiter"), "delimiter not equals")
			assert.Equal(t, test.expectedStartAfter, query.Get("start-after"), "start-after not equals")
		})
	}
}

func TestS3StoreListFoldersFromTo_StopsAtExclusiveTo(t *testing.T) {
	store, transport := newMockedS3Store(t, "root",
		s3MockPage{commonPrefixes: []string{"root/chain/a/", "root/chain/b/", "root/chain/c/"}, nextToken: "next"},
		s3MockPage{commonPrefixes: []string{"root/chain/d/"}},
	)

	folders, err := store.ListFoldersFromTo(context.Background(), "chain/", "", "chain/c/", -1)
	require.NoError(t, err)

	assert.Equal(t, []string{"chain/a/", "chain/b/"}, folders)
	assert.Len(t, transport.requests, 1, "second page must not be fetched once the upper bound is reached")
}

func TestS3StoreListFoldersFromTo_PaginatesWhenBoundNotReached(t *testing.T) {
	store, transport := newMockedS3Store(t, "root",
		s3MockPage{commonPrefixes: []string{"root/chain/a/"}, nextToken: "next"},
		s3MockPage{commonPrefixes: []string{"root/chain/b/", "root/chain/z/"}},
	)

	folders, err := store.ListFoldersFromTo(context.Background(), "chain/", "", "chain/c/", -1)
	require.NoError(t, err)

	assert.Equal(t, []string{"chain/a/", "chain/b/"}, folders)
	assert.Len(t, transport.requests, 2)
	assert.Equal(t, "next", transport.requests[1].Get("continuation-token"))
}

func TestS3StoreWalkFromTo_BoundsAndEarlyStop(t *testing.T) {
	store, transport := newMockedS3Store(t, "root",
		s3MockPage{keys: []string{"root/chain/b", "root/chain/c", "root/chain/x"}, nextToken: "next"},
		s3MockPage{keys: []string{"root/chain/z"}},
	)

	var seen []string
	err := store.WalkFromTo(context.Background(), "chain/", "chain/b", "chain/x", func(filename string) error {
		seen = append(seen, filename)
		return nil
	})
	require.NoError(t, err)

	assert.Equal(t, []string{"chain/b", "chain/c"}, seen)
	require.Len(t, transport.requests, 1, "second page must not be fetched once the upper bound is reached")
	assert.Equal(t, "root/chain/", transport.requests[0].Get("prefix"))
	assert.Equal(t, "root/chain/a", transport.requests[0].Get("start-after"))
}

func TestKeyBefore(t *testing.T) {
	tests := []struct {
		target   string
		expected string
	}{
		{"", ""},
		{"root/chain/helloworld.html", "root/chain/helloworld.htmk"},
		{"root/chain/0000000100", "root/chain/000000010/"},
		{"root/chain/b", "root/chain/a"},
		{"root/chain/a", "root/chain/`"},
		{"root/chain/", "root/chain."},
		{"root/chain/caf\u00e9", "root/chain/caf\u00e8"},
		{"root/chain/\ue000", "root/chain/\ud7ff"},
		{"root/chain/x\x00", "root/chain/x"},
		{"root/chain/x\xff", "root/chain/x"},
	}

	for _, test := range tests {
		t.Run(test.target, func(t *testing.T) {
			before := keyBefore(test.target)
			assert.Equal(t, test.expected, before)
			if test.target != "" {
				assert.Less(t, before, test.target, "must sort before the target")
			}
		})
	}
}
