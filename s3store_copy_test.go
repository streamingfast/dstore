package dstore

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// s3CopyMockRequest is one request the mocked bucket received.
type s3CopyMockRequest struct {
	method string
	path   string
	query  url.Values
	header http.Header
	body   string
}

// s3CopyMockTransport answers the calls a server-side copy makes — the head of the source,
// then either the single copy or the multipart sequence — and records each of them so a test
// can assert on the source, the ranges and the storage class the store asked for. Setting
// copyErrorCode makes every copy call fail instead, which is what drives the store onto its
// streaming fallback.
type s3CopyMockTransport struct {
	sourceContent   string
	sourceSize      int64
	copyErrorStatus int
	copyErrorCode   string

	requests []s3CopyMockRequest
}

func (t *s3CopyMockTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	query := request.URL.Query()
	recorded := s3CopyMockRequest{
		method: request.Method,
		path:   request.URL.Path,
		query:  query,
		header: request.Header.Clone(),
	}
	if request.Body != nil {
		body, err := io.ReadAll(request.Body)
		if err != nil {
			return nil, err
		}
		recorded.body = string(body)
	}
	t.requests = append(t.requests, recorded)

	respond := func(status int, contentType, body string) (*http.Response, error) {
		return &http.Response{
			StatusCode:    status,
			Header:        http.Header{"Content-Type": []string{contentType}, "ETag": []string{`"etag"`}},
			ContentLength: int64(len(body)),
			Body:          io.NopCloser(strings.NewReader(body)),
			Request:       request,
		}, nil
	}
	xml := func(body string) (*http.Response, error) {
		return respond(200, "application/xml", body)
	}
	isCopy := recorded.header.Get("x-amz-copy-source") != ""
	if isCopy && t.copyErrorCode != "" {
		return respond(t.copyErrorStatus, "application/xml",
			`<Error><Code>`+t.copyErrorCode+`</Code><Message>mocked</Message></Error>`)
	}

	switch {
	case request.Method == http.MethodHead:
		size := t.sourceSize
		if t.sourceContent != "" {
			size = int64(len(t.sourceContent))
		}
		return &http.Response{
			StatusCode:    200,
			Header:        http.Header{"Content-Length": []string{strconv.FormatInt(size, 10)}},
			ContentLength: size,
			Body:          http.NoBody,
			Request:       request,
		}, nil

	case request.Method == http.MethodGet:
		return respond(200, "application/octet-stream", t.sourceContent)

	case request.Method == http.MethodDelete:
		return respond(204, "application/xml", "")

	case request.Method == http.MethodPost && query.Has("uploads"):
		return xml(`<InitiateMultipartUploadResult><Bucket>bucket</Bucket><UploadId>upload-id</UploadId></InitiateMultipartUploadResult>`)

	case request.Method == http.MethodPost && query.Has("uploadId"):
		return xml(`<CompleteMultipartUploadResult><ETag>"completed"</ETag></CompleteMultipartUploadResult>`)

	case request.Method == http.MethodPut && query.Has("partNumber"):
		return xml(`<CopyPartResult><ETag>"part-` + query.Get("partNumber") + `"</ETag></CopyPartResult>`)

	case request.Method == http.MethodPut && isCopy:
		return xml(`<CopyObjectResult><ETag>"copied"</ETag></CopyObjectResult>`)

	case request.Method == http.MethodPut:
		return respond(200, "application/xml", "")
	}

	return nil, fmt.Errorf("unexpected request %s %q", request.Method, request.URL)
}

func newMockedS3CopyStore(t *testing.T, sourceSize int64, storageClass string) (*S3Store, *s3CopyMockTransport) {
	t.Helper()

	transport := &s3CopyMockTransport{sourceSize: sourceSize}
	client := s3.New(s3.Options{
		Region:       "none",
		BaseEndpoint: aws.String("https://mock.example.com"),
		Credentials:  credentials.NewStaticCredentialsProvider("key", "secret", ""),
		HTTPClient:   &http.Client{Transport: transport},
		UsePathStyle: true,
	})

	baseURL, err := url.Parse("s3://mock.example.com/bucket/root?region=none")
	require.NoError(t, err)

	return &S3Store{
		baseURL:      baseURL,
		bucket:       "bucket",
		path:         "root",
		storageClass: storageClass,
		client:       client,
		uploader:     manager.NewUploader(client),
		commonStore:  &commonStore{overwrite: true},
	}, transport
}

func TestS3StoreCopyObject_SingleCall(t *testing.T) {
	store, transport := newMockedS3CopyStore(t, s3MaxSingleCopySize, "GLACIER")

	require.NoError(t, store.CopyObject(context.Background(), "from", "to"))

	require.Len(t, transport.requests, 2)

	head := transport.requests[0]
	assert.Equal(t, http.MethodHead, head.method)
	assert.Equal(t, "/bucket/root/from", head.path)

	copied := transport.requests[1]
	assert.Equal(t, http.MethodPut, copied.method)
	assert.Equal(t, "/bucket/root/to", copied.path)
	assert.Equal(t, "bucket/root/from", copied.header.Get("x-amz-copy-source"))
	assert.Equal(t, "GLACIER", copied.header.Get("x-amz-storage-class"))
	assert.Empty(t, copied.header.Get("x-amz-metadata-directive"), "the directive defaults to COPY so the metadata of the source carries over")
}

func TestS3StoreCopyObject_Multipart(t *testing.T) {
	restoreThreshold, restorePartSize := s3MaxSingleCopySize, s3CopyPartSize
	defer func() { s3MaxSingleCopySize, s3CopyPartSize = restoreThreshold, restorePartSize }()
	s3CopyPartSize = 2 * 1024 * 1024 * 1024
	s3MaxSingleCopySize = 2 * s3CopyPartSize

	// Two full parts and a last one holding the single byte left over.
	store, transport := newMockedS3CopyStore(t, s3MaxSingleCopySize+1, "GLACIER")

	require.NoError(t, store.CopyObject(context.Background(), "from", "to"))

	require.Len(t, transport.requests, 6)

	create := transport.requests[1]
	assert.Equal(t, http.MethodPost, create.method)
	assert.Equal(t, "/bucket/root/to", create.path)
	assert.True(t, create.query.Has("uploads"))
	assert.Equal(t, "GLACIER", create.header.Get("x-amz-storage-class"))

	expectedRanges := []string{"bytes=0-2147483647", "bytes=2147483648-4294967295", "bytes=4294967296-4294967296"}
	for i, expectedRange := range expectedRanges {
		part := transport.requests[2+i]
		assert.Equal(t, http.MethodPut, part.method)
		assert.Equal(t, "/bucket/root/to", part.path)
		assert.Equal(t, strconv.Itoa(i+1), part.query.Get("partNumber"))
		assert.Equal(t, "upload-id", part.query.Get("uploadId"))
		assert.Equal(t, "bucket/root/from", part.header.Get("x-amz-copy-source"))
		assert.Equal(t, expectedRange, part.header.Get("x-amz-copy-source-range"))
	}

	complete := transport.requests[5]
	assert.Equal(t, http.MethodPost, complete.method)
	assert.Equal(t, "upload-id", complete.query.Get("uploadId"))
}

func TestS3StoreCopyObject_CopySourceEscaping(t *testing.T) {
	store, transport := newMockedS3CopyStore(t, 1024, "")

	require.NoError(t, store.CopyObject(context.Background(), "a b/c+d", "to"))

	require.Len(t, transport.requests, 2)
	assert.Equal(t, "bucket/root/a%20b/c%2Bd", transport.requests[1].header.Get("x-amz-copy-source"),
		"the slashes separating the segments stay literal, everything else is percent-encoded")
}

func TestS3StoreCopyObject_FallsBackWhenCopyIsNotImplemented(t *testing.T) {
	store, transport := newMockedS3CopyStore(t, 0, "")
	transport.sourceContent = "the object content"
	transport.copyErrorStatus, transport.copyErrorCode = 501, "NotImplemented"

	require.NoError(t, store.CopyObject(context.Background(), "from", "to"))

	var uploads int
	for _, request := range transport.requests {
		if request.method == http.MethodPut && request.header.Get("x-amz-copy-source") == "" {
			uploads++
			assert.Equal(t, "/bucket/root/to", request.path)
			assert.Equal(t, transport.sourceContent, request.body)
		}
	}
	assert.Equal(t, 1, uploads, "the object should have been written through the client")

	var reads int
	for _, request := range transport.requests {
		if request.method == http.MethodGet {
			reads++
			assert.Equal(t, "/bucket/root/from", request.path)
		}
	}
	assert.Equal(t, 1, reads, "the object should have been read through the client")
}

func TestS3StoreCopyObject_FallsBackWhenPartCopyIsNotImplemented(t *testing.T) {
	restoreThreshold, restorePartSize := s3MaxSingleCopySize, s3CopyPartSize
	defer func() { s3MaxSingleCopySize, s3CopyPartSize = restoreThreshold, restorePartSize }()
	s3MaxSingleCopySize, s3CopyPartSize = 4, 4

	store, transport := newMockedS3CopyStore(t, 0, "")
	transport.sourceContent = "the object content"
	transport.copyErrorStatus, transport.copyErrorCode = 501, "NotImplemented"

	require.NoError(t, store.CopyObject(context.Background(), "from", "to"))

	var aborts, uploads int
	for _, request := range transport.requests {
		switch {
		case request.method == http.MethodDelete && request.query.Has("uploadId"):
			aborts++
		case request.method == http.MethodPut && request.header.Get("x-amz-copy-source") == "":
			uploads++
			assert.Equal(t, transport.sourceContent, request.body)
		}
	}
	assert.Equal(t, 1, aborts, "the multipart upload should have been aborted before falling back")
	assert.Equal(t, 1, uploads, "the object should have been written through the client")
}

func TestS3StoreCopyObject_ReturnsOtherErrors(t *testing.T) {
	store, transport := newMockedS3CopyStore(t, 1024, "")
	transport.copyErrorStatus, transport.copyErrorCode = 403, "AccessDenied"

	err := store.CopyObject(context.Background(), "from", "to")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "AccessDenied")

	for _, request := range transport.requests {
		if request.method == http.MethodPut {
			require.NotEmpty(t, request.header.Get("x-amz-copy-source"), "no object should have been uploaded through the client")
		}
		assert.NotEqual(t, http.MethodGet, request.method, "no object should have been read through the client")
	}
}

// You need a running S3-compatible backend for this one, see docker-compose.yml:
//
//	docker compose up -d minio
//	STORETESTS_S3_MINIO_STORE_URL="s3://localhost:9000/store-tests?region=none&insecure=true&access_key_id=minioadmin&secret_access_key=minioadmin"
func TestS3StoreCopyObject_Minio(t *testing.T) {
	baseURL := os.Getenv("STORETESTS_S3_MINIO_STORE_URL")
	if baseURL == "" {
		t.Skip("You must provide a valid Minio S3 URL via STORETESTS_S3_MINIO_STORE_URL environment variable to execute those tests")
	}

	// Big enough that a 5 MiB part size, the floor S3 puts on every part but the last, still
	// splits it in two.
	content := strings.Repeat("the quick brown fox ", 6*1024*1024/20+13)

	tests := []struct {
		name     string
		partSize int64
	}{
		{name: "single call", partSize: 0},
		{name: "multipart", partSize: 5 * 1024 * 1024},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.partSize != 0 {
				restoreThreshold, restorePartSize := s3MaxSingleCopySize, s3CopyPartSize
				defer func() { s3MaxSingleCopySize, s3CopyPartSize = restoreThreshold, restorePartSize }()

				s3MaxSingleCopySize, s3CopyPartSize = int64(len(content))-1, test.partSize
			}

			store, cleanup := newMinioTestStore(t, baseURL)
			defer cleanup()

			ctx := context.Background()
			require.NoError(t, store.WriteObject(ctx, "source", strings.NewReader(content)))
			require.NoError(t, store.CopyObject(ctx, "source", "destination"))

			reader, err := store.OpenObject(ctx, "destination")
			require.NoError(t, err)
			defer reader.Close()

			copied, err := io.ReadAll(reader)
			require.NoError(t, err)
			assert.Equal(t, content, string(copied))
		})
	}

	t.Run("missing source", func(t *testing.T) {
		store, cleanup := newMinioTestStore(t, baseURL)
		defer cleanup()

		err := store.CopyObject(context.Background(), "nowhere", "destination")
		require.ErrorIs(t, err, ErrNotFound)
	})
}

// newMinioTestStore points a store at a path of its own within the test bucket and hands back
// the cleanup that empties it.
func newMinioTestStore(t *testing.T, baseURL string) (*S3Store, func()) {
	t.Helper()

	storeURL, err := url.Parse(baseURL)
	require.NoError(t, err)
	storeURL.Path = strings.TrimSuffix(storeURL.Path, "/") + fmt.Sprintf("/dstore-s3copy-tests-%08x", rand.Int63())

	store, err := NewS3Store(storeURL, "", "", true)
	require.NoError(t, err)

	return store, func() {
		ctx := context.Background()
		names, err := store.ListFiles(ctx, "", -1)
		require.NoError(t, err)
		for _, name := range names {
			require.NoError(t, store.DeleteObject(ctx, name))
		}
	}
}
