package dstore

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// failingReader returns its content, then fails instead of reaching the end.
type failingReader struct {
	content io.Reader
	err     error
}

func (r *failingReader) Read(p []byte) (int, error) {
	n, err := r.content.Read(p)
	if err == io.EOF {
		return n, r.err
	}
	return n, err
}

// TestS3StoreWriteObject_SourceFailsMidStream checks that a source failing partway leaves no
// object behind and returns its error, for an upload that fits in one PUT and for a multipart
// one. Without the error on the pipe the uploader takes the close as the end of the content and
// commits what it has read, which only the timing of the cancel can stop.
func TestS3StoreWriteObject_SourceFailsMidStream(t *testing.T) {
	tests := []struct {
		name string
		size int
	}{
		{"single put", 1024},
		{"multipart", int(manager.DefaultUploadPartSize) + 1024},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store, transport := newMockedS3CopyStore(t, 0, "")
			sourceErr := errors.New("source failed")

			content := strings.NewReader(strings.Repeat("a", test.size))
			err := store.WriteObject(context.Background(), "file", &failingReader{content: content, err: sourceErr})
			require.ErrorIs(t, err, sourceErr)

			for _, request := range transport.requests {
				committed := (request.method == http.MethodPut && !request.query.Has("partNumber")) ||
					(request.method == http.MethodPost && request.query.Has("uploadId"))
				assert.False(t, committed, "%s %s committed the partial content", request.method, request.path)
			}
		})
	}
}
