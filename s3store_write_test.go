package dstore

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
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

// contextIgnoringUploadClient accepts every upload call even after its context is canceled, so
// a canceled context alone never stops an upload from being committed. It records the calls
// that commit an object.
type contextIgnoringUploadClient struct {
	lock      sync.Mutex
	committed []string
}

func (c *contextIgnoringUploadClient) commit(call string) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.committed = append(c.committed, call)
}

func (c *contextIgnoringUploadClient) PutObject(_ context.Context, input *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	if _, err := io.Copy(io.Discard, input.Body); err != nil {
		return nil, err
	}
	c.commit("PutObject")
	return &s3.PutObjectOutput{}, nil
}

func (c *contextIgnoringUploadClient) UploadPart(_ context.Context, input *s3.UploadPartInput, _ ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
	if _, err := io.Copy(io.Discard, input.Body); err != nil {
		return nil, err
	}
	return &s3.UploadPartOutput{ETag: aws.String(`"part"`)}, nil
}

func (c *contextIgnoringUploadClient) CreateMultipartUpload(context.Context, *s3.CreateMultipartUploadInput, ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error) {
	return &s3.CreateMultipartUploadOutput{UploadId: aws.String("upload-id")}, nil
}

func (c *contextIgnoringUploadClient) CompleteMultipartUpload(context.Context, *s3.CompleteMultipartUploadInput, ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
	c.commit("CompleteMultipartUpload")
	return &s3.CompleteMultipartUploadOutput{}, nil
}

func (c *contextIgnoringUploadClient) AbortMultipartUpload(context.Context, *s3.AbortMultipartUploadInput, ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error) {
	return &s3.AbortMultipartUploadOutput{}, nil
}

// TestS3StoreWriteObject_SourceFailsMidStream checks that a source failing partway leaves no
// object behind and returns its error, for an upload that fits in one PUT and for a multipart
// one. The upload client ignores context cancellation, so the only thing that can stop the
// commit is the source's error reaching the uploader through the pipe.
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
			store, _ := newMockedS3CopyStore(t, 0, "")
			client := &contextIgnoringUploadClient{}
			store.uploader = manager.NewUploader(client)
			sourceErr := errors.New("source failed")

			content := strings.NewReader(strings.Repeat("a", test.size))
			err := store.WriteObject(context.Background(), "file", &failingReader{content: content, err: sourceErr})
			require.ErrorIs(t, err, sourceErr)
			assert.Empty(t, client.committed, "the partial content was committed")
		})
	}
}
