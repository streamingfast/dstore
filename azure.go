package dstore

import (
	"context"
	"fmt"
	"go.uber.org/zap"
	"io"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/Azure/azure-storage-blob-go/azblob"
)

type AzureStore struct {
	*commonStore

	baseURL      *url.URL
	containerURL azblob.ContainerURL
}

func NewAzureStore(baseURL *url.URL, extension, compressionType string, overwrite bool) (*AzureStore, error) {
	accountName, containerName, err := decodeAzureScheme(baseURL)
	if err != nil {
		return nil, fmt.Errorf("specify azure account name and container like: az://account.container/path")
	}

	accessKey := os.Getenv("AZURE_STORAGE_KEY")
	if accessKey == "" {
		return nil, fmt.Errorf("specify azure access storate key with env var: AZURE_STORAGE_KEY")
	}

	credential, err := azblob.NewSharedKeyCredential(accountName, accessKey)
	if err != nil {
		return nil, fmt.Errorf("azure authentication failed: %w", err)
	}

	p := azblob.NewPipeline(credential, azblob.PipelineOptions{
		RequestLog: azblob.RequestLogOptions{
			LogWarningIfTryOverThreshold: time.Millisecond * 200,
		},
	})
	u, _ := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net/%s", accountName, containerName))
	containerURL := azblob.NewContainerURL(*u, p)

	return &AzureStore{
		baseURL:      baseURL,
		containerURL: containerURL,
		commonStore: &commonStore{
			compressionType: compressionType,
			extension:       extension,
			overwrite:       overwrite,
		},
	}, nil
}

// context not used here
func (s *AzureStore) Clone(_ context.Context) (Store, error) {
	return NewAzureStore(s.baseURL, s.extension, s.compressionType, s.overwrite)
}

func (s *AzureStore) SubStore(subFolder string) (Store, error) {
	url, err := url.Parse(s.baseURL.String())
	if err != nil {
		return nil, fmt.Errorf("azure store parsing base url: %w", err)
	}
	url.Path = path.Join(url.Path, subFolder)

	return &AzureStore{
		baseURL:      url,
		containerURL: s.containerURL,
		commonStore:  s.commonStore,
	}, nil
}

func (s *AzureStore) CopyObject(ctx context.Context, src, dest string) error {
	// TODO optimize this
	reader, err := s.OpenObject(ctx, src)
	if err != nil {
		return err
	}
	defer reader.Close()

	return s.WriteObject(ctx, dest, reader)
}

func (s *AzureStore) BaseURL() *url.URL {
	return s.baseURL
}

func (a *AzureStore) ObjectPath(name string) string {
	return path.Join(strings.TrimLeft(a.baseURL.Path, "/"), a.pathWithExt(name))
}

func (a *AzureStore) ObjectURL(name string) string {
	return fmt.Sprintf("%s/%s", strings.TrimRight(a.baseURL.String(), "/"), strings.TrimLeft(a.pathWithExt(name), "/"))
}

func (a *AzureStore) FileExists(ctx context.Context, base string) (bool, error) {
	path := a.ObjectPath(base)

	blobURL := a.containerURL.NewBlockBlobURL(path)
	_, err := blobURL.GetProperties(ctx, azblob.BlobAccessConditions{}, azblob.ClientProvidedKeyOptions{})
	if err != nil {

		// azure returns a 404 error when blob NOT FOUND
		if serr, ok := err.(azblob.StorageError); ok { // This error is a Service-specific
			switch serr.ServiceCode() { // Compare serviceCode to ServiceCodeXxx constants
			case azblob.ServiceCodeBlobNotFound:
				return false, nil
			}
		}
		return false, err
	}

	return true, nil
}

func (a *AzureStore) ObjectAttributes(ctx context.Context, base string) (*ObjectAttributes, error) {
	path := a.ObjectPath(base)

	blobURL := a.containerURL.NewBlockBlobURL(path)
	props, err := blobURL.GetProperties(ctx, azblob.BlobAccessConditions{}, azblob.ClientProvidedKeyOptions{})
	if err != nil {
		return nil, err
	}

	return &ObjectAttributes{
		LastModified: props.LastModified(),
		Size:         props.ContentLength(),
	}, nil
}

func (a *AzureStore) WriteObject(ctx context.Context, base string, f io.Reader) (err error) {
	ctx = withFile(ctx, base)
	ctx = withStore(ctx, "azure")
	ctx = withLogger(ctx, zlog, tracer)

	path := a.ObjectPath(base)

	exists, err := a.FileExists(ctx, base)
	if err != nil {
		return err
	}

	if !a.overwrite && exists {
		// We silently ignore when we ask not to overwrite
		return nil
	}

	pipeRead, pipeWrite := io.Pipe()
	writeDone := make(chan error, 1)
	ctx, cancel := context.WithCancel(ctx)

	go func(ctx context.Context) {
		defer pipeWrite.Close()

		err := a.compressedCopy(ctx, pipeWrite, f)
		if err != nil {
			cancel()
		}
		writeDone <- err
	}(ctx)

	bufferSize := 1 * 1024 * 1024 // Size of the rotating buffers that are used when uploading
	maxBuffers := 3               // Number of rotating buffers that are used when uploading
	blobURL := a.containerURL.NewBlockBlobURL(path)
	blobHeader := azblob.BlobHTTPHeaders{
		ContentType:  "application/octet-stream",
		CacheControl: "public, max-age=86400",
	}

	_, err = azblob.UploadStreamToBlockBlob(ctx, pipeRead, blobURL, azblob.UploadStreamToBlockBlobOptions{BlobHTTPHeaders: blobHeader,
		BufferSize:       bufferSize,
		MaxBuffers:       maxBuffers,
		Metadata:         azblob.Metadata{},
		AccessConditions: azblob.BlobAccessConditions{},
	})
	if err != nil {
		return err
	}

	return nil
}

func (a *AzureStore) OpenObject(ctx context.Context, name string) (out io.ReadCloser, err error) {
	ctx = withStore(ctx, "azure")
	ctx = withLogger(ctx, zlog, tracer)

	path := a.ObjectPath(name)
	ctx = withFile(ctx, path)

	if tracer.Enabled() {
		zlog.Debug("opening dstore file", zap.String("path", a.pathWithExt(path)))
	}

	blobURL := a.containerURL.NewBlockBlobURL(path)

	get, err := blobURL.Download(ctx, 0, 0, azblob.BlobAccessConditions{}, false, azblob.ClientProvidedKeyOptions{})
	if err != nil {
		if err.Error() == string(azblob.ServiceCodeBlobNotFound) {
			return nil, ErrNotFound
		}

		return nil, err
	}

	reader := get.Body(azblob.RetryReaderOptions{})

	out, err = a.uncompressedReader(ctx, reader)
	if tracer.Enabled() {
		out = wrapReadCloser(out, func() {
			zlog.Debug("closing dstore file", zap.String("path", a.pathWithExt(path)))
		})
	}
	return
}

func (a *AzureStore) PushLocalFile(ctx context.Context, localFile, toBaseName string) error {
	remove, err := pushLocalFile(ctx, a, localFile, toBaseName)
	if err != nil {
		return err
	}
	return remove()
}

func (s *AzureStore) WalkFrom(ctx context.Context, prefix, startingPoint string, f func(filename string) (err error)) error {
	return commonWalkFrom(s, ctx, prefix, startingPoint, f)
}

func (a *AzureStore) Walk(ctx context.Context, prefix string, f func(filename string) (err error)) error {

	p := strings.TrimLeft(a.baseURL.Path, "/") + "/"
	if prefix != "" {
		p = filepath.Join(p, prefix)
		// join cleans the string and will remove the trailing / in the prefix is present.
		// adding it back to prevent false positive matches
		if prefix[len(prefix)-1:] == "/" {
			p = p + "/"
		}
	}

	for marker := (azblob.Marker{}); marker.NotDone(); { // The parens around Marker{} are required to avoid compiler error.
		// Get a result segment starting with the blob indicated by the current Marker.
		listBlob, err := a.containerURL.ListBlobsFlatSegment(ctx, marker, azblob.ListBlobsSegmentOptions{
			Prefix: p,
		})
		if err != nil {
			return err
		}

		// IMPORTANT: ListBlobs returns the start of the next segment; you MUST use this to get
		// the next segment (after processing the current result segment).
		marker = listBlob.NextMarker

		// Process the blobs returned in this result segment (if the segment is empty, the loop body won't execute)
		for _, blobInfo := range listBlob.Segment.BlobItems {
			if err := f(a.toBaseName(blobInfo.Name)); err != nil {
				if err == StopIteration {
					return nil
				}
			}
		}
	}
	return nil
}

func (a *AzureStore) ListFiles(ctx context.Context, prefix string, max int) ([]string, error) {
	return listFiles(ctx, a, prefix, max)
}

func (a AzureStore) DeleteObject(ctx context.Context, base string) error {
	path := a.ObjectPath(base)

	blobURL := a.containerURL.NewBlockBlobURL(path)

	_, err := blobURL.Delete(ctx, azblob.DeleteSnapshotsOptionNone, azblob.BlobAccessConditions{})

	return err
}

func decodeAzureScheme(baseURL *url.URL) (accountName string, container string, err error) {
	chunks := strings.Split(baseURL.Host, ".")
	if len(chunks) != 2 {
		err = fmt.Errorf("invalid schema expected cannot decode account name and container")
		return
	}
	accountName = chunks[0]
	container = chunks[1]

	if accountName == "" {
		err = fmt.Errorf("invalid schema missing account name")
		return
	}

	if container == "" {
		err = fmt.Errorf("invalid schema missing container")
		return
	}
	return
}

func (s *AzureStore) toBaseName(filename string) string {
	return strings.TrimPrefix(strings.TrimSuffix(filename, s.pathWithExt("")), strings.TrimLeft(s.baseURL.Path, "/")+"/")
}
