package dstore

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"go.uber.org/zap"

	"errors"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
)

type AzureStore struct {
	*commonStore

	baseURL       *url.URL
	client        *azblob.Client
	containerName string
}

func NewAzureStore(baseURL *url.URL, extension, compressionType string, overwrite bool, opts ...Option) (*AzureStore, error) {
	ctx := context.Background()
	return newAzureStoreContext(ctx, baseURL, extension, compressionType, overwrite, opts...)
}

func (s *AzureStore) Clone(ctx context.Context, opts ...Option) (Store, error) {
	return newAzureStoreContext(ctx, s.baseURL, s.extension, s.compressionType, s.overwrite, opts...)
}

func newAzureStoreContext(_ context.Context, baseURL *url.URL, extension, compressionType string, overwrite bool, opts ...Option) (*AzureStore, error) {
	accountName, containerName, err := decodeAzureScheme(baseURL)
	if err != nil {
		return nil, fmt.Errorf("specify azure account name and container like: az://account.container/path")
	}

	serviceURL := fmt.Sprintf("https://%s.blob.core.windows.net/", accountName)

	var client *azblob.Client

	// Authentication priority:
	// 1. If AZURE_STORAGE_KEY is set, use shared key credential
	// 2. Otherwise, use DefaultAzureCredential which supports:
	//    - Managed Identity (for Azure resources)
	//    - Service Principal (via AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, AZURE_TENANT_ID)
	//    - Azure CLI credentials
	//    - Visual Studio Code credentials
	//    - And other authentication methods
	//
	// Try to use shared key credential if AZURE_STORAGE_KEY is provided
	accessKey := os.Getenv("AZURE_STORAGE_KEY")
	if accessKey != "" {
		credential, err := azblob.NewSharedKeyCredential(accountName, accessKey)
		if err != nil {
			return nil, fmt.Errorf("azure shared key authentication failed: %w", err)
		}

		client, err = azblob.NewClientWithSharedKeyCredential(serviceURL, credential, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create azure client with shared key: %w", err)
		}
	} else {
		// Fall back to DefaultAzureCredential (supports managed identity, service principal, etc.)
		credential, err := azidentity.NewDefaultAzureCredential(nil)
		if err != nil {
			return nil, fmt.Errorf("azure default credential failed: %w", err)
		}

		client, err = azblob.NewClient(serviceURL, credential, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create azure client with default credential: %w", err)
		}
	}

	conf := config{}
	for _, opt := range opts {
		opt.apply(&conf)
	}

	common := &commonStore{
		compressionType:           compressionType,
		extension:                 extension,
		overwrite:                 overwrite,
		uncompressedReadCallback:  conf.uncompressedReadCallback,
		compressedReadCallback:    conf.compressedReadCallback,
		uncompressedWriteCallback: conf.uncompressedWriteCallback,
		compressedWriteCallback:   conf.compressedWriteCallback,
	}

	return &AzureStore{
		baseURL:       baseURL,
		client:        client,
		containerName: containerName,
		commonStore:   common,
	}, nil
}

func (s *AzureStore) SubStore(subFolder string) (Store, error) {
	url, err := url.Parse(s.baseURL.String())
	if err != nil {
		return nil, fmt.Errorf("azure store parsing base url: %w", err)
	}
	url.Path = path.Join(url.Path, subFolder)

	return &AzureStore{
		baseURL:       url,
		client:        s.client,
		containerName: s.containerName,
		commonStore:   s.commonStore,
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

func (s *AzureStore) ObjectPath(name string) string {
	return path.Join(strings.TrimLeft(s.baseURL.Path, "/"), s.pathWithExt(name))
}

func (s *AzureStore) ObjectURL(name string) string {
	return fmt.Sprintf("%s/%s", strings.TrimRight(s.baseURL.String(), "/"), strings.TrimLeft(s.pathWithExt(name), "/"))
}

func (s *AzureStore) FileExists(ctx context.Context, base string) (bool, error) {
	blobPath := s.ObjectPath(base)
	blobClient := s.client.ServiceClient().NewContainerClient(s.containerName).NewBlobClient(blobPath)
	_, err := blobClient.GetProperties(ctx, nil)
	if err != nil {
		var respErr *azcore.ResponseError
		if errors.As(err, &respErr) {
			if respErr.StatusCode == 404 {
				return false, nil
			}
		}
		return false, err
	}

	return true, nil
}

func (s *AzureStore) ObjectAttributes(ctx context.Context, base string) (*ObjectAttributes, error) {
	blobPath := s.ObjectPath(base)

	blobClient := s.client.ServiceClient().NewContainerClient(s.containerName).NewBlobClient(blobPath)
	props, err := blobClient.GetProperties(ctx, nil)
	if err != nil {
		return nil, err
	}

	var lastModified time.Time
	if props.LastModified != nil {
		lastModified = *props.LastModified
	}

	var size int64
	if props.ContentLength != nil {
		size = *props.ContentLength
	}

	metadata := make(map[string]string)
	if props.Metadata != nil {
		for k, v := range props.Metadata {
			if v != nil {
				metadata[k] = *v
			}
		}
	}

	return &ObjectAttributes{
		LastModified: lastModified,
		Size:         size,
		Metadata:     metadata,
	}, nil
}

func (s *AzureStore) SetMetadata(ctx context.Context, base string, metadata map[string]string) error {
	blobPath := s.ObjectPath(base)

	// Convert metadata to Azure format
	azureMetadata := make(map[string]*string)
	for k, v := range metadata {
		value := v
		azureMetadata[k] = &value
	}

	// Update blob metadata
	blobClient := s.client.ServiceClient().NewContainerClient(s.containerName).NewBlobClient(blobPath)
	_, err := blobClient.SetMetadata(ctx, azureMetadata, nil)
	return err
}

func (s *AzureStore) WriteObject(ctx context.Context, base string, f io.Reader, metadataKeyValues ...string) (err error) {
	ctx = withFileName(ctx, base)
	ctx = withStoreType(ctx, "azure")
	ctx = withLogger(ctx, zlog, tracer)

	// Parse metadataKeyValues array
	if len(metadataKeyValues)%2 != 0 {
		return fmt.Errorf("metadataKeyValues must have an even number of strings (key-value pairs), got %d", len(metadataKeyValues))
	}

	blobPath := s.ObjectPath(base)

	exists, err := s.FileExists(ctx, base)
	if err != nil {
		return err
	}

	if !s.overwrite && exists {
		// We silently ignore when we ask not to overwrite
		return nil
	}

	pipeRead, pipeWrite := io.Pipe()
	writeDone := make(chan error, 1)
	ctx, cancel := context.WithCancel(ctx)

	go func(ctx context.Context) {
		defer pipeWrite.Close()

		err := s.compressedCopy(ctx, pipeWrite, f)
		if err != nil {
			cancel()
		}
		writeDone <- err
	}(ctx)

	// Prepare upload options
	uploadOptions := &azblob.UploadStreamOptions{
		BlockSize:   1 * 1024 * 1024, // 1MB blocks
		Concurrency: 3,
		HTTPHeaders: &blob.HTTPHeaders{
			BlobContentType:  toPtr("application/octet-stream"),
			BlobCacheControl: toPtr("public, max-age=86400"),
		},
	}

	// Add metadata if provided
	if len(metadataKeyValues) > 0 {
		metadata := make(map[string]*string)
		for i := 0; i < len(metadataKeyValues); i += 2 {
			key := metadataKeyValues[i]
			value := metadataKeyValues[i+1]
			metadata[key] = toPtr(value)
		}
		uploadOptions.Metadata = metadata
	}

	_, err = s.client.UploadStream(ctx, s.containerName, blobPath, pipeRead, uploadOptions)
	if err != nil {
		return err
	}

	return <-writeDone
}

func (s *AzureStore) OpenObject(ctx context.Context, name string) (out io.ReadCloser, err error) {
	ctx = withStoreType(ctx, "azure")
	ctx = withLogger(ctx, zlog, tracer)

	blobPath := s.ObjectPath(name)
	ctx = withFileName(ctx, blobPath)

	if tracer.Enabled() {
		zlog.Debug("opening dstore file", zap.String("path", blobPath))
	}

	response, err := s.client.DownloadStream(ctx, s.containerName, blobPath, nil)
	if err != nil {
		var respErr *azcore.ResponseError
		if errors.As(err, &respErr) {
			if respErr.StatusCode == 404 {
				return nil, ErrNotFound
			}
		}
		return nil, err
	}

	reader := response.Body

	out, err = s.uncompressedReader(ctx, reader)
	if tracer.Enabled() {
		out = wrapReadCloser(out, func() {
			zlog.Debug("closing dstore file", zap.String("path", blobPath))
		})
	}
	return
}

func (s *AzureStore) PushLocalFile(ctx context.Context, localFile, toBaseName string) error {
	remove, err := pushLocalFile(ctx, s, localFile, toBaseName)
	if err != nil {
		return err
	}
	return remove()
}

func (s *AzureStore) WalkFrom(ctx context.Context, prefix, startingPoint string, f func(filename string) (err error)) error {
	return commonWalkFrom(s, ctx, prefix, startingPoint, f)
}

func (s *AzureStore) WalkFromTo(ctx context.Context, prefix, startingPoint, exclusiveEndPoint string, f func(filename string) (err error)) error {
	return commonWalkFromTo(s, ctx, prefix, startingPoint, exclusiveEndPoint, f)
}

func (s *AzureStore) Walk(ctx context.Context, prefix string, f func(filename string) (err error)) error {
	p := strings.TrimLeft(s.baseURL.Path, "/") + "/"
	if prefix != "" {
		p = filepath.Join(p, prefix)
		// join cleans the string and will remove the trailing / if the prefix is present.
		// adding it back to prevent false positive matches
		if prefix[len(prefix)-1:] == "/" {
			p = p + "/"
		}
	}

	listOptions := &azblob.ListBlobsFlatOptions{
		Prefix: &p,
	}

	pager := s.client.NewListBlobsFlatPager(s.containerName, listOptions)

	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return err
		}

		for _, blobItem := range page.Segment.BlobItems {
			if blobItem.Name != nil {
				if err := f(s.toBaseName(*blobItem.Name)); err != nil {
					if err == StopIteration {
						return nil
					}
					return err
				}
			}
		}
	}
	return nil
}

func (s *AzureStore) ListFiles(ctx context.Context, prefix string, max int) ([]string, error) {
	return listFiles(ctx, s, prefix, max)
}

func (s *AzureStore) DeleteObject(ctx context.Context, base string) error {
	blobPath := s.ObjectPath(base)

	_, err := s.client.DeleteBlob(ctx, s.containerName, blobPath, nil)
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

// Helper function to create string pointers
func toPtr(s string) *string {
	return &s
}

// WalkAttributes implements [AttributeWalker]: the blob listing returns the size and the
// modification time along with every name, so this costs exactly what Walk costs.
func (s *AzureStore) WalkAttributes(ctx context.Context, prefix string, f func(entry ObjectEntry) error) error {
	p := s.listingPrefix(prefix)

	pager := s.client.NewListBlobsFlatPager(s.containerName, &azblob.ListBlobsFlatOptions{Prefix: &p})
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return err
		}

		for _, blobItem := range page.Segment.BlobItems {
			if blobItem.Name == nil {
				continue
			}

			entry := ObjectEntry{Name: s.toBaseName(*blobItem.Name)}
			if properties := blobItem.Properties; properties != nil {
				if properties.ContentLength != nil {
					entry.Size = *properties.ContentLength
				}
				if properties.LastModified != nil {
					entry.LastModified = *properties.LastModified
				}
			}

			if err := f(entry); err != nil {
				if errors.Is(err, StopIteration) {
					return nil
				}
				return err
			}
		}
	}

	return nil
}

// ListFolders implements [FolderLister] with a hierarchical listing, which Azure answers with
// blob prefixes without ever walking the blobs nested under them.
func (s *AzureStore) ListFolders(ctx context.Context, prefix string, max int) ([]string, error) {
	p := s.listingPrefix(asFolderPrefix(prefix))

	folders := newLimitedFolders(max)

	containerClient := s.client.ServiceClient().NewContainerClient(s.containerName)
	pager := containerClient.NewListBlobsHierarchyPager("/", &container.ListBlobsHierarchyOptions{Prefix: &p})
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, err
		}

		for _, blobPrefix := range page.Segment.BlobPrefixes {
			if blobPrefix.Name == nil {
				continue
			}

			folder := s.toBaseName(*blobPrefix.Name)
			if folder == "" {
				continue
			}
			if folders.full() {
				return folders.folders, nil
			}
			folders.add(folder)
		}
	}

	return folders.folders, nil
}

// listingPrefix turns a store-relative prefix into the absolute blob prefix the container
// expects, keeping a trailing "/" that filepath.Join would eat.
func (s *AzureStore) listingPrefix(prefix string) string {
	p := strings.TrimLeft(s.baseURL.Path, "/") + "/"
	if prefix == "" {
		return p
	}

	p = path.Join(p, prefix)
	if strings.HasSuffix(prefix, "/") {
		p += "/"
	}
	return p
}
