package dstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/googleapis/gax-go/v2"
	"go.uber.org/zap"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/api/option/internaloption"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
)

//
// Google Storage Store

var warnSilenced = os.Getenv("DSTORE_WARN_SILENCED") == "true"

type GSStore struct {
	baseURL     *url.URL
	client      *storage.Client
	userProject string
	*commonStore
}

func NewGSStore(baseURL *url.URL, extension, compressionType string, overwrite bool, opts ...Option) (*GSStore, error) {
	ctx := context.Background()
	return newGSStoreContext(ctx, baseURL, extension, compressionType, overwrite, opts...)
}

func (s *GSStore) Clone(ctx context.Context, opts ...Option) (Store, error) {
	return newGSStoreContext(ctx, s.baseURL, s.extension, s.compressionType, s.overwrite, opts...)
}

func newGSStoreContext(ctx context.Context, baseURL *url.URL, extension, compressionType string, overwrite bool, opts ...Option) (*GSStore, error) {
	query := baseURL.Query()
	userProject := query.Get("project")

	var client *storage.Client
	var err error
	if query.Get("client_protocol") == "grpc" {
		var grpcOpts []option.ClientOption
		if userProject != "" {
			// DirectPath (automatically enabled on GKE) bypasses GFE and goes directly to the
			// storage backend. That backend does not honour the x-goog-user-project gRPC metadata
			// header for requester-pays billing, so we must disable DirectPath when a userProject
			// is set. We keep gRPC for efficient serialisation/streaming; traffic still goes over
			// gRPC but routes through GFE which correctly enforces requester-pays.
			// See: https://github.com/googleapis/google-cloud-go/blob/main/auth/grpctransport/directpath.go
			// TODO: remove once the upstream library implements the quota-project chained interceptor.
			grpcOpts = append(grpcOpts,
				internaloption.EnableDirectPath(false),
			)
			zlog.Warn("DirectPath GCS optimization DISABLED because it is not yet supported with both 'project=' and 'client_protocol=grpc'", zap.String("base_url", baseURL.String()))
		}
		client, err = storage.NewGRPCClient(ctx, grpcOpts...)
	} else {
		var clientOpts []option.ClientOption
		if os.Getenv("STORAGE_EMULATOR_HOST") != "" {
			// fake-gcs-server (and other emulators) don't handle the XML API correctly
			// for object reads with percent-encoded slashes; use the JSON API instead.
			clientOpts = append(clientOpts, storage.WithJSONReads())
		}
		client, err = storage.NewClient(ctx, clientOpts...)
	}
	if err != nil {
		return nil, err
	}

	client.SetRetry(storage.WithBackoff(gax.Backoff{}))

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

	return &GSStore{
		baseURL:     baseURL,
		client:      client,
		commonStore: common,
		userProject: userProject,
	}, nil
}

func (s *GSStore) SubStore(subFolder string) (Store, error) {
	url, err := url.Parse(s.baseURL.String())
	if err != nil {
		return nil, fmt.Errorf("gs store parsing base url: %w", err)
	}
	url.Path = path.Join(url.Path, subFolder)

	return &GSStore{
		baseURL:     url,
		client:      s.client,
		commonStore: s.commonStore,
		userProject: s.userProject,
	}, nil
}

func (s *GSStore) bucket() *storage.BucketHandle {
	if s.userProject != "" {
		return s.client.Bucket(s.baseURL.Host).UserProject(s.userProject)
	}
	return s.client.Bucket(s.baseURL.Host)
}

func (s *GSStore) BaseURL() *url.URL {
	return s.baseURL
}

func (s *GSStore) ObjectPath(name string) string {
	return path.Join(strings.TrimLeft(s.baseURL.Path, "/"), s.pathWithExt(name))
}

func (s *GSStore) ObjectURL(name string) string {
	return fmt.Sprintf("%s/%s", strings.TrimRight(s.baseURL.String(), "/"), strings.TrimLeft(s.pathWithExt(name), "/"))
}

func (s *GSStore) toBaseName(filename string) string {
	return strings.TrimPrefix(strings.TrimSuffix(filename, s.pathWithExt("")), strings.TrimLeft(s.baseURL.Path, "/")+"/")
}

func (s *GSStore) CopyObject(ctx context.Context, src, dest string) error {
	srcPath := s.ObjectPath(src)
	srcObj := s.bucket().Object(srcPath)

	destPath := s.ObjectPath(dest)
	_, err := s.bucket().Object(destPath).CopierFrom(srcObj).Run(ctx)
	return err
}

func (s *GSStore) WriteObject(ctx context.Context, base string, f io.Reader, metadataKeyValues ...string) (err error) {
	ctx = withFileName(ctx, base)
	ctx = withStoreType(ctx, "gstore")
	ctx = withLogger(ctx, zlog, tracer)

	path := s.ObjectPath(base)

	object := s.bucket().Object(path)

	if !s.overwrite {
		object = object.If(storage.Conditions{DoesNotExist: true})
	}
	w := object.NewWriter(ctx)
	w.ContentType = "application/octet-stream"
	w.CacheControl = "public, max-age=86400"

	// Parse metadataKeyValues array
	if len(metadataKeyValues)%2 != 0 {
		return fmt.Errorf("metadataKeyValues must have an even number of strings (key-value pairs), got %d", len(metadataKeyValues))
	}

	if len(metadataKeyValues) > 0 {
		metadata := make(map[string]string)
		for i := 0; i < len(metadataKeyValues); i += 2 {
			key := metadataKeyValues[i]
			value := metadataKeyValues[i+1]
			metadata[key] = value
		}
		w.Metadata = metadata
	}

	if err := s.compressedCopy(ctx, w, f); err != nil {
		return err
	}

	if err := w.Close(); err != nil {
		if s.overwrite {
			return err
		}
		silenced := silencePreconditionError(err)
		if warnSilenced {
			zlog.Info("silenced precondition error", zap.Error(err), zap.NamedError("silenced", silenced), zap.String("path", path))
		}
		return silenced
	}

	return nil
}

func silencePreconditionError(err error) error {
	if e, ok := err.(*googleapi.Error); ok {
		if e.Code == http.StatusPreconditionFailed {
			return nil
		}
	}
	if st, ok := grpcstatus.FromError(err); ok {
		if st.Code() == codes.FailedPrecondition {
			return nil
		}
	}
	return err
}

func (s *GSStore) OpenObject(ctx context.Context, name string) (out io.ReadCloser, err error) {
	ctx = withStoreType(ctx, "gstore")
	ctx = withLogger(ctx, zlog, tracer)

	path := s.ObjectPath(name)
	ctx = withFileName(ctx, path)

	if tracer.Enabled() {
		zlog.Debug("opening dstore file", zap.String("path", path))
	}
	reader, err := s.bucket().Object(path).NewReader(ctx)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			return nil, ErrNotFound
		}

		return nil, err
	}

	out, err = s.uncompressedReader(ctx, reader)
	if tracer.Enabled() {
		out = wrapReadCloser(out, func() {
			zlog.Debug("closing dstore file", zap.String("path", path))
		})
	}
	return
}

func (s *GSStore) DeleteObject(ctx context.Context, base string) error {
	path := s.ObjectPath(base)
	err := s.bucket().Object(path).Delete(ctx)
	if errors.Is(err, storage.ErrObjectNotExist) {
		return ErrNotFound
	}
	return err
}

func (s *GSStore) FileExists(ctx context.Context, base string) (bool, error) {
	path := s.ObjectPath(base)

	_, err := s.bucket().Object(path).Attrs(ctx)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			return false, nil
		}

		return false, err
	}
	return true, nil
}

func (s *GSStore) ObjectAttributes(ctx context.Context, base string) (*ObjectAttributes, error) {
	path := s.ObjectPath(base)

	attrs, err := s.bucket().Object(path).Attrs(ctx)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			return nil, ErrNotFound
		}

		return nil, err
	}

	return &ObjectAttributes{
		LastModified: attrs.Updated,
		Size:         attrs.Size,
		Metadata:     attrs.Metadata,
	}, nil
}

func (s *GSStore) SetMetadata(ctx context.Context, base string, metadata map[string]string) error {
	path := s.ObjectPath(base)
	objectHandle := s.bucket().Object(path)
	attrs := storage.ObjectAttrsToUpdate{
		Metadata: metadata,
	}

	_, err := objectHandle.Update(ctx, attrs)
	return err
}

func (s *GSStore) PushLocalFile(ctx context.Context, localFile, toBaseName string) error {
	remove, err := pushLocalFile(ctx, s, localFile, toBaseName)
	if err != nil {
		return err
	}
	return remove()
}

func (s *GSStore) ListFiles(ctx context.Context, prefix string, max int) ([]string, error) {
	return listFiles(ctx, s, prefix, max)
}

func (s *GSStore) Walk(ctx context.Context, prefix string, f func(filename string) (err error)) error {
	return s.WalkFromTo(ctx, prefix, "", "", f)
}

func getGSWalkQuery(prefix, startingPoint, exclusiveEndPoint, baseURLPath string) (*storage.Query, error) {
	q := &storage.Query{}
	q.SetAttrSelection([]string{"Name"}) // only fetch the name, 25% faster
	q.Prefix = strings.TrimLeft(baseURLPath+"/", "/")
	if prefix != "" {
		q.Prefix = filepath.Join(q.Prefix, prefix)
		// join cleans the string and will remove the trailing / in the prefix if present.
		// adding it back to prevent false positive matches
		if prefix[len(prefix)-1:] == "/" {
			q.Prefix = q.Prefix + "/"
		}
	}

	if startingPoint != "" {
		if !strings.HasPrefix(startingPoint, prefix) {
			return nil, fmt.Errorf("starting point %q must start with prefix %q", startingPoint, prefix)
		}

		// "startingPoint" is known to start with "prefix" (checked when entering function), but our the prefix received do
		// not contain the "baseURL" which is required because it contains the "path" of the store. So we remove the
		// "original prefix" from the "startingPoint" and append it to the real "final" prefix instead.
		relativeStartingPoint := strings.TrimPrefix(startingPoint, prefix)

		q.StartOffset = q.Prefix + relativeStartingPoint
	}

	if exclusiveEndPoint != "" {
		if !strings.HasPrefix(exclusiveEndPoint, prefix) {
			return nil, fmt.Errorf("exclusive end point %q must start with prefix %q", exclusiveEndPoint, prefix)
		}
		// same adjustment as above
		q.EndOffset = q.Prefix + strings.TrimPrefix(exclusiveEndPoint, prefix)
	}
	return q, nil

}

func (s *GSStore) WalkFromTo(ctx context.Context, prefix, startingPoint, exclusiveEndPoint string, f func(filename string) (err error)) error {
	q, err := getGSWalkQuery(prefix, startingPoint, exclusiveEndPoint, s.baseURL.Path)
	if err != nil {
		return err
	}

	it := s.bucket().Objects(ctx, q)
	if tracer.Enabled() {
		zlog.Info("walking files from", zap.String("original_prefix", prefix), zap.String("prefix", q.Prefix), zap.String("start_offset", q.StartOffset))
	}

	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}
		if err := f(s.toBaseName(attrs.Name)); err != nil {
			if errors.Is(err, StopIteration) {
				return nil
			}
			return err
		}
	}
	return nil
}

func (s *GSStore) WalkFrom(ctx context.Context, prefix, startingPoint string, f func(filename string) (err error)) error {
	return s.WalkFromTo(ctx, prefix, startingPoint, "", f)
}

// WalkAttributes implements [AttributeWalker]: Google Cloud Storage returns the size and the
// modification time along with every name it lists, so this costs exactly what Walk costs.
func (s *GSStore) WalkAttributes(ctx context.Context, prefix string, f func(entry ObjectEntry) error) error {
	q, err := getGSWalkQuery(prefix, "", "", s.baseURL.Path)
	if err != nil {
		return err
	}
	q.SetAttrSelection([]string{"Name", "Size", "Updated"})

	it := s.bucket().Objects(ctx, q)
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}

		entry := ObjectEntry{
			Name:         s.toBaseName(attrs.Name),
			Size:         attrs.Size,
			LastModified: attrs.Updated,
		}
		if err := f(entry); err != nil {
			if errors.Is(err, StopIteration) {
				return nil
			}
			return err
		}
	}

	return nil
}

// ListFolders implements [FolderLister] with a delimited listing, which Google Cloud Storage
// answers without ever walking the objects nested under the folders it returns.
func (s *GSStore) ListFolders(ctx context.Context, prefix string, max int) ([]string, error) {
	return s.ListFoldersFromTo(ctx, prefix, "", "", max)
}

// ListFoldersFromTo pushes the bounds down as the listing's start and end offsets, so the
// service only walks the slice of the key space that was asked for.
func (s *GSStore) ListFoldersFromTo(ctx context.Context, prefix, inclusiveFrom, exclusiveTo string, max int) ([]string, error) {
	prefix = asFolderPrefix(prefix)
	if err := checkFolderRange(prefix, inclusiveFrom, exclusiveTo); err != nil {
		return nil, err
	}

	q, err := getGSWalkQuery(prefix, inclusiveFrom, exclusiveTo, s.baseURL.Path)
	if err != nil {
		return nil, err
	}
	q.Delimiter = "/"
	q.SetAttrSelection([]string{"Name"})

	folders := newLimitedFolders(max)
	if folders.full() {
		return folders.folders, nil
	}

	it := s.bucket().Objects(ctx, q)
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}

		if attrs.Prefix == "" {
			continue // an object sitting directly in prefix, not a folder
		}
		if folders.full() {
			break
		}
		folders.add(s.toBaseName(attrs.Prefix))
	}

	return folders.folders, nil
}
