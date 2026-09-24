// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ategcs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"cloud.google.com/go/storage/experimental"
	"github.com/googleapis/gax-go/v2"
	"google.golang.org/api/option"
)

const rapidPartsMagic = "ATE_RAPID_PARTS:"

func isRapidBucket(bucket string) bool {
	if strings.Contains(strings.ToLower(bucket), "rapid") {
		return true
	}
	v := strings.ToLower(strings.TrimSpace(os.Getenv("ATE_GCS_RAPID")))
	return v == "true" || v == "1"
}

type gcsClient struct {
	client *storage.Client
	// opts are how client was built, so the pool below is built the same way.
	// A pooled client that authenticates differently from the one that opened
	// an object fails partway through reading it.
	opts []option.ClientOption
	// pool holds extra clients so concurrent parts get their own connections;
	// built on first use by uploadClient.
	poolOnce sync.Once
	pool     []*storage.Client

	grpcOnce   sync.Once
	grpcClient *storage.Client
	grpcPool   []*storage.Client
}

// NewGCSClient returns a GCS-backed ObjectStorage. It builds its own
// storage.Client from opts (and more from the same opts for the part-upload
// pool, see uploadClient) rather than accepting one, because it installs a
// RetryAlways policy (see setRetry) that is only safe for this package's own
// operations and must not leak onto a client shared with other code.
func NewGCSClient(ctx context.Context, opts ...option.ClientOption) (ObjectStorage, error) {
	client, err := storage.NewClient(ctx, opts...)
	if err != nil {
		return nil, err
	}
	setRetry(client)
	return &gcsClient{client: client, opts: opts}, nil
}

func (g *gcsClient) initGRPC(ctx context.Context) {
	g.grpcOnce.Do(func() {
		grpcOpts := append([]option.ClientOption{}, g.opts...)
		grpcOpts = append(grpcOpts, experimental.WithZonalBucketAPIs())
		c, err := storage.NewGRPCClient(context.WithoutCancel(ctx), grpcOpts...)
		if err != nil {
			slog.WarnContext(ctx, "Failed to create gRPC client for GCS Rapid bucket; falling back to default client", slog.Any("err", err))
			return
		}
		setRetry(c)
		g.grpcClient = c
		for range uploadPoolSize {
			pc, err := storage.NewGRPCClient(context.WithoutCancel(ctx), grpcOpts...)
			if err != nil {
				slog.WarnContext(ctx, "Falling back to primary gRPC client for part pool", slog.Any("err", err))
				return
			}
			setRetry(pc)
			g.grpcPool = append(g.grpcPool, pc)
		}
	})
}

func (g *gcsClient) clientForBucket(ctx context.Context, bucket string) *storage.Client {
	if isRapidBucket(bucket) {
		g.initGRPC(ctx)
		if g.grpcClient != nil {
			return g.grpcClient
		}
	}
	return g.client
}

func (g *gcsClient) pooledClientForBucket(ctx context.Context, bucket string, i int) *storage.Client {
	if isRapidBucket(bucket) {
		g.initGRPC(ctx)
		if len(g.grpcPool) > 0 {
			return g.grpcPool[i%len(g.grpcPool)]
		}
		if g.grpcClient != nil {
			return g.grpcClient
		}
	}
	return g.uploadClient(ctx, i)
}

// setRetry makes every operation on c retry transient errors (408, 429, 5xx)
// with backoff. The client's default RetryIdempotent policy never retries an
// object write without a precondition, so a single 429 — routine while GCS
// scales a cold bucket's key ranges up to a suspend burst — failed the whole
// snapshot. RetryAlways is safe for this client because every write targets a
// unique name (snapshot UUID directories, runID-suffixed part names) or is
// idempotent (compose and copy from fixed sources, delete).
func setRetry(c *storage.Client) {
	c.SetRetry(
		storage.WithPolicy(storage.RetryAlways),
		// Suspend is latency-sensitive, so bound the worst case: at most 5
		// attempts, under 5s of backoff sleep in total (250ms+500ms+1s+2s).
		storage.WithBackoff(gax.Backoff{Initial: 250 * time.Millisecond, Max: 2 * time.Second, Multiplier: 2}),
		storage.WithMaxAttempts(5),
	)
}

// supportsStreamingPut is the streamingPutter marker: the GCS client's PutObject
// accepts a non-seekable streaming body without buffering (it copies the reader
// straight into a storage.Writer — no Content-Length / signing requirement), so
// callers can pipe compression directly into the upload (overlap) instead of
// staging a seekable temp file. (S3's PutObject needs a seekable body, so s3Client
// does NOT implement this — see objects.go sendZstd.) Never called: its presence is
// the signal.
func (g *gcsClient) supportsStreamingPut() {}

// uploadChunkSize is how much of a streamed object the GCS client buffers before
// starting a request. Each chunk costs a round trip, so a snapshot that fits in one
// chunk pays only one: measured on a GKE worker node, a 24 MiB object took 425-530ms
// at the 16 MiB default versus 258-314ms at 64 MiB. The buffer is capped by the object
// size, so small objects still cost only their own bytes.
const uploadChunkSize = 64 << 20

// PutObject writes reader to the object, in a single request below
// uploadCompositeMin and as parallel parts above it. The size is not known up front,
// so this reads that many bytes to find out which case it is, then hands them on.
func (g *gcsClient) PutObject(ctx context.Context, bucket, object string, reader io.Reader) error {
	if isRapidBucket(bucket) {
		// Zonal Rapid buckets do not support ComposeObject; stream directly via BidiWriteObject
		// with FinalizeOnClose enabled.
		return g.putSingle(ctx, bucket, object, reader)
	}
	head := make([]byte, uploadCompositeMin)
	n, err := io.ReadFull(reader, head)
	switch {
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		// The whole object is in hand and fits in one request.
		return g.putSingle(ctx, bucket, object, bytes.NewReader(head[:n]))
	case err != nil:
		return fmt.Errorf("while reading object body: %w", err)
	}
	return g.putComposite(ctx, bucket, object, bytes.NewReader(head[:n]), reader)
}

// putSingle writes the whole body in one resumable or appendable BidiWriteObject request.
func (g *gcsClient) putSingle(ctx context.Context, bucket, object string, reader io.Reader) error {
	wc := g.clientForBucket(ctx, bucket).Bucket(bucket).Object(object).NewWriter(ctx)
	wc.ChunkSize = uploadChunkSize
	if isRapidBucket(bucket) {
		wc.FinalizeOnClose = true
	}
	// io.Copy reports local read errors; wc.Close() reports the actual
	// GCS upload (auth, permissions, transient). Join both so the caller
	// doesn't lose either.
	_, copyErr := io.Copy(wc, reader)
	closeErr := wc.Close()
	if err := errors.Join(copyErr, closeErr); err != nil {
		return fmt.Errorf("while putting GCS object: %w", err)
	}
	return nil
}
