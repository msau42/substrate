// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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
	"strconv"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/agent-substrate/substrate/internal/ateerrors"
	"golang.org/x/sync/errgroup"
)

// GetObject streams the object, fetching it as parallel byte ranges when it spans
// more than one chunk (see rangedget.go). Smaller objects stay a single request.
func (g *gcsClient) GetObject(ctx context.Context, bucket, object string) (io.ReadCloser, error) {
	// The first chunk doubles as the size probe: a range read reports the whole
	// object's size in its attrs, so nothing pays an extra round trip for it.
	head, err := g.clientForBucket(ctx, bucket).Bucket(bucket).Object(object).NewRangeReader(ctx, 0, downloadChunkSize)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) || errors.Is(err, storage.ErrBucketNotExist) {
			return nil, fmt.Errorf("%w: Bucket:%q, Object:%q", ateerrors.ReasonFailedGetExternalObject, bucket, object)
		}
		return nil, err
	}
	size := head.Attrs.Size
	if isRapidBucket(bucket) && size > 0 && size <= 64 {
		raw, rErr := io.ReadAll(head)
		_ = head.Close()
		if rErr != nil {
			return nil, rErr
		}
		s := strings.TrimSpace(string(raw))
		if strings.HasPrefix(s, rapidPartsMagic) {
			nParts, pErr := strconv.Atoi(strings.TrimPrefix(s, rapidPartsMagic))
			if pErr == nil && nParts > 0 {
				return g.newRapidPartsReader(ctx, bucket, object, nParts), nil
			}
		}
		return io.NopCloser(bytes.NewReader(raw)), nil
	}
	if size <= downloadChunkSize {
		return head, nil
	}
	return newRangedReader(ctx, size, head, g.fetchRange(bucket, object)), nil
}

func (g *gcsClient) newRapidPartsReader(ctx context.Context, bucket, object string, nParts int) io.ReadCloser {
	pr, pw := io.Pipe()
	go func() {
		partBytes := make([][]byte, nParts)
		done := make([]chan error, nParts)
		for i := range nParts {
			done[i] = make(chan error, 1)
		}
		grp, gctx := errgroup.WithContext(ctx)
		grp.SetLimit(downloadConcurrency)
		for i := range nParts {
			partName := fmt.Sprintf("%s.part-%04d", object, i)
			grp.Go(func() error {
				rc, err := g.pooledClientForBucket(gctx, bucket, i).Bucket(bucket).Object(partName).NewRangeReader(gctx, 0, -1)
				if err != nil {
					done[i] <- err
					return err
				}
				data, rErr := io.ReadAll(rc)
				_ = rc.Close()
				if rErr != nil {
					done[i] <- rErr
					return rErr
				}
				partBytes[i] = data
				done[i] <- nil
				return nil
			})
		}
		for i := range nParts {
			if err := <-done[i]; err != nil {
				_ = grp.Wait()
				_ = pw.CloseWithError(err)
				return
			}
			if _, wErr := pw.Write(partBytes[i]); wErr != nil {
				_ = grp.Wait()
				_ = pw.CloseWithError(wErr)
				return
			}
			partBytes[i] = nil
		}
		_ = pw.CloseWithError(grp.Wait())
	}()
	return pr
}

// fetchRange reads one range with a pooled client, so concurrent ranges do not all
// multiplex onto the one HTTP/2 connection a single storage.Client holds.
func (g *gcsClient) fetchRange(bucket, object string) fetchRangeFunc {
	return func(ctx context.Context, i int, off, n int64, buf []byte) error {
		rc, err := g.pooledClientForBucket(ctx, bucket, i).Bucket(bucket).Object(object).NewRangeReader(ctx, off, n)
		if err != nil {
			return fmt.Errorf("while opening range %d+%d of %q: %w", off, n, object, err)
		}
		defer rc.Close()
		if _, err := io.ReadFull(rc, buf); err != nil {
			return fmt.Errorf("while reading range %d+%d of %q: %w", off, n, object, err)
		}
		return nil
	}
}
