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

package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/agent-substrate/substrate/internal/nodepath"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
)

// ateomWorkloads tells the snapshot tracker whether an actor runs on this node
// by asking the node's ateoms which workloads they host. It finds the ateoms
// as the stats poller does, by their directories, and keeps nothing between
// calls, so a failed start or checkpoint cannot leave its answer wrong.
type ateomWorkloads struct {
	// ateomsDir is the directory whose entries are worker pod UIDs
	// (nodepath.AteomsDir on a real node; a fixture in tests).
	ateomsDir string
	// dial returns a client for one ateom plus the closer that releases its
	// connection, which lives for one call.
	dial func(ctx context.Context, podUID string) (activeStatsClient, io.Closer, error)
}

// newAteomWorkloads returns an ateomWorkloads over the ateoms on this node.
func newAteomWorkloads() *ateomWorkloads {
	return &ateomWorkloads{
		ateomsDir: nodepath.AteomsDir(),
		dial: func(_ context.Context, podUID string) (activeStatsClient, io.Closer, error) {
			conn, closer, err := dialAteomStats(podUID)
			if err != nil {
				return nil, nil, err
			}
			return ateompb.NewAteomClient(conn), closer, nil
		},
	}
}

// Runs reports whether an ateom on this node hosts a workload of the actor,
// one still booting or restoring included. It asks statsSweepConcurrency
// ateoms at a time. An ateom that nothing listens for hosts nothing: it has
// exited, or its worker pod is gone and left the directory behind. Failing to
// ask any other ateom is an error, unless an ateom that answered hosts the
// actor.
func (w *ateomWorkloads) Runs(ctx context.Context, actorUID string) (bool, error) {
	entries, err := os.ReadDir(w.ateomsDir)
	if errors.Is(err, os.ErrNotExist) {
		// No ateom has started on this node.
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("while listing ateoms: %w", err)
	}

	// Once an ateom reports the actor, the others need not answer.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var (
		mu    sync.Mutex
		found bool
		errs  []error
		g     errgroup.Group
	)
	g.SetLimit(statsSweepConcurrency)
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		podUID := e.Name()
		g.Go(func() error {
			hosts, err := w.hosts(ctx, podUID, actorUID)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case hosts:
				found = true
				cancel()
			case err != nil:
				errs = append(errs, fmt.Errorf("ateom of worker pod %s: %w", podUID, err))
			}
			return nil
		})
	}
	_ = g.Wait()
	if found {
		return true, nil
	}
	return false, errors.Join(errs...)
}

// hosts reports whether the ateom of worker pod podUID hosts a workload of the
// actor.
func (w *ateomWorkloads) hosts(ctx context.Context, podUID, actorUID string) (bool, error) {
	// One deadline over the whole call, dial included, as in a stats sweep.
	ctx, cancel := context.WithTimeout(ctx, statsRPCTimeout)
	defer cancel()
	client, closer, err := w.dial(ctx, podUID)
	if err != nil {
		return false, err
	}
	defer closer.Close()
	resp, err := client.GetActiveWorkloadStats(ctx, &ateompb.GetActiveWorkloadStatsRequest{})
	if status.Code(err) == codes.Unavailable {
		// Nothing listens on the ateom's socket.
		return false, nil
	}
	if err != nil {
		return false, err
	}
	for _, sample := range resp.GetSamples() {
		if sample.GetActorUid() == actorUID {
			return true, nil
		}
	}
	return false, nil
}
