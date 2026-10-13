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
	"path/filepath"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/agent-substrate/substrate/internal/proto/ateompb"
)

// TestAteomWorkloadsRuns checks how ateomWorkloads answers from what the
// node's ateoms report. An ateom that nothing listens for hosts nothing, and
// one that fails otherwise may host the actor, unless another reports it.
func TestAteomWorkloadsRuns(t *testing.T) {
	const actorUID = "actor-uid-1"
	gone := status.Error(codes.Unavailable, "connection refused")
	stuck := status.Error(codes.DeadlineExceeded, "context deadline exceeded")
	runs := &ateompb.GetActiveWorkloadStatsResponse{Samples: []*ateompb.WorkloadStatsSample{
		measuredSample("other-actor", "ns-a", "tmpl-a", 1000, 700, 10),
		measuredSample(actorUID, "ns-a", "tmpl-a", 1000, 700, 10),
	}}

	t.Run("no ateoms directory", func(t *testing.T) {
		w := &ateomWorkloads{ateomsDir: filepath.Join(t.TempDir(), "ateoms")}
		if got, err := w.Runs(t.Context(), actorUID); got || err != nil {
			t.Errorf("Runs = %v, %v; want false, nil", got, err)
		}
	})

	for _, tc := range []struct {
		name    string
		fakes   map[string]*fakeStatsAteom
		want    bool
		wantErr bool
	}{
		{name: "no ateom"},
		{
			name:  "idle ateoms and another actor",
			fakes: map[string]*fakeStatsAteom{"uid-1": {resp: availableResponse()}, "uid-2": {resp: pendingResponse("other-actor")}},
		},
		{
			name:  "the actor runs",
			fakes: map[string]*fakeStatsAteom{"uid-1": {resp: availableResponse()}, "uid-2": {resp: runs}},
			want:  true,
		},
		{
			name:  "the actor is booting or restoring",
			fakes: map[string]*fakeStatsAteom{"uid-1": {resp: pendingResponse(actorUID)}},
			want:  true,
		},
		{
			name:  "an ateom that nothing listens for",
			fakes: map[string]*fakeStatsAteom{"uid-1": {err: gone}, "uid-2": {resp: availableResponse()}},
		},
		{
			name:    "an ateom that does not answer",
			fakes:   map[string]*fakeStatsAteom{"uid-1": {err: stuck}, "uid-2": {resp: availableResponse()}},
			wantErr: true,
		},
		{
			name:    "an ateom that cannot be dialed",
			fakes:   map[string]*fakeStatsAteom{"uid-1": nil},
			wantErr: true,
		},
		{
			name:  "another ateom reports the actor",
			fakes: map[string]*fakeStatsAteom{"uid-1": {err: stuck}, "uid-2": {resp: runs}},
			want:  true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, closers := newPollerFixture(t, tc.fakes)
			w := &ateomWorkloads{ateomsDir: p.ateomsDir, dial: p.dial}
			got, err := w.Runs(t.Context(), actorUID)
			if got != tc.want || (err != nil) != tc.wantErr {
				t.Errorf("Runs = %v, %v; want %v, error %v", got, err, tc.want, tc.wantErr)
			}
			for uid, f := range tc.fakes {
				if f != nil && !f.sawDeadline {
					t.Errorf("the call to ateom %s carried no deadline", uid)
				}
			}
			assertClosed(t, tc.fakes, closers, 1)
		})
	}
}
