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

package snapshottracker

import (
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const trackedActorUID = "actor-uid-1"

// testRef is a MEMORY snapshot of the tracked actor.
func testRef(uuid string, generation int32, commitTime time.Time) SnapshotRef {
	return SnapshotRef{
		UUID:       uuid,
		Generation: generation,
		Fidelity:   ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
		URI:        "gs://test-bucket/atespaces/ate-demo/actors/" + trackedActorUID + "/snapshots/" + uuid,
		CommitTime: commitTime,
	}
}

func timeOf(ts *timestamppb.Timestamp) time.Time {
	if ts == nil {
		return time.Time{}
	}
	return ts.AsTime()
}

func uuidOf(r *ateletpb.SnapshotReport) string {
	return r.GetSnapshot().GetSnapshotUuid()
}

// State reports the latest local snapshot a deferred checkpoint leaves, and
// the latest durable one an immediate checkpoint leaves without a local copy.
// Forget drops both.
func TestTrackerState(t *testing.T) {
	tr := New()
	t0 := time.Now()
	st := tr.CommitLocal(trackedActorUID, testRef("s1", 1, t0))
	if got := st.GetLatestLocal(); uuidOf(got) != "s1" || !timeOf(got.GetLocalCommitTime()).Equal(t0) {
		t.Errorf("latest_local = %v, want s1 committed at %v", got, t0)
	}
	if st.GetLatestDurable() != nil {
		t.Errorf("latest_durable = %v, want unset", st.GetLatestDurable())
	}

	s2 := testRef("s2", 2, t0.Add(time.Minute))
	s2.UploadTime = s2.CommitTime.Add(time.Second)
	st = tr.CommitDurable(trackedActorUID, s2)
	if st.GetLatestLocal() != nil {
		t.Errorf("latest_local = %v after an immediate checkpoint, want unset", st.GetLatestLocal())
	}
	if got := st.GetLatestDurable(); uuidOf(got) != "s2" || !timeOf(got.GetDurableCompleteTime()).Equal(s2.UploadTime) {
		t.Errorf("latest_durable = %v, want s2 uploaded at %v", got, s2.UploadTime)
	}

	st = tr.CommitLocal(trackedActorUID, testRef("s3", 3, t0.Add(2*time.Minute)))
	if uuidOf(st.GetLatestLocal()) != "s3" || uuidOf(st.GetLatestDurable()) != "s2" {
		t.Errorf("state = %v, want s3 local and s2 still durable", st)
	}
	if got := tr.State(trackedActorUID); !proto.Equal(got, st) {
		t.Errorf("State() = %v, want %v, as CommitLocal returned", got, st)
	}

	tr.Forget(trackedActorUID)
	if got := tr.State(trackedActorUID); !proto.Equal(got, &ateletpb.ActorSnapshotState{}) {
		t.Errorf("state after forget = %v, want empty", got)
	}
}
