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

package controlapi

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/internal/resources"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	listersv1alpha1 "github.com/agent-substrate/substrate/pkg/client/listers/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/wait"
)

// DeleteInput holds the immutable parameters requested by the client.
type DeleteInput struct {
	ActorRef resources.ActorRef
	Force    bool
}

// DeleteState holds the mutable state loaded and modified during execution.
type DeleteState struct {
	Actor         *ateapipb.Actor
	ActorTemplate *atev1alpha1.ActorTemplate
	DeletedActor  *ateapipb.Actor
}

type LoadActorForDeleteStep struct {
	store               store.Interface
	actorTemplateLister listersv1alpha1.ActorTemplateLister
}

func (s *LoadActorForDeleteStep) Name() string { return "LoadActorForDelete" }
func (s *LoadActorForDeleteStep) IsComplete(ctx context.Context, input *DeleteInput, state *DeleteState) (bool, error) {
	// Always run to get the freshest state
	return false, nil
}
func (s *LoadActorForDeleteStep) CheckPrerequisite(ctx context.Context, input *DeleteInput, state *DeleteState) error {
	return nil
}
func (s *LoadActorForDeleteStep) Execute(ctx context.Context, input *DeleteInput, state *DeleteState) error {
	actor, err := s.store.GetActor(ctx, input.ActorRef)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return status.Errorf(codes.NotFound, "Actor %s not found", input.ActorRef)
		}
		return fmt.Errorf("while fetching actor: %w", err)
	}
	state.Actor = actor

	if s.actorTemplateLister != nil && actor.GetActorTemplateNamespace() != "" && actor.GetActorTemplateName() != "" {
		tmpl, err := s.actorTemplateLister.ActorTemplates(actor.GetActorTemplateNamespace()).Get(actor.GetActorTemplateName())
		if err != nil && !k8serrors.IsNotFound(err) {
			return fmt.Errorf("while fetching actor template: %w", err)
		}
		state.ActorTemplate = tmpl
	}
	return nil
}

func (s *LoadActorForDeleteStep) RetryBackoff() *wait.Backoff { return nil }

type MarkTerminatingStep struct {
	store store.Interface
}

func (s *MarkTerminatingStep) Name() string { return "MarkTerminating" }

func (s *MarkTerminatingStep) IsComplete(ctx context.Context, input *DeleteInput, state *DeleteState) (bool, error) {
	st := state.Actor.GetStatus()
	return st == ateapipb.Actor_STATUS_TERMINATING || st == ateapipb.Actor_STATUS_DELETING, nil
}

func (s *MarkTerminatingStep) CheckPrerequisite(ctx context.Context, input *DeleteInput, state *DeleteState) error {
	if input.Force {
		switch state.Actor.GetStatus() {
		case ateapipb.Actor_STATUS_RUNNING,
			ateapipb.Actor_STATUS_RESUMING,
			ateapipb.Actor_STATUS_SUSPENDING,
			ateapipb.Actor_STATUS_PAUSING,
			ateapipb.Actor_STATUS_PAUSED,
			ateapipb.Actor_STATUS_CRASHED,
			ateapipb.Actor_STATUS_TERMINATING,
			ateapipb.Actor_STATUS_DELETING,
			ateapipb.Actor_STATUS_SUSPENDED:
			return nil
		default:
			return status.Errorf(codes.FailedPrecondition, "Actor %s is not in a deletable status (status: %v)", input.ActorRef, state.Actor.GetStatus())
		}
	}
	switch state.Actor.GetStatus() {
	case ateapipb.Actor_STATUS_SUSPENDED,
		ateapipb.Actor_STATUS_DELETING:
		return nil
	default:
		return status.Errorf(codes.FailedPrecondition, "Actor %s is not in a deletable status (status: %v)", input.ActorRef, state.Actor.GetStatus())
	}
}

func (s *MarkTerminatingStep) Execute(ctx context.Context, input *DeleteInput, state *DeleteState) error {
	state.Actor.Status = ateapipb.Actor_STATUS_TERMINATING
	updated, err := s.store.UpdateActor(ctx, state.Actor, state.Actor.GetMetadata().GetVersion())
	if err != nil {
		if errors.Is(err, store.ErrVersionConflict) {
			return status.Error(codes.Aborted, "concurrent update conflict, please retry")
		}
		return fmt.Errorf("while setting actor status to TERMINATING: %w", err)
	}
	state.Actor = updated
	return nil
}

func (s *MarkTerminatingStep) RetryBackoff() *wait.Backoff { return nil }

type MarkDeletingStep struct {
	store store.Interface
}

func (s *MarkDeletingStep) Name() string { return "MarkDeleting" }
func (s *MarkDeletingStep) IsComplete(ctx context.Context, input *DeleteInput, state *DeleteState) (bool, error) {
	return state.Actor.GetStatus() == ateapipb.Actor_STATUS_DELETING, nil
}
func (s *MarkDeletingStep) CheckPrerequisite(ctx context.Context, input *DeleteInput, state *DeleteState) error {
	st := state.Actor.GetStatus()
	if st != ateapipb.Actor_STATUS_TERMINATING && st != ateapipb.Actor_STATUS_DELETING {
		return status.Errorf(codes.FailedPrecondition, "MarkDeletingStep prerequisite not met for Actor: %s (got: %v, want %s or %s)", input.ActorRef, st, ateapipb.Actor_STATUS_TERMINATING, ateapipb.Actor_STATUS_DELETING)
	}
	return nil
}
func (s *MarkDeletingStep) Execute(ctx context.Context, input *DeleteInput, state *DeleteState) error {
	state.Actor.Status = ateapipb.Actor_STATUS_DELETING
	for _, vol := range state.Actor.GetActorVolumes() {
		vol.Status = ateapipb.ExternalVolume_STATUS_DELETING
	}
	updated, err := s.store.UpdateActor(ctx, state.Actor, state.Actor.GetMetadata().GetVersion())
	if err != nil {
		if errors.Is(err, store.ErrVersionConflict) {
			return status.Error(codes.Aborted, "concurrent update conflict, please retry")
		}
		return fmt.Errorf("while setting actor status to DELETING: %w", err)
	}
	state.Actor = updated
	return nil
}

func (s *MarkDeletingStep) RetryBackoff() *wait.Backoff { return nil }

type CallAteletTerminateStep struct {
	store  store.Interface
	dialer *AteletDialer
}

func (s *CallAteletTerminateStep) Name() string { return "CallAteletTerminate" }

func (s *CallAteletTerminateStep) IsComplete(ctx context.Context, input *DeleteInput, state *DeleteState) (bool, error) {
	return state.Actor.GetStatus() == ateapipb.Actor_STATUS_DELETING, nil
}

func (s *CallAteletTerminateStep) CheckPrerequisite(ctx context.Context, input *DeleteInput, state *DeleteState) error {
	st := state.Actor.GetStatus()
	if st != ateapipb.Actor_STATUS_TERMINATING {
		return status.Errorf(codes.FailedPrecondition, "CallAteletTerminateStep prerequisite not met for Actor: %s (got: %v, want %s)", input.ActorRef, st, ateapipb.Actor_STATUS_TERMINATING)
	}
	if state.ActorTemplate == nil {
		return status.Errorf(codes.FailedPrecondition, "actor template %s/%s not found for actor %s", state.Actor.GetActorTemplateNamespace(), state.Actor.GetActorTemplateName(), input.ActorRef)
	}
	return nil
}

func (s *CallAteletTerminateStep) Execute(ctx context.Context, input *DeleteInput, state *DeleteState) error {
	// TODO: if a workload has crashed, it's still possible there are resources on the node that need
	// to be cleaned up.
	assignment := state.Actor.GetWorkerAssignment()
	if assignment == nil {
		slog.InfoContext(ctx, "actor has no worker assignment, skipping CallAteletTerminateStep", slog.Any("actor", input.ActorRef))
		return nil
	}

	workerPodNs := assignment.GetWorkerNamespace()
	workerPodName := assignment.GetWorkerPod()

	conn, err := s.dialer.DialForWorker(workerPodNs, workerPodName)
	if err != nil {
		if errors.Is(err, ErrWorkerPodNotFound) {
			return status.Errorf(codes.NotFound, "worker pod %s/%s not found: %v", workerPodNs, workerPodName, err)
		}
		return fmt.Errorf("while connecting to worker pod %s/%s: %w", workerPodNs, workerPodName, err)
	}

	client := ateletpb.NewAteomHerderClient(conn)

	workloadSpec, err := workloadSpecFromActorTemplate(state.ActorTemplate, state.Actor)
	if err != nil {
		return err
	}

	req := &ateletpb.TerminateRequest{
		TargetAteomUid:         assignment.GetWorkerPodUid(),
		Atespace:               state.Actor.GetMetadata().GetAtespace(),
		ActorName:              state.Actor.GetMetadata().GetName(),
		ActorUid:               state.Actor.GetMetadata().GetUid(),
		ActorTemplateNamespace: state.Actor.GetActorTemplateNamespace(),
		ActorTemplateName:      state.Actor.GetActorTemplateName(),
		Spec:                   workloadSpec,
	}

	if _, err := client.Terminate(ctx, req); err != nil {
		return fmt.Errorf("while terminating actor on atelet: %w", err)
	}

	return nil
}

func (s *CallAteletTerminateStep) RetryBackoff() *wait.Backoff { return nil }

type DetachVolumesForDeleteStep struct {
	store store.Interface
}

func (s *DetachVolumesForDeleteStep) Name() string { return "DetachVolumesForDelete" }

func (s *DetachVolumesForDeleteStep) IsComplete(ctx context.Context, input *DeleteInput, state *DeleteState) (bool, error) {
	return state.Actor.GetStatus() == ateapipb.Actor_STATUS_DELETING, nil
}

func (s *DetachVolumesForDeleteStep) CheckPrerequisite(ctx context.Context, input *DeleteInput, state *DeleteState) error {
	st := state.Actor.GetStatus()
	if st != ateapipb.Actor_STATUS_TERMINATING {
		return status.Errorf(codes.FailedPrecondition, "DetachVolumesForDeleteStep prerequisite not met for Actor: %s (got: %v, want %s)", input.ActorRef, st, ateapipb.Actor_STATUS_TERMINATING)
	}
	if state.ActorTemplate == nil {
		return status.Errorf(codes.FailedPrecondition, "actor template %s/%s not found for actor %s", state.Actor.GetActorTemplateNamespace(), state.Actor.GetActorTemplateName(), input.ActorRef)
	}
	return nil
}

func (s *DetachVolumesForDeleteStep) Execute(ctx context.Context, input *DeleteInput, state *DeleteState) error {
	return detachActorVolumes(ctx, s.store, state.Actor, state.ActorTemplate, "delete")
}

func (s *DetachVolumesForDeleteStep) RetryBackoff() *wait.Backoff { return nil }

type ReleaseWorkerStep struct {
	store store.Interface
}

func (s *ReleaseWorkerStep) Name() string { return "ReleaseWorker" }

func (s *ReleaseWorkerStep) IsComplete(ctx context.Context, input *DeleteInput, state *DeleteState) (bool, error) {
	if state.Actor.GetStatus() == ateapipb.Actor_STATUS_DELETING {
		return true, nil
	}
	return state.Actor.GetWorkerAssignment() == nil, nil
}

func (s *ReleaseWorkerStep) CheckPrerequisite(ctx context.Context, input *DeleteInput, state *DeleteState) error {
	st := state.Actor.GetStatus()
	if st != ateapipb.Actor_STATUS_TERMINATING {
		return status.Errorf(codes.FailedPrecondition, "ReleaseWorkerStep prerequisite not met for Actor: %s (got: %v, want %s)", input.ActorRef, st, ateapipb.Actor_STATUS_TERMINATING)
	}
	return nil
}

func (s *ReleaseWorkerStep) Execute(ctx context.Context, input *DeleteInput, state *DeleteState) error {
	latestActor, err := s.store.GetActor(ctx, input.ActorRef)
	if err != nil {
		return err
	}

	if latestActor.GetWorkerAssignment() != nil {
		if err := releaseWorker(ctx, s.store, latestActor); err != nil {
			return err
		}

		latestActor, err = s.store.GetActor(ctx, input.ActorRef)
		if err != nil {
			return err
		}

		// TODO: can this be done inside releaseWorker()?
		latestActor.LocalSnapshotInfo = nil
		updatedActor, err := s.store.UpdateActor(ctx, latestActor, latestActor.GetMetadata().GetVersion())
		if err != nil {
			return err
		}
		state.Actor = updatedActor
	}
	return nil
}

func (s *ReleaseWorkerStep) RetryBackoff() *wait.Backoff { return nil }

type DeleteVolumesStep struct {
	store store.Interface
}

func (s *DeleteVolumesStep) Name() string { return "DeleteVolumes" }
func (s *DeleteVolumesStep) IsComplete(ctx context.Context, input *DeleteInput, state *DeleteState) (bool, error) {
	return false, nil
}
func (s *DeleteVolumesStep) CheckPrerequisite(ctx context.Context, input *DeleteInput, state *DeleteState) error {
	if state.Actor.GetStatus() != ateapipb.Actor_STATUS_DELETING {
		return status.Errorf(codes.FailedPrecondition, "DeleteVolumesStep prerequisite not met for Actor: %s (got: %v, want %s)", input.ActorRef, state.Actor.GetStatus(), ateapipb.Actor_STATUS_DELETING)
	}
	return nil
}
func (s *DeleteVolumesStep) Execute(ctx context.Context, input *DeleteInput, state *DeleteState) error {
	if err := deleteActorVolumes(ctx, state.Actor.GetMetadata().GetUid(), state.Actor.GetActorVolumes()); err != nil {
		return status.Errorf(codes.Internal, "while deleting actor volumes: %v", err)
	}
	return nil
}

func (s *DeleteVolumesStep) RetryBackoff() *wait.Backoff { return nil }

type FinalizeDeletedStep struct {
	store store.Interface
}

func (s *FinalizeDeletedStep) Name() string { return "FinalizeDeleted" }
func (s *FinalizeDeletedStep) IsComplete(ctx context.Context, input *DeleteInput, state *DeleteState) (bool, error) {
	return state.DeletedActor != nil, nil
}
func (s *FinalizeDeletedStep) CheckPrerequisite(ctx context.Context, input *DeleteInput, state *DeleteState) error {
	if state.Actor.GetStatus() != ateapipb.Actor_STATUS_DELETING {
		return status.Errorf(codes.FailedPrecondition, "FinalizeDeletedStep prerequisite not met for Actor: %s (got: %v, want %s)", input.ActorRef, state.Actor.GetStatus(), ateapipb.Actor_STATUS_DELETING)
	}
	return nil
}
func (s *FinalizeDeletedStep) Execute(ctx context.Context, input *DeleteInput, state *DeleteState) error {
	deleted, err := s.store.DeleteActor(ctx, input.ActorRef)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return status.Errorf(codes.NotFound, "Actor %s not found", input.ActorRef)
		}
		if errors.Is(err, store.ErrFailedPrecondition) {
			current, getErr := s.store.GetActor(ctx, input.ActorRef)
			if getErr == nil {
				return status.Errorf(codes.FailedPrecondition, "Actor %s is not in a deletable status (status: %v)", input.ActorRef, current.GetStatus())
			}
			return status.Errorf(codes.FailedPrecondition, "Actor %s is not in a deletable status", input.ActorRef)
		}
		if errors.Is(err, store.ErrVersionConflict) {
			return status.Error(codes.Aborted, "concurrent update conflict, please retry")
		}
		return fmt.Errorf("while deleting actor from DB: %w", err)
	}
	state.DeletedActor = deleted
	return nil
}

func (s *FinalizeDeletedStep) RetryBackoff() *wait.Backoff { return nil }
