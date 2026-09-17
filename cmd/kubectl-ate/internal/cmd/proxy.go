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

package cmd

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net/http"
	"strings"
	"time"

	"github.com/agent-substrate/substrate/internal/ateclient"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/spf13/cobra"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var proxyPortFlag int

type proxyActorReq struct {
	Atespace string `json:"atespace"`
	Name     string `json:"name"`
}

type proxyActorResp struct {
	WorkerPod     string   `json:"worker_pod"`
	SnapshotNodes []string `json:"snapshot_nodes"`
	ElapsedMs     float64  `json:"elapsed_ms"`
	Error         string   `json:"error,omitempty"`
}

var proxyCmd = &cobra.Command{
	Use:   "proxy",
	Short: "Run a local HTTP proxy that multiplexes requests over a single persistent gRPC connection to ate-api-server",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		apiClient, err := ateclient.NewClient(ctx, kubeconfig, k8sContext, endpoint, tokenFile, traceEnabled)
		if err != nil {
			return fmt.Errorf("failed to connect to ate-api-server: %w", err)
		}
		defer apiClient.Close()

		mux := http.NewServeMux()

		mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
		})

		mux.HandleFunc("/actor", func(w http.ResponseWriter, r *http.Request) {
			atespace := r.URL.Query().Get("atespace")
			name := r.URL.Query().Get("name")
			actorRef := resources.ActorRef{Atespace: atespace, Name: name}
			t0 := time.Now()
			a, err := apiClient.GetActor(r.Context(), &ateapipb.GetActorRequest{
				Actor: actorRef.ToObjectRef(),
			})
			elapsed := float64(time.Since(t0).Microseconds()) / 1000.0
			w.Header().Set("Content-Type", "application/json")
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				_ = json.NewEncoder(w).Encode(proxyActorResp{Error: err.Error(), ElapsedMs: elapsed})
				return
			}
			_ = json.NewEncoder(w).Encode(proxyActorResp{
				WorkerPod:     a.GetStatus().GetWorkerAssignment().GetWorkerPod(),
				SnapshotNodes: a.GetStatus().GetLocalSnapshotInfo().GetNodeVmsWithLocalSnapshots(),
				ElapsedMs:     elapsed,
			})
		})

		mux.HandleFunc("/resume", func(w http.ResponseWriter, r *http.Request) {
			var req proxyActorReq
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			actorRef := resources.ActorRef{Atespace: req.Atespace, Name: req.Name}
			t0 := time.Now()
			var resp *ateapipb.ResumeActorResponse
			var err error
			// Retry transient concurrent update conflicts up to 5 times with jitter (matching PR #1631)
			for attempt := 0; attempt < 5; attempt++ {
				resp, err = apiClient.ResumeActor(r.Context(), &ateapipb.ResumeActorRequest{
					Actor: actorRef.ToObjectRef(),
				})
				if err == nil {
					break
				}
				if status.Code(err) == codes.Aborted && strings.Contains(err.Error(), "concurrent update conflict") {
					time.Sleep(time.Duration(40+rand.IntN(30)) * time.Millisecond)
					continue
				}
				break
			}
			elapsed := float64(time.Since(t0).Microseconds()) / 1000.0
			w.Header().Set("Content-Type", "application/json")
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				_ = json.NewEncoder(w).Encode(proxyActorResp{Error: err.Error(), ElapsedMs: elapsed})
				return
			}
			a := resp.GetActor()
			_ = json.NewEncoder(w).Encode(proxyActorResp{
				WorkerPod:     a.GetStatus().GetWorkerAssignment().GetWorkerPod(),
				SnapshotNodes: a.GetStatus().GetLocalSnapshotInfo().GetNodeVmsWithLocalSnapshots(),
				ElapsedMs:     elapsed,
			})
		})

		mux.HandleFunc("/pause", func(w http.ResponseWriter, r *http.Request) {
			var req proxyActorReq
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			actorRef := resources.ActorRef{Atespace: req.Atespace, Name: req.Name}
			t0 := time.Now()
			resp, err := apiClient.PauseActor(r.Context(), &ateapipb.PauseActorRequest{
				Actor: actorRef.ToObjectRef(),
			})
			elapsed := float64(time.Since(t0).Microseconds()) / 1000.0
			w.Header().Set("Content-Type", "application/json")
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				_ = json.NewEncoder(w).Encode(proxyActorResp{Error: err.Error(), ElapsedMs: elapsed})
				return
			}
			a := resp.GetActor()
			_ = json.NewEncoder(w).Encode(proxyActorResp{
				WorkerPod:     a.GetStatus().GetWorkerAssignment().GetWorkerPod(),
				SnapshotNodes: a.GetStatus().GetLocalSnapshotInfo().GetNodeVmsWithLocalSnapshots(),
				ElapsedMs:     elapsed,
			})
		})

		mux.HandleFunc("/suspend", func(w http.ResponseWriter, r *http.Request) {
			var req proxyActorReq
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			actorRef := resources.ActorRef{Atespace: req.Atespace, Name: req.Name}
			t0 := time.Now()
			resp, err := apiClient.SuspendActor(r.Context(), &ateapipb.SuspendActorRequest{
				Actor: actorRef.ToObjectRef(),
			})
			elapsed := float64(time.Since(t0).Microseconds()) / 1000.0
			w.Header().Set("Content-Type", "application/json")
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				_ = json.NewEncoder(w).Encode(proxyActorResp{Error: err.Error(), ElapsedMs: elapsed})
				return
			}
			a := resp.GetActor()
			_ = json.NewEncoder(w).Encode(proxyActorResp{
				WorkerPod:     a.GetStatus().GetWorkerAssignment().GetWorkerPod(),
				SnapshotNodes: a.GetStatus().GetLocalSnapshotInfo().GetNodeVmsWithLocalSnapshots(),
				ElapsedMs:     elapsed,
			})
		})

		addr := fmt.Sprintf("127.0.0.1:%d", proxyPortFlag)
		fmt.Printf("kubectl-ate persistent gRPC proxy listening on http://%s\n", addr)
		srv := &http.Server{Addr: addr, Handler: mux}
		go func() {
			<-ctx.Done()
			_ = srv.Close()
		}()
		return srv.ListenAndServe()
	},
}

func init() {
	proxyCmd.Flags().IntVar(&proxyPortFlag, "port", 18080, "Local port for HTTP-to-gRPC proxy")
	rootCmd.AddCommand(proxyCmd)
}
