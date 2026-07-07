// Copyright 2026 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

package allocate_test

import (
	"fmt"
	"runtime"
	"strings"
	"testing"

	"go.uber.org/mock/gomock"

	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/actions/allocate"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/common_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/pod_status"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/constants"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/framework"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/test_utils"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/test_utils/jobs_fake"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/test_utils/nodes_fake"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/test_utils/tasks_fake"
)

const (
	fitErrorBenchmarkRunningQueue = "fit-error-running-queue"
	fitErrorBenchmarkPendingQueue = "fit-error-pending-queue"
	fitErrorBenchmarkDepartment   = "fit-error-department"
	fitErrorBenchmarkGPUsPerNode  = 8
)

var allocateFitErrorsBenchmarkKeepAlive *framework.Session

func TestManyPendingTasksFailAcrossNodesFitErrors(t *testing.T) {
	test_utils.InitTestingInfrastructure()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	const numNodes = 10
	topology := buildManyPendingTasksFailAcrossNodesTopology(numNodes)
	ssn := test_utils.BuildSession(topology, ctrl)

	allocate.New().Execute(ssn)

	assertManyPendingTasksFailAcrossNodesFitErrors(t, ssn, numNodes)
}

func BenchmarkAllocateManyPendingTasksFailAcrossNodes_10Node(b *testing.B) {
	benchmarkAllocateManyPendingTasksFailAcrossNodes(b, 10)
}

func BenchmarkAllocateManyPendingTasksFailAcrossNodes_50Node(b *testing.B) {
	benchmarkAllocateManyPendingTasksFailAcrossNodes(b, 50)
}

func BenchmarkAllocateManyPendingTasksFailAcrossNodes_100Node(b *testing.B) {
	benchmarkAllocateManyPendingTasksFailAcrossNodes(b, 100)
}

func BenchmarkAllocateManyPendingTasksFailAcrossNodes_200Node(b *testing.B) {
	benchmarkAllocateManyPendingTasksFailAcrossNodes(b, 200)
}

func BenchmarkAllocateManyPendingTasksFailAcrossNodes_500Node(b *testing.B) {
	benchmarkAllocateManyPendingTasksFailAcrossNodes(b, 500)
}

func benchmarkAllocateManyPendingTasksFailAcrossNodes(b *testing.B, numNodes int) {
	test_utils.InitTestingInfrastructure()
	ctrl := gomock.NewController(b)
	defer ctrl.Finish()

	topology := buildManyPendingTasksFailAcrossNodesTopology(numNodes)
	action := allocate.New()
	expectedFailures := numNodes * numNodes * fitErrorBenchmarkGPUsPerNode

	b.ReportAllocs()
	b.ResetTimer()
	var heapLiveDelta uint64
	var totalFailures int
	for range b.N {
		b.StopTimer()
		runtime.GC()
		var before runtime.MemStats
		runtime.ReadMemStats(&before)
		b.StartTimer()

		ssn := test_utils.BuildSession(topology, ctrl)
		action.Execute(ssn)
		totalFailures = countFitErrorsForPendingJobs(ssn)
		if totalFailures != expectedFailures {
			b.Fatalf("recorded fit failures = %d, want %d", totalFailures, expectedFailures)
		}
		allocateFitErrorsBenchmarkKeepAlive = ssn

		b.StopTimer()
		var after runtime.MemStats
		runtime.ReadMemStats(&after)
		if after.HeapAlloc > before.HeapAlloc {
			heapLiveDelta = after.HeapAlloc - before.HeapAlloc
		} else {
			heapLiveDelta = 0
		}
		b.StartTimer()
	}
	b.StopTimer()
	b.ReportMetric(float64(totalFailures), "fit_failures/op")
	b.ReportMetric(float64(heapLiveDelta), "heap_live_delta_bytes/op")
}

func buildManyPendingTasksFailAcrossNodesTopology(numNodes int) test_utils.TestTopologyBasic {
	totalGPUs := numNodes * fitErrorBenchmarkGPUsPerNode
	nodes := make(map[string]nodes_fake.TestNodeBasic, numNodes)
	for nodeIndex := range numNodes {
		nodes[fmt.Sprintf("node-%d", nodeIndex)] = nodes_fake.TestNodeBasic{
			GPUs: fitErrorBenchmarkGPUsPerNode,
		}
	}

	jobs := make([]*jobs_fake.TestJobBasic, 0, totalGPUs*2)
	for jobIndex := range totalGPUs {
		nodeName := fmt.Sprintf("node-%d", jobIndex/fitErrorBenchmarkGPUsPerNode)
		jobs = append(jobs, &jobs_fake.TestJobBasic{
			Name:                fmt.Sprintf("running-job-%d", jobIndex),
			RequiredGPUsPerTask: 1,
			Priority:            constants.PriorityTrainNumber,
			QueueName:           fitErrorBenchmarkRunningQueue,
			Tasks: []*tasks_fake.TestTaskBasic{
				{
					State:    pod_status.Running,
					NodeName: nodeName,
				},
			},
		})
	}

	for jobIndex := range totalGPUs {
		jobs = append(jobs, &jobs_fake.TestJobBasic{
			Name:                fmt.Sprintf("pending-job-%d", jobIndex),
			RequiredGPUsPerTask: 1,
			Priority:            constants.PriorityTrainNumber,
			QueueName:           fitErrorBenchmarkPendingQueue,
			Tasks: []*tasks_fake.TestTaskBasic{
				{
					State: pod_status.Pending,
				},
			},
		})
	}

	return test_utils.TestTopologyBasic{
		Name:  "many pending tasks fail across all nodes",
		Nodes: nodes,
		Jobs:  jobs,
		Queues: []test_utils.TestQueueBasic{
			{
				Name:               fitErrorBenchmarkRunningQueue,
				ParentQueue:        fitErrorBenchmarkDepartment,
				DeservedGPUs:       float64(totalGPUs),
				GPUOverQuotaWeight: 1,
			},
			{
				Name:               fitErrorBenchmarkPendingQueue,
				ParentQueue:        fitErrorBenchmarkDepartment,
				DeservedGPUs:       float64(totalGPUs),
				GPUOverQuotaWeight: 1,
			},
		},
		Departments: []test_utils.TestDepartmentBasic{
			{
				Name:         fitErrorBenchmarkDepartment,
				DeservedGPUs: float64(totalGPUs * 2),
			},
		},
	}
}

func assertManyPendingTasksFailAcrossNodesFitErrors(t *testing.T, ssn *framework.Session, numNodes int) {
	t.Helper()

	expectedPendingJobs := numNodes * fitErrorBenchmarkGPUsPerNode
	expectedFailures := expectedPendingJobs * numNodes
	pendingJobs := 0

	for _, job := range ssn.ClusterInfo.PodGroupInfos {
		if !strings.HasPrefix(job.Name, "pending-job-") {
			continue
		}
		pendingJobs++
		if len(job.PodStatusIndex[pod_status.Pending]) != 1 {
			t.Fatalf("expected job %q to remain pending, got %d pending tasks", job.Name, len(job.PodStatusIndex[pod_status.Pending]))
		}
		if len(job.PodStatusIndex[pod_status.Running]) != 0 {
			t.Fatalf("expected job %q to have no running tasks", job.Name)
		}
	}

	if pendingJobs != expectedPendingJobs {
		t.Fatalf("pending jobs = %d, want %d", pendingJobs, expectedPendingJobs)
	}

	totalFailures := countFitErrorsForPendingJobs(ssn)
	if totalFailures != expectedFailures {
		t.Fatalf("recorded fit failures = %d, want %d", totalFailures, expectedFailures)
	}

	pendingJob := ssn.ClusterInfo.PodGroupInfos[common_info.PodGroupID("pending-job-0")]
	if pendingJob == nil {
		t.Fatal("expected pending-job-0 to exist")
	}
	task := pendingJob.PodStatusIndex[pod_status.Pending]["pending-job-0-0"]
	if task == nil {
		t.Fatal("expected pending task pending-job-0-0 to exist")
	}
	fitErrors := pendingJob.TasksFitErrors[task.UID]
	if fitErrors == nil {
		t.Fatal("expected pending task to have fit errors")
	}

	wantMessage := fmt.Sprintf("no nodes with enough resources were found: %d node(s) didn't have enough resources: GPUs.", numNodes)
	if got := fitErrors.Error(); got != wantMessage {
		t.Fatalf("fitErrors.Error() = %q, want %q", got, wantMessage)
	}
	if got := fitErrors.StoredDetailedNodeErrors(); got != 0 {
		t.Fatalf("StoredDetailedNodeErrors() = %d, want 0", got)
	}
}

func countFitErrorsForPendingJobs(ssn *framework.Session) int {
	totalFailures := 0
	for _, job := range ssn.ClusterInfo.PodGroupInfos {
		if !strings.HasPrefix(job.Name, "pending-job-") {
			continue
		}
		for _, fitErrors := range job.TasksFitErrors {
			totalFailures += fitErrors.TotalNodeErrors()
		}
	}
	return totalFailures
}
