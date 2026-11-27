// Copyright 2026 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

package reclaim

import (
	"testing"

	"go.uber.org/mock/gomock"

	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/pod_status"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/conf_util"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/constants"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/framework"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/test_utils"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/test_utils/jobs_fake"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/test_utils/nodes_fake"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/test_utils/tasks_fake"
)

func TestReclaimCountsPipelinedPodTowardQuota(t *testing.T) {
	test_utils.InitTestingInfrastructure()
	topology := test_utils.TestTopologyBasic{
		Name: "reclaim uses GPUs pipelined by allocate in the same session",
		Nodes: map[string]nodes_fake.TestNodeBasic{
			"node-a": {GPUs: 8},
			"node-b": {GPUs: 8},
			// Spare capacity raises inference-a's fair share to 16 GPUs. Neither job can use it.
			"node-c": {GPUs: 8},
		},
		Queues: []test_utils.TestQueueBasic{
			{Name: "inference-a", DeservedGPUs: 8, GPUOverQuotaWeight: 1},
			{Name: "inference-b", DeservedGPUs: 8, GPUOverQuotaWeight: 1},
			{Name: "other"},
		},
		Jobs: []*jobs_fake.TestJobBasic{
			eightGPUJob("inference-a-running", "inference-a", "node-a", pod_status.Running, "node-a", "node-b"),
			eightGPUJob("inference-a-pending", "inference-a", "", pod_status.Pending, "node-a", "node-b"),
			eightGPUJob("inference-b", "inference-b", "", pod_status.Pending, "node-a"),
			eightGPUJob("other-terminating", "other", "node-b", pod_status.Releasing, "node-b"),
		},
		JobExpectedResults: map[string]test_utils.TestExpectedResultBasic{
			"other-terminating":   {GPUsRequired: 8, Status: pod_status.Releasing},
			"inference-a-pending": {GPUsRequired: 8, Status: pod_status.Pipelined, NodeName: "node-b"},
			"inference-a-running": {GPUsRequired: 8, Status: pod_status.Releasing},
			"inference-b":         {GPUsRequired: 8, Status: pod_status.Pipelined, NodeName: "node-a"},
		},
		Mocks: &test_utils.TestMock{CacheRequirements: &test_utils.CacheMocking{
			NumberOfCacheEvictions:  1,
			NumberOfPipelineActions: 2,
		}},
	}
	ssn := test_utils.BuildSession(topology, gomock.NewController(t))

	actions, err := conf_util.GetActionsFromConfig(ssn.Config)
	if err != nil {
		t.Fatal(err)
	}
	for _, action := range actions {
		action.Execute(ssn)
		if action.Name() == framework.Allocate {
			afterAllocate := topology
			afterAllocate.JobExpectedResults = map[string]test_utils.TestExpectedResultBasic{
				"inference-a-pending": topology.JobExpectedResults["inference-a-pending"],
				"inference-a-running": {GPUsRequired: 8, Status: pod_status.Running, NodeName: "node-a"},
				"inference-b":         {GPUsRequired: 8, Status: pod_status.Pending},
			}
			test_utils.MatchExpectedAndRealTasks(t, 0, afterAllocate, ssn)
		}
	}

	test_utils.MatchExpectedAndRealTasks(t, 1, topology, ssn)
}

func eightGPUJob(name, queue, node string, status pod_status.PodStatus, affinity ...string) *jobs_fake.TestJobBasic {
	return &jobs_fake.TestJobBasic{
		Name:                name,
		QueueName:           queue,
		RequiredGPUsPerTask: 8,
		Priority:            constants.PriorityTrainNumber,
		Tasks:               []*tasks_fake.TestTaskBasic{{NodeName: node, State: status, NodeAffinityNames: affinity}},
	}
}
