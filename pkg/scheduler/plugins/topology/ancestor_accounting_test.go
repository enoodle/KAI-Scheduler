// Copyright 2026 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

package topology

import (
	"testing"

	kaiv1alpha1 "github.com/kai-scheduler/api/kai/v1alpha1"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/node_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/pod_status"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/podgroup_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/podgroup_info/subgroup_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/resource_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/topology_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/test_utils/jobs_fake"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/test_utils/nodes_fake"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/test_utils/tasks_fake"
)

func TestSubsetNodesAccountsForCandidateSubtreeInAncestors(t *testing.T) {
	jobSpec := &jobs_fake.TestJobBasic{
		Name: "job", RequiredGPUsPerTask: 1,
		RootSubGroupSet: subgroup_info.NewSubGroupSet(subgroup_info.RootSubGroupSetName,
			&topology_info.TopologyConstraintInfo{Topology: "t", RequiredLevel: "root", PreferredLevel: "root"}),
		Tasks: []*tasks_fake.TestTaskBasic{{State: pod_status.Pending}, {State: pod_status.Pending}},
	}
	vec := resource_info.NewResourceVectorMap()
	jobs, tasksToNodes, _ := jobs_fake.BuildJobsAndTasksMaps([]*jobs_fake.TestJobBasic{jobSpec}, vec)
	newNode := func(zone, rack string, gpus int) nodes_fake.TestNodeBasic {
		return nodes_fake.TestNodeBasic{GPUs: gpus, CPUMillis: 4000, MaxTaskNum: ptr.To(100),
			Labels: map[string]string{"region": "us", "zone": zone, "rack": rack}}
	}
	nodes := nodes_fake.BuildNodesInfoMap(map[string]nodes_fake.TestNodeBasic{
		"east-a": newNode("east", "a", 1),
		"east-b": newNode("east", "b", 1),
		"west-c": newNode("west", "c", 8),
	}, tasksToNodes, nil, vec)
	plugin := New(nil).(*topologyPlugin)
	plugin.initializeTopologyTree([]*kaiv1alpha1.Topology{{
		ObjectMeta: metav1.ObjectMeta{Name: "t"},
		Spec: kaiv1alpha1.TopologySpec{Levels: []kaiv1alpha1.TopologyLevel{
			{NodeLabel: "region"}, {NodeLabel: "zone"}, {NodeLabel: "rack"},
		}},
	}}, nodes)
	job := jobs["job"]
	group := &job.RootSubGroupSet.SubGroupInfo
	tasks := podgroup_info.GetTasksToAllocate(job, nil, nil, podgroup_info.RealTaskAllocation)
	candidates := node_info.NodeSet{nodes["east-a"], nodes["east-b"]}

	subsets, err := plugin.subSetNodesFn(job, group, nil, tasks, candidates)
	require.NoError(t, err)
	require.Len(t, subsets, 1)
	require.ElementsMatch(t, candidates, subsets[0])
	domains := plugin.TopologyTrees["t"].DomainsByLevel
	for _, domain := range []*DomainInfo{domains["region"]["us"], domains[rootLevel][rootDomainId]} {
		require.Equal(t, float64(2), domain.IdleOrReleasingVector.Get(resource_info.GPUIndex))
		require.Equal(t, 2, domain.AllocatablePods)
	}
	for _, domain := range []*DomainInfo{domains["zone"]["us.west"], domains["rack"]["us.west.c"]} {
		require.Zero(t, domain.IdleOrReleasingVector.Get(resource_info.GPUIndex))
		require.Equal(t, allocatablePodsNotSet, domain.AllocatablePods)
	}
	scores := plugin.getRelevantNodeScores(group)
	require.Contains(t, scores, "east-a")
	require.Contains(t, scores, "east-b")
	require.Equal(t, scores["east-a"], scores["east-b"])

	group.GetTopologyConstraint().PreferredLevel = "rack"
	subsets, err = plugin.subSetNodesFn(job, group, nil, tasks, candidates)
	require.NoError(t, err)
	require.Len(t, subsets, 1)
	require.ElementsMatch(t, candidates, subsets[0])

	subsets, err = plugin.subSetNodesFn(job, group, nil, tasks[:1], candidates)
	require.NoError(t, err)
	require.Len(t, subsets, 3)
	require.Len(t, subsets[0], 1)
	require.Len(t, subsets[1], 1)
	require.ElementsMatch(t, candidates, node_info.NodeSet{subsets[0][0], subsets[1][0]})
	require.ElementsMatch(t, candidates, subsets[2])
}
