// Copyright 2026 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

package predicates

import (
	"context"
	"fmt"
	"slices"

	v1 "k8s.io/api/core/v1"
	k8sframework "k8s.io/kube-scheduler/framework"
	kubernetesframework "k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/interpodaffinity"

	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/common_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/node_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/pod_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/pod_status"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/framework"
)

type releasingTask struct {
	task *pod_info.PodInfo
	node *node_info.NodeInfo
}

func (pp *predicatesPlugin) initializeReleasingTasks() {
	pp.releasingTasks = make(map[common_info.PodID]releasingTask)
	pp.releasingTasksWithAntiAffinity = make(map[common_info.PodID]releasingTask)
	for _, node := range pp.ssn.ClusterInfo.Nodes {
		for _, task := range node.PodInfos {
			pp.addReleasingTask(task, node)
		}
	}
	pp.ssn.AddEventHandler(&framework.EventHandler{
		AllocateFunc:   pp.updateReleasingTask,
		DeallocateFunc: pp.updateReleasingTask,
	})
}

func (pp *predicatesPlugin) addReleasingTask(task *pod_info.PodInfo, node *node_info.NodeInfo) {
	if task.Status != pod_status.Releasing {
		return
	}
	entry := releasingTask{task: task, node: node}
	pp.releasingTasks[task.UID] = entry
	if len(k8sframework.GetPodAntiAffinityTerms(task.Pod.Spec.Affinity)) > 0 {
		pp.releasingTasksWithAntiAffinity[task.UID] = entry
	}
}

func (pp *predicatesPlugin) updateReleasingTask(event *framework.Event) {
	previous, wasReleasing := pp.releasingTasks[event.Task.UID]
	delete(pp.releasingTasks, event.Task.UID)
	delete(pp.releasingTasksWithAntiAffinity, event.Task.UID)
	key := pod_info.PodKey(event.Task.Pod)
	if node := pp.ssn.ClusterInfo.Nodes[event.Task.NodeName]; node != nil {
		if task := node.PodInfos[key]; task != nil {
			pp.addReleasingTask(task, node)
		}
	}
	// A relocation can leave a releasing clone on the original node.
	if wasReleasing && previous.node.Name != event.Task.NodeName {
		if task := previous.node.PodInfos[key]; task != nil {
			pp.addReleasingTask(task, previous.node)
		}
	}
}

func (pp *predicatesPlugin) bindReady(task *pod_info.PodInfo, node *node_info.NodeInfo) (bool, error) {
	if len(pp.releasingTasks) == 0 {
		return true, nil
	}
	taskAntiAffinityRules := k8sframework.GetPodAntiAffinityTerms(task.Pod.Spec.Affinity)
	candidates := pp.releasingTasksWithAntiAffinity
	if len(taskAntiAffinityRules) > 0 {
		candidates = pp.releasingTasks
	}
	if len(candidates) == 0 {
		return true, nil
	}
	for _, entry := range candidates {
		if entry.task.UID == task.UID {
			continue
		}
		for _, direction := range []struct {
			owner, other         *v1.Pod
			ownerNode, otherNode *v1.Node
		}{
			{task.Pod, entry.task.Pod, node.Node, entry.node.Node},
			{entry.task.Pod, task.Pod, entry.node.Node, node.Node},
		} {
			conflict, err := pp.requiredAntiAffinityMatches(direction.owner, direction.other, direction.ownerNode, direction.otherNode)
			if err != nil || conflict {
				return false, err
			}
		}
	}
	return true, nil
}

type antiAffinityState struct {
	pod   *v1.Pod
	state k8sframework.CycleState
}

func (pp *predicatesPlugin) requiredAntiAffinityMatches(owner, other *v1.Pod, ownerNode, otherNode *v1.Node) (bool, error) {
	if len(k8sframework.GetPodAntiAffinityTerms(owner.Spec.Affinity)) == 0 {
		return false, nil
	}
	plugin := pp.ssn.InternalK8sPlugins().PodAffinity.(*interpodaffinity.InterPodAffinity)
	prepared, found := pp.antiAffinityStates.Load(owner)
	if !found {
		pod := owner.DeepCopy()
		// Evaluate required anti-affinity independently of ordinary affinity predicates.
		pod.Spec.Affinity = &v1.Affinity{PodAntiAffinity: pod.Spec.Affinity.PodAntiAffinity}
		state := kubernetesframework.NewCycleState()
		_, status := plugin.PreFilter(context.Background(), state, pod, nil)
		if !status.IsSuccess() {
			return false, fmt.Errorf("preparing required anti-affinity: %s", status.Message())
		}
		prepared, _ = pp.antiAffinityStates.LoadOrStore(owner, antiAffinityState{pod: pod, state: state})
	}
	base := prepared.(antiAffinityState)
	state := base.state.Clone()
	otherInfo, err := kubernetesframework.NewPodInfo(other)
	if err != nil {
		return false, err
	}
	otherNodeInfo := kubernetesframework.NewNodeInfo()
	otherNodeInfo.SetNode(otherNode)
	status := plugin.AddPod(context.Background(), state, base.pod, otherInfo, otherNodeInfo)
	if !status.IsSuccess() {
		return false, status.AsError()
	}
	ownerNodeInfo := kubernetesframework.NewNodeInfo()
	ownerNodeInfo.SetNode(ownerNode)
	status = plugin.Filter(context.Background(), state, base.pod, ownerNodeInfo)
	if status.IsSuccess() {
		return false, nil
	}
	if slices.Contains(status.Reasons(), interpodaffinity.ErrReasonAntiAffinityRulesNotMatch) {
		return true, nil
	}
	// The reverse direction is checked separately; ordinary pods belong to predicates.
	if slices.Contains(status.Reasons(), interpodaffinity.ErrReasonExistingAntiAffinityRulesNotMatch) {
		return false, nil
	}
	return false, status.AsError()
}
