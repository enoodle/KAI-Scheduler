// Copyright 2026 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

package antiaffinity

import (
	"context"
	"fmt"
	"slices"
	"sync"

	v1 "k8s.io/api/core/v1"
	k8sframework "k8s.io/kube-scheduler/framework"
	kubernetesframework "k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/interpodaffinity"

	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/node_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/pod_info"
)

type bindReadiness struct {
	nodes              []*node_info.NodeInfo
	plugin             *interpodaffinity.InterPodAffinity
	antiAffinityStates sync.Map
}

// New creates a checker for the current scheduling session.
func New(nodes map[string]*node_info.NodeInfo, plugin *interpodaffinity.InterPodAffinity) BindReadiness {
	checker := &bindReadiness{plugin: plugin, nodes: make([]*node_info.NodeInfo, 0, len(nodes))}
	for _, node := range nodes {
		checker.nodes = append(checker.nodes, node)
	}
	return checker
}

func (checker *bindReadiness) IsReadyForBinding(task *pod_info.PodInfo, node *node_info.NodeInfo) (bool, error) {
	for _, otherNode := range checker.nodes {
		if len(otherNode.ReleasingPods) == 0 {
			continue
		}
		for _, releasing := range otherNode.ReleasingPods {
			if releasing.UID == task.UID {
				continue
			}
			for _, direction := range []struct {
				owner, other         *v1.Pod
				ownerNode, otherNode *v1.Node
			}{
				{task.Pod, releasing.Pod, node.Node, otherNode.Node},
				{releasing.Pod, task.Pod, otherNode.Node, node.Node},
			} {
				conflict, err := checker.requiredAntiAffinityMatches(direction.owner, direction.other, direction.ownerNode, direction.otherNode)
				if err != nil || conflict {
					return false, err
				}
			}
		}
	}
	return true, nil
}

type antiAffinityState struct {
	pod   *v1.Pod
	state k8sframework.CycleState
}

func (checker *bindReadiness) requiredAntiAffinityMatches(owner, other *v1.Pod, ownerNode, otherNode *v1.Node) (bool, error) {
	if len(k8sframework.GetPodAntiAffinityTerms(owner.Spec.Affinity)) == 0 {
		return false, nil
	}
	plugin := checker.plugin
	prepared, found := checker.antiAffinityStates.Load(owner)
	if !found {
		pod := owner.DeepCopy()
		// Evaluate required anti-affinity independently of ordinary affinity predicates.
		pod.Spec.Affinity = &v1.Affinity{PodAntiAffinity: pod.Spec.Affinity.PodAntiAffinity}
		state := kubernetesframework.NewCycleState()
		_, status := plugin.PreFilter(context.Background(), state, pod, nil)
		if !status.IsSuccess() {
			return false, fmt.Errorf("preparing required anti-affinity: %s", status.Message())
		}
		prepared, _ = checker.antiAffinityStates.LoadOrStore(owner, antiAffinityState{pod: pod, state: state})
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
