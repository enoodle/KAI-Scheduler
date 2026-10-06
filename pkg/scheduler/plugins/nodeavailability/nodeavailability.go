// Copyright 2025 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

package nodeavailability

import (
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/node_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/pod_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/resource_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/framework"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/log"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/plugins/scores"
)

type nodeAvailabilityPlugin struct{}

// New function returns nodeAvailabilityPlugin object
func New(_ framework.PluginArguments) framework.Plugin {
	return &nodeAvailabilityPlugin{}
}

func (pp *nodeAvailabilityPlugin) Name() string {
	return "nodeavailability"
}

func (pp *nodeAvailabilityPlugin) OnSessionOpen(ssn *framework.Session) {
	ssn.AddNodeOrderFn(func(task *pod_info.PodInfo, node *node_info.NodeInfo) (float64, error) {
		return pp.nodeOrderFn(ssn, task, node)
	})
}

func (pp *nodeAvailabilityPlugin) nodeOrderFn(ssn *framework.Session, task *pod_info.PodInfo, node *node_info.NodeInfo) (float64, error) {
	score := 0.0
	if taskAllocatable := node.IsTaskAllocatable(task); taskAllocatable {
		ready, err := ssn.IsTaskReadyForBinding(task, node)
		if err != nil {
			return 0, err
		}
		if ready {
			score = scores.Availability
		}
	}

	log.InfraLogger.V(7).Do(func() {
		log.InfraLogger.Infof(
			"Estimating Task: <%v/%v> Job: <%v> for node: <%s> that has <%f> idle GPUs and <%f> releasing GPUs and <%f> allocated GPUs. Score: %f",
			task.Namespace, task.Name, task.Job, node.Name,
			node.IdleVector.Get(resource_info.GPUIndex),
			node.ReleasingVector.Get(resource_info.GPUIndex),
			node.UsedVector.Get(resource_info.GPUIndex),
			score)
	})
	return score, nil
}

func (pp *nodeAvailabilityPlugin) OnSessionClose(_ *framework.Session) {}
