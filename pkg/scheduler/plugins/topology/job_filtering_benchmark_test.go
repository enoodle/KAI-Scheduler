// Copyright 2026 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

package topology

import (
	"fmt"
	"testing"

	kaiv1alpha1 "github.com/kai-scheduler/api/kai/v1alpha1"

	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/node_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/pod_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/podgroup_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/podgroup_info/subgroup_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/resource_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/topology_info"
)

const (
	benchmarkDomainCount     = 100
	benchmarkNodesPerDomain  = 10
	benchmarkTaskCount       = 8
	benchmarkNodeMilliCPU    = 3000
	benchmarkNodeMemoryBytes = 64 << 30
	benchmarkNodePodCapacity = 100
	benchmarkTaskMemoryBytes = 1 << 30
	benchmarkDeviceMemoryMiB = 16 << 10
	benchmarkSharedMemoryMiB = benchmarkDeviceMemoryMiB / 2
	benchmarkTopologyName    = "benchmark-topology"
	benchmarkTopologyLevel   = "zone"
)

// BenchmarkTopologyDomainFiltering measures candidate selection for a job that requires a single zone.
func BenchmarkTopologyDomainFiltering(b *testing.B) {
	for _, workload := range []struct {
		name          string
		idleGPUs      float64
		sharedGPUs    int
		taskMilliCPUs []float64
		taskGPUs      float64
	}{
		{
			name:          "whole_gpu",
			idleGPUs:      8,
			taskMilliCPUs: []float64{2000},
			taskGPUs:      1,
		},
		{
			name:          "fractional_gpu",
			sharedGPUs:    8,
			taskMilliCPUs: []float64{2000},
			taskGPUs:      0.5,
		},
		// Every zone has enough total CPU, but no node can fit a 4-CPU task.
		{
			name:          "fragmented_cpu",
			idleGPUs:      8,
			taskMilliCPUs: []float64{2000, 4000},
			taskGPUs:      1,
		},
	} {
		b.Run(workload.name, func(b *testing.B) {
			tree, nodes := newDomainFilteringBenchmarkTopology(workload.idleGPUs, workload.sharedGPUs)
			tasks := newDomainFilteringBenchmarkTasks(tree.VectorMap, workload.taskMilliCPUs, workload.taskGPUs)
			job := &podgroup_info.PodGroupInfo{
				Name: "benchmark-job", Namespace: "default",
				RootSubGroupSet: subgroup_info.NewSubGroupSet(subgroup_info.RootSubGroupSetName,
					&topology_info.TopologyConstraintInfo{Topology: tree.Name, RequiredLevel: benchmarkTopologyLevel}),
			}
			plugin := &topologyPlugin{TopologyTrees: map[string]*Info{tree.Name: tree}}
			subGroup := &job.RootSubGroupSet.SubGroupInfo
			sets, err := plugin.subSetNodesFn(job, subGroup, nil, tasks, nodes)
			if err != nil {
				b.Fatal(err)
			}
			// Candidate counts can differ across scheduler revisions.
			expectedCandidates := len(sets)
			b.ReportAllocs()
			for b.Loop() {
				// Each attempt starts without fit errors; retain error construction in the measured work.
				job.JobFitErrors = nil
				sets, err := plugin.subSetNodesFn(job, subGroup, nil, tasks, nodes)
				if err != nil {
					b.Fatal(err)
				}
				if len(sets) != expectedCandidates {
					b.Fatalf("candidate count changed between attempts: got %d, want %d", len(sets), expectedCandidates)
				}
			}
			b.ReportMetric(float64(expectedCandidates), "domains/op")
		})
	}
}

func newDomainFilteringBenchmarkTopology(idleGPUs float64, sharedGPUs int) (*Info, node_info.NodeSet) {
	vectorMap := resource_info.NewResourceVectorMap()
	root := &DomainInfo{ID: rootDomainId, Level: rootLevel, Nodes: map[string]*node_info.NodeInfo{}}
	domains := make(LevelDomainInfos, benchmarkDomainCount)
	nodes := make(node_info.NodeSet, 0, benchmarkDomainCount*benchmarkNodesPerDomain)
	for domainIndex := range benchmarkDomainCount {
		zone := fmt.Sprintf("zone-%03d", domainIndex)
		domain := &DomainInfo{
			ID: DomainID(zone), Level: benchmarkTopologyLevel,
			Nodes: make(map[string]*node_info.NodeInfo, benchmarkNodesPerDomain),
		}
		domains[domain.ID] = domain
		root.Children = append(root.Children, domain)
		for nodeIndex := range benchmarkNodesPerDomain {
			name := fmt.Sprintf("%s-node-%02d", zone, nodeIndex)
			node := newDomainFilteringBenchmarkNode(name, zone, vectorMap, idleGPUs, sharedGPUs)
			domain.Nodes[name] = node
			root.Nodes[name] = node
			nodes = append(nodes, node)
		}
	}
	return &Info{
		Name: benchmarkTopologyName, VectorMap: vectorMap,
		TopologyResource: &kaiv1alpha1.Topology{Spec: kaiv1alpha1.TopologySpec{
			Levels: []kaiv1alpha1.TopologyLevel{{NodeLabel: benchmarkTopologyLevel}},
		}},
		DomainsByLevel: domainsByLevel{rootLevel: {rootDomainId: root}, benchmarkTopologyLevel: domains},
	}, nodes
}

func newDomainFilteringBenchmarkNode(name, zone string, vectorMap *resource_info.ResourceVectorMap,
	idleGPUs float64, sharedGPUs int,
) *node_info.NodeInfo {
	node := newNodeInfo(name, map[string]string{benchmarkTopologyLevel: zone})
	node.VectorMap = vectorMap
	node.IdleVector = resource_info.NewResourceVector(vectorMap)
	node.IdleVector.Set(resource_info.CPUIndex, benchmarkNodeMilliCPU)
	node.IdleVector.Set(resource_info.MemoryIndex, benchmarkNodeMemoryBytes)
	node.IdleVector.Set(resource_info.GPUIndex, idleGPUs)
	node.IdleVector.Set(resource_info.PodsIndex, benchmarkNodePodCapacity)
	node.ReleasingVector = resource_info.NewResourceVector(vectorMap)
	if sharedGPUs > 0 {
		node.MemoryOfEveryGpuOnNode = benchmarkDeviceMemoryMiB
		node.UsedSharedGPUsMemory = make(map[string]int64, sharedGPUs)
		node.AllocatedSharedGPUsMemory = make(map[string]int64, sharedGPUs)
		for deviceIndex := range sharedGPUs {
			id := fmt.Sprintf("GPU-%d", deviceIndex)
			node.UsedSharedGPUsMemory[id] = benchmarkSharedMemoryMiB
			node.AllocatedSharedGPUsMemory[id] = benchmarkSharedMemoryMiB
		}
	}
	return node
}

func newDomainFilteringBenchmarkTasks(vectorMap *resource_info.ResourceVectorMap,
	milliCPUs []float64, gpus float64,
) []*pod_info.PodInfo {
	tasks := make([]*pod_info.PodInfo, 0, benchmarkTaskCount)
	for taskIndex := range benchmarkTaskCount {
		request := resource_info.NewResourceVector(vectorMap)
		request.Set(resource_info.CPUIndex, milliCPUs[taskIndex%len(milliCPUs)])
		request.Set(resource_info.MemoryIndex, benchmarkTaskMemoryBytes)
		request.Set(resource_info.GPUIndex, gpus)
		request.Set(resource_info.PodsIndex, 1)
		tasks = append(tasks, &pod_info.PodInfo{
			VectorMap: vectorMap, ResReqVector: request,
			GpuRequirement: *resource_info.NewGpuResourceRequirementWithGpus(gpus, 0),
		})
	}
	return tasks
}
