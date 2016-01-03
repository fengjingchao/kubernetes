/*
Copyright 2015 The Kubernetes Authors All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package schedulercache

import (
	"fmt"

	"k8s.io/kubernetes/pkg/api"
)

// NodeInfo is node level aggregated information based on pod event updates.
// Note: if change fields, also change String() method
type NodeInfo struct {
	PodNum            int
	UsedPorts         map[int]bool
	RequestedResource *Resource
}

// Resource is a collection of compute resource .
type Resource struct {
	MilliCPU int64
	Memory   int64
}

// NewNodeInfo returns a ready to use NodeInfo object.
func NewNodeInfo() *NodeInfo {
	return &NodeInfo{
		UsedPorts:         make(map[int]bool),
		RequestedResource: &Resource{},
		PodNum:            0,
	}
}

// Clone returns a copy of this NodeInfo object.
func (n *NodeInfo) Clone() *NodeInfo {
	used := make(map[int]bool)
	for port := range n.UsedPorts {
		used[port] = true
	}

	res := &NodeInfo{
		UsedPorts: used,
		RequestedResource: &Resource{
			MilliCPU: n.RequestedResource.MilliCPU,
			Memory:   n.RequestedResource.Memory,
		},
		PodNum: n.PodNum,
	}
	return res
}

// AddPodInfo adds pod information to this NodeInfo.
func (n *NodeInfo) AddPodInfo(pod *api.Pod) {
	for _, c := range pod.Spec.Containers {
		for _, port := range c.Ports {
			n.UsedPorts[port.HostPort] = true
		}
	}
	for _, c := range pod.Spec.Containers {
		req := c.Resources.Requests
		n.RequestedResource.MilliCPU += req.Cpu().MilliValue()
		n.RequestedResource.Memory += req.Memory().Value()
	}
	n.PodNum++
}

// SubPodInfo subtracts pod information to this NodeInfo.
func (n *NodeInfo) SubPodInfo(pod *api.Pod) {
	for _, c := range pod.Spec.Containers {
		for _, port := range c.Ports {
			delete(n.UsedPorts, port.HostPort)
		}
	}
	for _, c := range pod.Spec.Containers {
		req := c.Resources.Requests
		n.RequestedResource.MilliCPU -= req.Cpu().MilliValue()
		n.RequestedResource.Memory -= req.Memory().Value()
	}
	n.PodNum--
}

// String returns representation of human readable format of this NodeInfo.
func (n *NodeInfo) String() string {
	return fmt.Sprintf("&NodeInfo{PodNum:%v, UsedPorts:%v, RequestedResource:%#v}", n.PodNum, n.UsedPorts, n.RequestedResource)
}
