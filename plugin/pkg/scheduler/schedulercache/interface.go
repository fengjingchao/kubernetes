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

import "k8s.io/kubernetes/pkg/api"

// NodeInfoCache collects pods' information and provides node-level aggregated information.
// It's intended to supplant system modeler in some cases for efficient lookup.
// NodeInfoCache's operations are pod centric. It incrementally updates itself based on pod event.
// Pod events are sent via network. We don't have guaranteed delivery of all events except last seen.
// Thus, we organized the state machine flow of a pod's events and handle it clearly.
//
// State Machine (life cycle) of a pod in scheduler's cache:
//
//                                                +-------+
//                                                |       |
//                                                |       | Update
//           Assume                Add            +       |
// Initial +--------> Binded  +----------------> Added <--+
//                      +                         +
//                      |                         |
//                      |                         |
//                      | Remove                  | Remove
//                      |                         v
//                      +------------------->  Deleted
//
// Depending on the implementation, it might choose to manage the expiration of assumed pod.
// Thus, a new state "Expired" could be added to the diagram:
//
//                                                     +--------+
//                                                     |        |
//                                                     |        | Update
//                             Add                     +        |
//                        +---------------+------->  Added <----+
//                        |               |            +
//                        |               |            |
//                        |               |            |
//           Assume       +    expire     +            |
// Initial +--------> Binded +-------> Expired         |
//                        +               +            |Remove
//                        |               |            |
//                        |               |            v
//                        +---------------+------> Deleted
//                              Remove
//
// Note:
// - Both "Initial" and "Deleted" pods do not actually exist in cache.
//   In order to differentiate them, we need external request to guarantee
//   no same pod will be created twice.
type NodeInfoCache interface {
	// AssumePod assumes a pod to be scheduled. The pod's information is aggregated into assigned node.
	// The implementation might decide the policy to expire/remove the assumed pod before it is confirmed to be scheduled.
	// After expiration, its information would be subtracted.
	AssumePod(pod *api.Pod) error
	// AddPod will confirms a pod if it's assumed, or adds back if it's expired.
	// If added back, the pod's information would be added again.
	AddPod(pod *api.Pod) error
	// UpdatePod updates a pod's information.
	UpdatePod(pod *api.Pod) error
	// RemovePod removes a pod. The pod's information would be subtracted from assigned node.
	RemovePod(pod *api.Pod) error
	// RemovePodByKey is similar to RemovePod except that it use pod's key.
	RemovePodByKey(key string) error
	// GetNodeInfo returns aggregated node information for given node name.
	// If no pod's been aggregated on the node, it returns nil.
	GetNodeInfo(nodeName string) *NodeInfo
}
