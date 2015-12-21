/*
Copyright 2014 The Kubernetes Authors All rights reserved.

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

package nodeinfo

import (
	"sync"
	"time"

	"github.com/golang/glog"
	"k8s.io/kubernetes/pkg/api"
	"k8s.io/kubernetes/pkg/client/cache"
)

// State Machine of a pod in scheduler's cache:
//
// Initial -->  Bind  -->  Add
//               |          |
//               |          V
//                ------> Delete
//                          |
//                          V
//                        End (Gone)
// Note:
//   Bind -> Delete: Missing Add event because of network disconnection;
//                   asumed pods might expire.
//   Bind -> Add: assumed pods might expire.

type Store interface {
	AssumePod(pod *api.Pod) error
	ConfirmPodScheduled(pod *api.Pod) error
	RemovePod(pod *api.Pod) error
	RemovePodByKey(key string)
	NodeInfoSnapshot(nodeName string) *NodeInfo
	CleanupExpiredAssumedPods()
}

type NodeInfo struct {
}

type store struct {
	keyFunc cache.KeyFunc

	mu          sync.Mutex
	ttl         time.Duration
	assumedPods map[string]assumedPod
	pods        map[string]*api.Pod
	nodes       map[string]*NodeInfo
}

type assumedPod struct {
	pod            *api.Pod
	expectToExpire time.Time
}

func NewStore(keyFunc cache.KeyFunc, ttl time.Duration) Store {
	return &store{
		keyFunc: keyFunc,
		ttl:     ttl,

		nodes:       make(map[string]*NodeInfo),
		pods:        make(map[string]*api.Pod),
		assumedPods: make(map[string]assumedPod),
	}
}

// return nil if no such node is found for given node name.
func (s *store) NodeInfoSnapshot(nodeName string) *NodeInfo {
	s.mu.Lock()
	defer s.mu.Unlock()

	n, ok := s.nodes[nodeName]
	if !ok {
		return nil
	}
	return n.clone()
}

func (s *store) AssumePod(pod *api.Pod) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key, err := s.keyFunc(pod)
	if err != nil {
		return err
	}

	if _, ok := s.pods[key]; ok {
		panic("Duplicate assume shouldn't happen")
	}

	// Assuming that the pod wasn't added yet.
	s.addPod(pod, key)
	aPod := assumedPod{
		pod:            pod,
		expectToExpire: time.Now().Add(s.ttl),
	}
	s.assumedPods[key] = aPod
	return nil
}

func (s *store) ConfirmPodScheduled(pod *api.Pod) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key, err := s.keyFunc(pod)
	if err != nil {
		return err
	}
	_, ok := s.assumedPods[key]
	if ok {
		delete(s.assumedPods, key)
		return nil
	}
	// The pod was not found in assumedPods.
	// It means that until expired the pod hadn't been confirmed.
	// We should add it still.
	s.addPod(pod, key)
	return nil
}

func (s *store) addPod(pod *api.Pod, key string) {
	n, ok := s.nodes[pod.Spec.NodeName]
	if !ok {
		n = &NodeInfo{}
		s.nodes[pod.Spec.NodeName] = n
	}
	n.addPodInfo(pod)
	s.pods[key] = pod
}

func (s *store) RemovePod(pod *api.Pod) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key, err := s.keyFunc(pod)
	if err != nil {
		return err
	}

	s.removePod(key)
	return nil
}

func (s *store) RemovePodByKey(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.removePod(key)
}

func (s *store) removePod(key string) {
	pod, ok := s.pods[key]
	// The pod was assumed and then expired.
	// We should still receive delete event at reconnection.
	if !ok {
		return
	}
	// It's guaranteed here the pod is in the cache
	delete(s.assumedPods, key)
	delete(s.pods, key)

	n := s.nodes[pod.Spec.NodeName]
	n.subPodInfo(pod)
}

func (s *store) CleanupExpiredAssumedPods() {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	// the size of assumedPods should be small
	for _, aPod := range s.assumedPods {
		if now.After(aPod.expectToExpire) {
			key, err := s.keyFunc(aPod.pod)
			if err != nil {
				glog.Errorf("pod didn't have key: %v", err)
				continue
			}
			s.removePod(key)
		}
	}
}

func (n *NodeInfo) clone() *NodeInfo {
	res := &NodeInfo{}
	return res
}

func (n *NodeInfo) addPodInfo(pod *api.Pod) {
}
func (n *NodeInfo) subPodInfo(pod *api.Pod) {
}
