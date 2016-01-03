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

package nodeinfocache

import (
	"sync"
	"time"

	"fmt"

	"github.com/golang/glog"
	"k8s.io/kubernetes/pkg/api"
	"k8s.io/kubernetes/pkg/util"
	"k8s.io/kubernetes/plugin/pkg/scheduler/schedulercache"
)

// New returns a NodeInfoCache implementation.
// It automatically starts a go routine that manages expiration of assumed pods.
// "ttl" is how long the assumed pod will get expired.
// "period" is how long the background goroutine should wait before cleaning up expired pods periodically.
// "stop" is the channel that signals stopping and we would close background goroutines.
func New(ttl, period time.Duration, stop chan struct{}) schedulercache.NodeInfoCache {
	cache := newNodeInfoCache(ttl, period, stop)
	cache.run()
	return cache
}

// This is copied from client/cache.MetaNamespaceKeyFunc .
// Since pod is ensured to have object, we don't need to take care of the error returned.
// A more appropriate way is to support Key() method in pod object.
func podKeyFunc(pod *api.Pod) string {
	meta := pod.GetObjectMeta()
	if len(meta.GetNamespace()) > 0 {
		return meta.GetNamespace() + "/" + meta.GetName()
	}
	return meta.GetName()
}

type nodeInfoCache struct {
	stop chan struct{}

	mu          sync.Mutex
	ttl         time.Duration
	period      time.Duration
	assumedPods map[string]assumedPod
	// A map of added pods
	// - support RemovePodByKey
	// - update: subtracts information from old pod
	// - record pod state and implement state machine flow
	pods      map[string]*api.Pod
	podStates map[string]podState
	// TODO: we should also watch node deletion and clean up node entries
	nodes map[string]*schedulercache.NodeInfo
}

type podState int

const (
	podBinded podState = iota + 1
	podAdded
	podExpired
)

type assumedPod struct {
	pod            *api.Pod
	expectToExpire time.Time
}

func newNodeInfoCache(ttl, period time.Duration, stop chan struct{}) *nodeInfoCache {
	return &nodeInfoCache{
		ttl:    ttl,
		period: period,
		stop:   stop,

		nodes:       make(map[string]*schedulercache.NodeInfo),
		pods:        make(map[string]*api.Pod),
		podStates:   make(map[string]podState),
		assumedPods: make(map[string]assumedPod),
	}
}

func (cache *nodeInfoCache) run() {
	go util.Until(cache.cleanupExpiredAssumedPods, cache.period, cache.stop)
}

// return nil if no such node is found for given node name.
func (cache *nodeInfoCache) GetNodeInfo(nodeName string) *schedulercache.NodeInfo {
	cache.mu.Lock()
	defer cache.mu.Unlock()

	n, ok := cache.nodes[nodeName]
	if !ok {
		return nil
	}
	if n.PodNum == 0 {
		return nil
	}
	return n.Clone()
}

func (cache *nodeInfoCache) AssumePod(pod *api.Pod) error {
	return cache.assumePod(pod, time.Now())
}

// separate out form time deterministic testsability
func (cache *nodeInfoCache) assumePod(pod *api.Pod, now time.Time) error {
	cache.mu.Lock()
	defer cache.mu.Unlock()

	key := podKeyFunc(pod)
	if _, ok := cache.podStates[key]; ok {
		return fmt.Errorf("Pod state wasn't initial but get assumed. Pod key: %v", key)
	}

	cache.addPod(pod, key)
	cache.podStates[key] = podBinded
	aPod := assumedPod{
		pod:            pod,
		expectToExpire: now.Add(cache.ttl),
	}
	cache.assumedPods[key] = aPod
	return nil
}

func (cache *nodeInfoCache) AddPod(pod *api.Pod) error {
	cache.mu.Lock()
	defer cache.mu.Unlock()

	key := podKeyFunc(pod)
	state, ok := cache.podStates[key]
	switch {
	case ok && state == podBinded:
		delete(cache.assumedPods, key)
	case ok && state == podExpired:
		// Pod was expired and then deleted. We should add it back.
		cache.addPod(pod, key)
	default:
		return fmt.Errorf("Pod state wasn't binded or expired but get added. Pod key: %v", key)
	}
	cache.podStates[key] = podAdded
	return nil
}

func (cache *nodeInfoCache) UpdatePod(pod *api.Pod) error {
	cache.mu.Lock()
	defer cache.mu.Unlock()

	key := podKeyFunc(pod)
	state, ok := cache.podStates[key]
	switch {
	case ok && state == podAdded:
		cache.updatePod(pod, key)
	default:
		return fmt.Errorf("Pod state wasn't added but get updated. Pod key: %v", key)
	}
	return nil
}

func (cache *nodeInfoCache) updatePod(newPod *api.Pod, key string) {
	oldPod := cache.pods[key]
	cache.deletePod(oldPod, key)
	cache.addPod(newPod, key)
}

func (cache *nodeInfoCache) addPod(pod *api.Pod, key string) {
	n, ok := cache.nodes[pod.Spec.NodeName]
	if !ok {
		n = schedulercache.NewNodeInfo()
		cache.nodes[pod.Spec.NodeName] = n
	}
	n.AddPodInfo(pod)
	cache.pods[key] = pod
}

func (cache *nodeInfoCache) deletePod(pod *api.Pod, key string) {
	n := cache.nodes[pod.Spec.NodeName]
	n.SubPodInfo(pod)
	delete(cache.pods, key)
}

func (cache *nodeInfoCache) RemovePod(pod *api.Pod) error {
	key := podKeyFunc(pod)
	return cache.RemovePodByKey(key)
}

func (cache *nodeInfoCache) RemovePodByKey(key string) error {
	cache.mu.Lock()
	defer cache.mu.Unlock()

	state, ok := cache.podStates[key]
	switch {
	case ok && state == podExpired:
	case ok && state == podBinded:
		delete(cache.assumedPods, key)
		fallthrough
	case ok && state == podAdded:
		cache.deletePod(cache.pods[key], key)
	default:
		return fmt.Errorf("Pod state wasn't binded, expired, or added but get removed. Pod key: %v", key)
	}
	delete(cache.podStates, key)
	return nil
}

func (cache *nodeInfoCache) cleanupExpiredAssumedPods() {
	cache.cleanupAssumedPods(time.Now())
}

// separate this function out for time deterministic testability
func (cache *nodeInfoCache) cleanupAssumedPods(now time.Time) {
	cache.mu.Lock()
	defer cache.mu.Unlock()

	// the size of assumedPods should be small
	for _, aPod := range cache.assumedPods {
		if now.After(aPod.expectToExpire) {
			key := podKeyFunc(aPod.pod)
			err := cache.expirePod(key)
			if err != nil {
				glog.Errorf("cache.expirePod failed: %v", err)
			}
		}
	}
}

func (cache *nodeInfoCache) expirePod(key string) error {
	if state, ok := cache.podStates[key]; !ok || (state != podBinded) {
		return fmt.Errorf("Pod state wasn't binded but get expired. Pod key: %v", key)
	}
	delete(cache.assumedPods, key)
	cache.deletePod(cache.pods[key], key)
	cache.podStates[key] = podExpired
	return nil
}
