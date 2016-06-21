/*
Copyright 2016 The Kubernetes Authors All rights reserved.

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

package framework

import (
	"fmt"
	"time"

	"k8s.io/kubernetes/pkg/client/cache"
	"k8s.io/kubernetes/pkg/runtime"
)

type coreInformer struct {
	*Controller
}

func newCoreInformer(lw cache.ListerWatcher, objType runtime.Object, resyncPeriod time.Duration, indexers cache.Indexers) *coreInformer {
	indexer := cache.NewIndexer(DeletionHandlingMetaNamespaceKeyFunc, indexers)
	informer := &coreInformer{
		Controller: New(lw, resyncPeriod, indexer),
	}
	return informer
}

func (c *coreInformer) GetIndexer() cache.Indexer {
	return c.store
}

func (c *coreInformer) AddIndexers(indexers cache.Indexers) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.started {
		return fmt.Errorf("informer has already started")
	}

	return c.store.AddIndexers(indexers)
}
