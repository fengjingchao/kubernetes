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

package framework

import (
	"fmt"
	"sync"
	"time"

	"k8s.io/kubernetes/pkg/api"
	"k8s.io/kubernetes/pkg/client/cache"
	"k8s.io/kubernetes/pkg/runtime"
	utilruntime "k8s.io/kubernetes/pkg/util/runtime"
	"k8s.io/kubernetes/pkg/util/sets"
	"k8s.io/kubernetes/pkg/util/wait"

	"github.com/golang/glog"
)

// Controller...
type Controller struct {
	// User provided functions to list and watch objects.
	lw cache.ListerWatcher
	// Iterate everything again at least this often. Objects will be given at UpdateFunc.
	resyncPeriod time.Duration
	// store reflects the latest state of the remote storage.
	store cache.Indexer
	// User registered handlers.
	handlers []ResourceEventHandler
	// mu protects all internal data structures
	mu sync.Mutex
	// populated tells whether the store has been populated with data.
	populated bool
	// started tells whether controller has been started.
	started bool
	// internal channels.
	eventChan chan *ctrlEvent
	queueChan chan *ctrlEvent
}

// TODO make the "Controller" private, and convert all references to use ControllerInterface instead
type ControllerInterface interface {
	Run(stopCh <-chan struct{})
	HasSynced() bool
}

// New makes a new Controller.
func New(lw cache.ListerWatcher, resyncPeriod time.Duration, store cache.Indexer) *Controller {
	return &Controller{
		lw:           lw,
		resyncPeriod: resyncPeriod,
		eventChan:    make(chan *ctrlEvent),
		queueChan:    make(chan *ctrlEvent),
		store:        store,
	}
}

// Run starts controller to keep syncing with remote storage until stopCh returns.
// It's an error to call Run more than once.
// Run blocks.
func (c *Controller) Run(stopCh <-chan struct{}) {
	c.mu.Lock()
	c.started = true
	c.mu.Unlock()

	go wait.Until(func() {
		if err := c.listWatch(stopCh); err != nil {
			utilruntime.HandleError(err)
		}
		// TODO: Retry policy at relisting?
	}, time.Second, stopCh)

	go c.eventQueuing(stopCh)
	if c.resyncPeriod != 0 {
		go c.resyncPeriodically(stopCh)
	}

	if err := c.eventProcessing(stopCh); err != nil {
		// Other option:
		// - backoff and reprocess it. But on what situations?
		panic(err)
	}
}

// watch ...
// - never stop watching on client side.
// - never relist unless version is compacted.
func (c *Controller) listWatch(stopCh <-chan struct{}) error {
	watchRev, err := c.listItems(stopCh)
	if err != nil {
		return err
	}

	for {
		// TODO: retry policy on rewatch?
		w, err := c.lw.Watch(api.ListOptions{ResourceVersion: watchRev})
		if err != nil {
			if isVersionCompactedErr(err) {
				glog.Warningf("version (%v) compacted. Will do relist. It might miss middle events.", watchRev)
				return nil
			}
			glog.Warningf("Watch failed: %+v.\nWill rewatch on rev (%v)", err, watchRev)
			continue
		}

		select {
		case r, ok := <-w.ResultChan():
			if !ok {
				glog.Warningf("watch channel is closed. Will re-watch on rev (%v)", watchRev)
				break
			}
			c.queueEvent(&ctrlEvent{
				eventType: ctrlEventType(r.Type),
				obj:       r.Object,
			}, stopCh)
			watchRev, err = getResourceVersion(r.Object)
			if err != nil {
				return err
			}
		case <-stopCh:
			return nil
		}
	}
}

func (c *Controller) listItems(stopCh <-chan struct{}) (string, error) {
	options := api.ListOptions{ResourceVersion: "0"}
	items, rev, err := listAndConvert(c.lw, options)
	if err != nil {
		return "", err
	}
	c.queueEvent(&ctrlEvent{
		eventType: eventTypeRelist,
		items:     items,
	}, stopCh)
	return rev, nil
}

func (c *Controller) eventQueuing(stopCh <-chan struct{}) {
	pending := make([]*ctrlEvent, 0)
	var eventCh chan *ctrlEvent
	var ev *ctrlEvent
	for {
		if len(pending) > 0 {
			ev = pending[0]
			eventCh = c.eventChan
		} else {
			eventCh = nil
		}

		select {
		case msg := <-c.queueChan:
			pending = append(pending, msg)
		case eventCh <- ev:
			pending = pending[1:]
		case <-stopCh:
		}
	}
}

// All updates to store goes here. Serialize all operations.
func (c *Controller) eventProcessing(stopCh <-chan struct{}) error {
	for {
		select {
		case ev := <-c.eventChan:
			if err := c.handle(ev); err != nil {
				return err
			}
		case <-stopCh:
		}
	}
}

func (c *Controller) handle(ev *ctrlEvent) error {
	switch ev.eventType {
	case eventTypeRelist:
		if err := c.diffSync(ev.items); err != nil {
			return err
		}
	case eventTypeAdd:
		c.handleAdd(ev.obj)
	case eventTypeModify:
		key, err := cache.MetaNamespaceKeyFunc(ev.obj)
		if err != nil {
			return err
		}
		oldItem, _, err := c.store.GetByKey(key)
		if err != nil {
			return err
		}
		c.handleUpdate(oldItem, ev.obj)
	case eventTypeDelete:
		c.handleDelete(ev.obj)
	case eventTypeResync:
		c.iterateStoreItems()
	}
	return nil
}

func (c *Controller) queueEvent(ev *ctrlEvent, stopCh <-chan struct{}) {
	select {
	case c.queueChan <- ev:
	case <-stopCh:
	}
}

func (c *Controller) diffSync(newItems []runtime.Object) error {
	// Definitions:
	// - newItems: the items listed from remote store.
	// - oldItems: the items that existed in local cache.
	// Actions based on conditions:
	// - Add item that exist in newItems but not in oldItems.
	// - Update item that exist in both newItems not oldItems.
	// - Delete item that exist in oldItems but not newItems.
	// Note:
	// - Update doesn't take in account no revision change. Two reasons: Resync and backward compability.
	newItemKeys := make(sets.String, len(newItems))
	for _, newItem := range newItems {
		key, err := cache.MetaNamespaceKeyFunc(newItem)
		if err != nil {
			return err
		}
		newItemKeys.Insert(key)
		oldItem, exists, err := c.store.GetByKey(key)
		if err != nil {
			return err
		}
		if !exists {
			c.handleAdd(newItem)
			continue
		}
		c.handleUpdate(oldItem, newItem)
	}

	oldItemKeys := c.store.ListKeys()
	for _, key := range oldItemKeys {
		if !newItemKeys.Has(key) {
			oldItem, _, err := c.store.GetByKey(key)
			if err != nil {
				return err
			}
			c.handleDelete(cache.DeletedFinalStateUnknown{
				Key: key,
				Obj: oldItem,
			})
		}
	}
	c.mu.Lock()
	c.populated = true
	c.mu.Unlock()
	return nil
}

func (c *Controller) handleAdd(item interface{}) {
	c.store.Add(item)
	for _, handler := range c.handlers {
		handler.OnAdd(item)
	}
}

func (c *Controller) handleUpdate(oldItem, newItem interface{}) {
	c.store.Update(newItem)
	for _, handler := range c.handlers {
		handler.OnUpdate(oldItem, newItem)
	}
}

func (c *Controller) handleDelete(item interface{}) {
	c.store.Delete(item)
	for _, handler := range c.handlers {
		handler.OnDelete(item)
	}
}

func (c *Controller) iterateStoreItems() {
	for _, item := range c.store.List() {
		c.handleUpdate(item, item)
	}
}

// Returns true once this controller has completed an initial resource listing
func (c *Controller) HasSynced() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.populated
}

func (c *Controller) resyncPeriodically(stopCh <-chan struct{}) {
	for {
		select {
		case <-time.After(c.resyncPeriod):
			c.queueEvent(&ctrlEvent{
				eventType: eventTypeResync,
			}, stopCh)
		case <-stopCh:
		}
	}
}

// Requeue adds the provided object back into the queue if it does not already exist.
func (c *Controller) Requeue(obj interface{}) error {
	// Race of old implementation:
	// - Requeue would add an item back if it's been deleted. But requeue is just trying to reprocess it.
	return nil
}

func (c *Controller) addEventHandler(handler ResourceEventHandler) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.started {
		return fmt.Errorf("controller has already started")
	}

	c.handlers = append(c.handlers, handler)
	return nil
}

// ResourceEventHandler can handle notifications for events that happen to a
// resource.  The events are informational only, so you can't return an
// error.
//  * OnAdd is called when an object is added.
//  * OnUpdate is called when an object is modified. Note that oldObj is the
//      last known state of the object-- it is possible that several changes
//      were combined together, so you can't use this to see every single
//      change. OnUpdate is also called when a re-list happens, and it will
//      get called even if nothing changed. This is useful for periodically
//      evaluating or syncing something.
//  * OnDelete will get the final state of the item if it is known, otherwise
//      it will get an object of type cache.DeletedFinalStateUnknown. This can
//      happen if the watch is closed and misses the delete event and we don't
//      notice the deletion until the subsequent re-list.
type ResourceEventHandler interface {
	OnAdd(obj interface{})
	OnUpdate(oldObj, newObj interface{})
	OnDelete(obj interface{})
}

// ResourceEventHandlerFuncs is an adaptor to let you easily specify as many or
// as few of the notification functions as you want while still implementing
// ResourceEventHandler.
type ResourceEventHandlerFuncs struct {
	AddFunc    func(obj interface{})
	UpdateFunc func(oldObj, newObj interface{})
	DeleteFunc func(obj interface{})
}

// OnAdd calls AddFunc if it's not nil.
func (r ResourceEventHandlerFuncs) OnAdd(obj interface{}) {
	if r.AddFunc != nil {
		r.AddFunc(obj)
	}
}

// OnUpdate calls UpdateFunc if it's not nil.
func (r ResourceEventHandlerFuncs) OnUpdate(oldObj, newObj interface{}) {
	if r.UpdateFunc != nil {
		r.UpdateFunc(oldObj, newObj)
	}
}

// OnDelete calls DeleteFunc if it's not nil.
func (r ResourceEventHandlerFuncs) OnDelete(obj interface{}) {
	if r.DeleteFunc != nil {
		r.DeleteFunc(obj)
	}
}

// DeletionHandlingMetaNamespaceKeyFunc checks for
// cache.DeletedFinalStateUnknown objects before calling
// cache.MetaNamespaceKeyFunc.
func DeletionHandlingMetaNamespaceKeyFunc(obj interface{}) (string, error) {
	if d, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		return d.Key, nil
	}
	return cache.MetaNamespaceKeyFunc(obj)
}

// NewInformer returns a cache.Store and a controller for populating the store
// while also providing event notifications. You should only used the returned
// cache.Store for Get/List operations; Add/Modify/Deletes will cause the event
// notifications to be faulty.
//
// Parameters:
//  * lw is list and watch functions for the source of the resource you want to
//    be informed of.
//  * objType is an object of the type that you expect to receive.
//  * resyncPeriod: if non-zero, will re-list this often (you will get OnUpdate
//    calls, even if nothing changed). Otherwise, re-list will be delayed as
//    long as possible (until the upstream source closes the watch or times out,
//    or you stop the controller).
//  * h is the object you want notifications sent to.
func NewInformer(
	lw cache.ListerWatcher,
	objType runtime.Object,
	resyncPeriod time.Duration,
	h ResourceEventHandler,
) (cache.Store, *Controller) {
	informer := newCoreInformer(lw, objType, resyncPeriod, cache.Indexers{})
	informer.addEventHandler(h)
	return informer.GetIndexer(), informer.Controller
}

// NewIndexerInformer returns a cache.Indexer and a controller for populating the index
// while also providing event notifications. You should only used the returned
// cache.Index for Get/List operations; Add/Modify/Deletes will cause the event
// notifications to be faulty.
//
// Parameters:
//  * lw is list and watch functions for the source of the resource you want to
//    be informed of.
//  * objType is an object of the type that you expect to receive.
//  * resyncPeriod: if non-zero, will re-list this often (you will get OnUpdate
//    calls, even if nothing changed). Otherwise, re-list will be delayed as
//    long as possible (until the upstream source closes the watch or times out,
//    or you stop the controller).
//  * h is the object you want notifications sent to.
func NewIndexerInformer(
	lw cache.ListerWatcher,
	objType runtime.Object,
	resyncPeriod time.Duration,
	h ResourceEventHandler,
	indexers cache.Indexers,
) (cache.Indexer, *Controller) {
	informer := newCoreInformer(lw, objType, resyncPeriod, indexers)
	informer.addEventHandler(h)
	return informer.GetIndexer(), informer.Controller
}
