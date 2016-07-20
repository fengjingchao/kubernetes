/*
Copyright 2014 The Kubernetes Authors.

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

package registry

import (
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"k8s.io/kubernetes/pkg/api"
	kubeerr "k8s.io/kubernetes/pkg/api/errors"
	storeerr "k8s.io/kubernetes/pkg/api/errors/storage"
	"k8s.io/kubernetes/pkg/api/meta"
	"k8s.io/kubernetes/pkg/api/rest"
	"k8s.io/kubernetes/pkg/api/unversioned"
	"k8s.io/kubernetes/pkg/api/validation"
	"k8s.io/kubernetes/pkg/conversion"
	"k8s.io/kubernetes/pkg/fields"
	"k8s.io/kubernetes/pkg/labels"
	"k8s.io/kubernetes/pkg/registry/generic"
	"k8s.io/kubernetes/pkg/runtime"
	"k8s.io/kubernetes/pkg/storage"
	"k8s.io/kubernetes/pkg/storage/selector"
	"k8s.io/kubernetes/pkg/storage/versioner"
	utilruntime "k8s.io/kubernetes/pkg/util/runtime"
	"k8s.io/kubernetes/pkg/util/validation/field"
	"k8s.io/kubernetes/pkg/watch"

	"github.com/golang/glog"
	"golang.org/x/net/context"
)

// EnableGarbageCollector affects the handling of Update and Delete requests. It
// must be synced with the corresponding flag in kube-controller-manager.
var EnableGarbageCollector bool

// Store implements generic.Registry.
// It's intended to be embeddable, so that you can implement any
// non-generic functions if needed.
// You must supply a value for every field below before use; these are
// left public as it's meant to be overridable if need be.
// This object is intended to be copyable so that it can be used in
// different ways but share the same underlying behavior.
//
// The intended use of this type is embedding within a Kind specific
// RESTStorage implementation. This type provides CRUD semantics on
// a Kubelike resource, handling details like conflict detection with
// ResourceVersion and semantics. The RESTCreateStrategy and
// RESTUpdateStrategy are generic across all backends, and encapsulate
// logic specific to the API.
//
// TODO: make the default exposed methods exactly match a generic RESTStorage
type Store struct {
	// Called to make a new object, should return e.g., &api.Pod{}
	NewFunc func() runtime.Object

	// Called to make a new listing object, should return e.g., &api.PodList{}
	NewListFunc func() runtime.Object

	// Used for error reporting
	QualifiedResource unversioned.GroupResource

	// Used for listing/watching; should not include trailing "/"
	KeyRootFunc func(ctx api.Context) string

	// Called for Create/Update/Get/Delete. Note that 'namespace' can be
	// gotten from ctx.
	KeyFunc func(ctx api.Context, name string) (string, error)

	// Called to get the name of an object
	ObjectNameFunc func(obj runtime.Object) (string, error)

	// Return the TTL objects should be persisted with. Update is true if this
	// is an operation against an existing object. Existing is the current TTL
	// or the default for this operation.
	TTLFunc func(obj runtime.Object, existing uint64, update bool) (uint64, error)

	// Returns a matcher corresponding to the provided labels and fields.
	PredicateFunc func(label labels.Selector, field fields.Selector) generic.Matcher

	// DeleteCollectionWorkers is the maximum number of workers in a single
	// DeleteCollection call.
	DeleteCollectionWorkers int

	// Called on all objects returned from the underlying store, after
	// the exit hooks are invoked. Decorators are intended for integrations
	// that are above storage and should only be used for specific cases where
	// storage of the value is not appropriate, since they cannot
	// be watched.
	Decorator rest.ObjectFunc
	// Allows extended behavior during creation, required
	CreateStrategy rest.RESTCreateStrategy
	// On create of an object, attempt to run a further operation.
	AfterCreate rest.ObjectFunc
	// Allows extended behavior during updates, required
	UpdateStrategy rest.RESTUpdateStrategy
	// On update of an object, attempt to run a further operation.
	AfterUpdate rest.ObjectFunc
	// Allows extended behavior during updates, optional
	DeleteStrategy rest.RESTDeleteStrategy
	// On deletion of an object, attempt to run a further operation.
	AfterDelete rest.ObjectFunc
	// If true, return the object that was deleted. Otherwise, return a generic
	// success status response.
	ReturnDeletedObject bool
	// Allows extended behavior during export, optional
	ExportStrategy rest.RESTExportStrategy

	// Each registry should know how to parse its own type of object
	FVGetFunc selector.FieldValueGetFunc

	// Used for all storage access functions
	Storage storage.Interface
}

const OptimisticLockErrorMsg = "the object has been modified; please apply your changes to the latest version and try again"

// NamespaceKeyRootFunc is the default function for constructing storage paths to resource directories enforcing namespace rules.
func NamespaceKeyRootFunc(ctx api.Context, prefix string) string {
	key := prefix
	ns, ok := api.NamespaceFrom(ctx)
	if ok && len(ns) > 0 {
		key = key + "/" + ns
	}
	return key
}

// NamespaceKeyFunc is the default function for constructing storage paths to a resource relative to prefix enforcing namespace rules.
// If no namespace is on context, it errors.
func NamespaceKeyFunc(ctx api.Context, prefix string, name string) (string, error) {
	key := NamespaceKeyRootFunc(ctx, prefix)
	ns, ok := api.NamespaceFrom(ctx)
	if !ok || len(ns) == 0 {
		return "", kubeerr.NewBadRequest("Namespace parameter required.")
	}
	if len(name) == 0 {
		return "", kubeerr.NewBadRequest("Name parameter required.")
	}
	if msgs := validation.IsValidPathSegmentName(name); len(msgs) != 0 {
		return "", kubeerr.NewBadRequest(fmt.Sprintf("Name parameter invalid: %q: %s", name, strings.Join(msgs, ";")))
	}
	key = key + "/" + name
	return key, nil
}

// NoNamespaceKeyFunc is the default function for constructing storage paths to a resource relative to prefix without a namespace
func NoNamespaceKeyFunc(ctx api.Context, prefix string, name string) (string, error) {
	if len(name) == 0 {
		return "", kubeerr.NewBadRequest("Name parameter required.")
	}
	if msgs := validation.IsValidPathSegmentName(name); len(msgs) != 0 {
		return "", kubeerr.NewBadRequest(fmt.Sprintf("Name parameter invalid: %q: %s", name, strings.Join(msgs, ";")))
	}
	key := prefix + "/" + name
	return key, nil
}

// New implements RESTStorage
func (e *Store) New() runtime.Object {
	return e.NewFunc()
}

// NewList implements RESTLister
func (e *Store) NewList() runtime.Object {
	return e.NewListFunc()
}

// List returns a list of items matching labels and field
func (e *Store) List(ctx api.Context, options *api.ListOptions) (runtime.Object, error) {
	label := labels.Everything()
	if options != nil && options.LabelSelector != nil {
		label = options.LabelSelector
	}
	field := fields.Everything()
	if options != nil && options.FieldSelector != nil {
		field = options.FieldSelector
	}
	resourceVersion := "0"
	if options != nil {
		resourceVersion = options.ResourceVersion
	}

	version, err := storage.ParseListResourceVersion(resourceVersion)
	if err != nil {
		return nil, err
	}

	ss := append(
		e.convertLSToSS(label.ToSelectorFields(), MidFieldLabel),
		e.convertLSToSS(field.ToSelectorFields(), MidFieldField)...)
	for _, s := range ss {
		if s.Op == labels.NoneOperator {
			glog.Warningf("This is stupid. Why doing this? key: %s", e.KeyRootFunc(ctx)+"/")
			return e.NewListFunc(), nil
		}
	}
	objects, globalRev, err := e.Storage.List(e.KeyRootFunc(ctx)+"/", int64(version), ss...)
	if err != nil {
		return nil, err
	}
	return convertObjectsToListObject(objects, globalRev, e.NewListFunc())
	//return e.ListPredicate(ctx, e.PredicateFunc(label, field), options)
}

func convertObjectsToListObject(objects []runtime.Object, rev int64, listObj runtime.Object) (runtime.Object, error) {
	listPtr, err := meta.GetItemsPtr(listObj)
	if err != nil {
		return nil, err
	}
	v, err := conversion.EnforcePtr(listPtr)
	if err != nil || v.Kind() != reflect.Slice {
		panic("need ptr to slice")
	}
	for _, obj := range objects {
		v.Set(reflect.Append(v, reflect.ValueOf(obj).Elem()))
	}
	versioner.GlobalVersioner.UpdateList(listObj, uint64(rev))
	return listObj, nil
}

const (
	MidFieldLabel    = "label"
	MidFieldField    = "field"
	MidFieldLabelKey = "labelkey"
)

func (e *Store) convertLSToSS(lss []labels.SelectorFields, midField string) (ss []selector.Selector) {
	gvk, _, err := api.Scheme.ObjectKind(e.NewFunc())
	if err != nil {
		panic(fmt.Errorf("There is unknown object for this registry, resource: %v, err: %v", e.QualifiedResource, err))
	}
	kind := gvk.Kind

	for _, ls := range lss {
		if ls.Op == labels.NoneOperator {
			return []selector.Selector{
				{Op: labels.NoneOperator},
			}
		}

		if ls.Op == labels.ExistsOperator || ls.Op == labels.DoesNotExistOperator {
			if midField != MidFieldLabel {
				panic(fmt.Sprintf("unexpected midfield: %s", midField))
			}
			s := selector.Selector{
				Op: ls.Op,
				// We combine label key into the field.
				// FVGetFunc will parse it out and check if it exists in labels.
				Field:     combineFields(kind, MidFieldLabelKey, ls.Key),
				FVGetFunc: e.FVGetFunc,
			}
			ss = append(ss, s)
			continue
		}

		s := selector.Selector{
			Op:        ls.Op,
			Field:     combineFields(kind, midField, ls.Key),
			Values:    ls.Values, // We rely on validation on label side.
			FVGetFunc: e.FVGetFunc,
		}
		ss = append(ss, s)
	}
	return ss
}

func combineFields(kind, midField, key string) string {
	return strings.Join([]string{kind, midField, key}, ".")
}

// GetFVCommon will try to extract the value for given field.
// All FVGetFunc has the pattern to extract it from labels, fields.
// So we make this common routine.
// TODO: save creating fields map
func GetFVCommon(field string, ls, fs map[string]string) (string, bool) {
	s := strings.SplitN(field, ".", 3)
	if len(s) != 3 {
		glog.Warningf("Unexpected field: %v", field)
		return "", false
	}
	// assumption:
	// - If an object doesn't even have labels map created, we assume it doesn't have the key
	// - For fields, we assumes object always has the field (it's predefined).
	k := s[2]
	switch s[1] {
	case MidFieldLabel:
		if ls == nil {
			return "", false
		}
		v, ok := ls[k]
		return v, ok
	case MidFieldLabelKey:
		if ls == nil {
			return "", false
		}
		_, ok := ls[k]
		if !ok {
			return "", false
		}
		return k, true
	case MidFieldField:
		if fs == nil {
			return "", true
		}
		return fs[k], true
	default:
		glog.Warningf("Unexpected field: %v", field)
		return "", false
	}
}

// ListPredicate returns a list of all the items matching m.
//func (e *Store) ListPredicate(ctx api.Context, m generic.Matcher, options *api.ListOptions) (runtime.Object, error) {
//	list := e.NewListFunc()
//	filterFunc := e.filterAndDecorateFunction(m)
//	if name, ok := m.MatchesSingle(); ok {
//		if key, err := e.KeyFunc(ctx, name); err == nil {
//			err := e.Storage.GetToList(ctx, key, filterFunc, list)
//			return list, storeerr.InterpretListError(err, e.QualifiedResource)
//		}
//		// if we cannot extract a key based on the current context, the optimization is skipped
//	}
//
//	if options == nil {
//		options = &api.ListOptions{ResourceVersion: "0"}
//	}
//	err := e.Storage.List(ctx, e.KeyRootFunc(ctx), options.ResourceVersion, filterFunc, list)
//	return list, storeerr.InterpretListError(err, e.QualifiedResource)
//}

// Create inserts a new item according to the unique key from the object.
func (e *Store) Create(ctx api.Context, obj runtime.Object) (runtime.Object, error) {
	if err := rest.BeforeCreate(e.CreateStrategy, ctx, obj); err != nil {
		return nil, err
	}
	name, err := e.ObjectNameFunc(obj)
	if err != nil {
		return nil, err
	}
	key, err := e.KeyFunc(ctx, name)
	if err != nil {
		return nil, err
	}
	// TODO: Ignore ttl. We should rewrite event storage.
	//ttl, err := e.calculateTTL(obj, 0, false)
	//if err != nil {
	//	return nil, err
	//}

	//out := e.NewFunc()
	//err = e.Storage.Create(ctx, key, obj, out, ttl);
	out, err := e.create(name, key, obj)
	if err != nil {
		return nil, err
	}

	if e.AfterCreate != nil {
		if err := e.AfterCreate(out); err != nil {
			return nil, err
		}
	}
	if e.Decorator != nil {
		if err := e.Decorator(obj); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func (e *Store) create(name, key string, obj runtime.Object) (runtime.Object, error) {
	res, err := e.Storage.Put(key, obj, 0)
	if storage.IsTestFailed(err) {
		err = storage.NewKeyExistsError(key, 0)
	}
	if err != nil {
		err = storeerr.InterpretCreateError(err, e.QualifiedResource, name)
		err = rest.CheckGeneratedNameError(e.CreateStrategy, err, obj)
		return nil, err
	}
	return res, nil
}

// shouldDelete checks if a Update is removing all the object's finalizers. If so,
// it further checks if the object's DeletionGracePeriodSeconds is 0. If so, it
// returns true.
func (e *Store) shouldDelete(ctx api.Context, key string, obj, existing runtime.Object) bool {
	if !EnableGarbageCollector {
		return false
	}
	newMeta, err := api.ObjectMetaFor(obj)
	if err != nil {
		utilruntime.HandleError(err)
		return false
	}
	oldMeta, err := api.ObjectMetaFor(existing)
	if err != nil {
		utilruntime.HandleError(err)
		return false
	}
	return len(newMeta.Finalizers) == 0 && oldMeta.DeletionGracePeriodSeconds != nil && *oldMeta.DeletionGracePeriodSeconds == 0
}

func (e *Store) deleteForEmptyFinalizers(ctx api.Context, name, key string, obj runtime.Object, preconditions *storage.Preconditions) (runtime.Object, bool, error) {
	glog.V(6).Infof("going to delete %s from regitry, triggered by update", name)
	//out := e.NewFunc()
	//if err := e.Storage.Delete(ctx, key, out, preconditions); err != nil {
	out, err := e.delete(ctx, key, preconditions)
	if err != nil {
		// Deletion is racy, i.e., there could be multiple update
		// requests to remove all finalizers from the object, so we
		// ignore the NotFound error.
		if storage.IsNotFound(err) {
			_, err := e.finalizeDelete(obj, true)
			// clients are expecting an updated object if a PUT succeeded,
			// but finalizeDelete returns a unversioned.Status, so return
			// the object in the request instead.
			return obj, false, err
		}
		return nil, false, storeerr.InterpretDeleteError(err, e.QualifiedResource, name)
	}
	_, err = e.finalizeDelete(out, true)
	// clients are expecting an updated object if a PUT succeeded, but
	// finalizeDelete returns a unversioned.Status, so return the object in
	// the request instead.
	return obj, false, err
}

// Update performs an atomic update and set of the object. Returns the result of the update
// or an error. If the registry allows create-on-update, the create flow will be executed.
// A bool is returned along with the object and any errors, to indicate object creation.
func (e *Store) Update(ctx api.Context, name string, objInfo rest.UpdatedObjectInfo) (runtime.Object, bool, error) {
	key, err := e.KeyFunc(ctx, name)
	if err != nil {
		return nil, false, err
	}

	var (
		creatingObj runtime.Object
		creating    = false
	)

	storagePreconditions := &storage.Preconditions{}
	if preconditions := objInfo.Preconditions(); preconditions != nil {
		storagePreconditions.UID = preconditions.UID
	}

	// deleteObj is only used in case a deletion is carried out
	var deleteObj runtime.Object
	out, err := GuaranteedUpdate(e.Storage, key, e.NewFunc(), true, storagePreconditions, func(existing runtime.Object, res storage.ResponseMeta) (runtime.Object, *uint64, error) {
		// Given the existing object, get the new object
		obj, err := objInfo.UpdatedObject(ctx, existing)
		if err != nil {
			return nil, nil, err
		}

		// If AllowUnconditionalUpdate() is true and the object specified by the user does not have a resource version,
		// then we populate it with the latest version.
		// Else, we check that the version specified by the user matches the version of latest storage object.
		resourceVersion, err := versioner.GlobalVersioner.ObjectResourceVersion(obj)
		if err != nil {
			return nil, nil, err
		}
		doUnconditionalUpdate := resourceVersion == 0 && e.UpdateStrategy.AllowUnconditionalUpdate()

		version, err := versioner.GlobalVersioner.ObjectResourceVersion(existing)
		if err != nil {
			return nil, nil, err
		}
		if version == 0 {
			if !e.UpdateStrategy.AllowCreateOnUpdate() {
				return nil, nil, kubeerr.NewNotFound(e.QualifiedResource, name)
			}
			creating = true
			creatingObj = obj
			if err := rest.BeforeCreate(e.CreateStrategy, ctx, obj); err != nil {
				return nil, nil, err
			}
			ttl, err := e.calculateTTL(obj, 0, false)
			if err != nil {
				return nil, nil, err
			}
			return obj, &ttl, nil
		}

		creating = false
		creatingObj = nil
		if doUnconditionalUpdate {
			// Update the object's resource version to match the latest storage object's resource version.
			err = versioner.GlobalVersioner.UpdateObject(obj, res.ResourceVersion)
			if err != nil {
				return nil, nil, err
			}
		} else {
			// Check if the object's resource version matches the latest resource version.
			newVersion, err := versioner.GlobalVersioner.ObjectResourceVersion(obj)
			if err != nil {
				return nil, nil, err
			}
			if newVersion == 0 {
				// TODO: The Invalid error should has a field for Resource.
				// After that field is added, we should fill the Resource and
				// leave the Kind field empty. See the discussion in #18526.
				qualifiedKind := unversioned.GroupKind{Group: e.QualifiedResource.Group, Kind: e.QualifiedResource.Resource}
				fieldErrList := field.ErrorList{field.Invalid(field.NewPath("metadata").Child("resourceVersion"), newVersion, "must be specified for an update")}
				return nil, nil, kubeerr.NewInvalid(qualifiedKind, name, fieldErrList)
			}
			if newVersion != version {
				return nil, nil, kubeerr.NewConflict(e.QualifiedResource, name, fmt.Errorf(OptimisticLockErrorMsg))
			}
		}
		if err := rest.BeforeUpdate(e.UpdateStrategy, ctx, obj, existing); err != nil {
			return nil, nil, err
		}
		delete := e.shouldDelete(ctx, key, obj, existing)
		if delete {
			deleteObj = obj
			return nil, nil, errEmptiedFinalizers
		}
		ttl, err := e.calculateTTL(obj, res.TTL, true)
		if err != nil {
			return nil, nil, err
		}
		if int64(ttl) != res.TTL {
			return obj, &ttl, nil
		}
		return obj, nil, nil
	})

	if err != nil {
		// delete the object
		if err == errEmptiedFinalizers {
			return e.deleteForEmptyFinalizers(ctx, name, key, deleteObj, storagePreconditions)
		}
		if creating {
			err = storeerr.InterpretCreateError(err, e.QualifiedResource, name)
			err = rest.CheckGeneratedNameError(e.CreateStrategy, err, creatingObj)
		} else {
			err = storeerr.InterpretUpdateError(err, e.QualifiedResource, name)
		}
		return nil, false, err
	}
	if creating {
		if e.AfterCreate != nil {
			if err := e.AfterCreate(out); err != nil {
				return nil, false, err
			}
		}
	} else {
		if e.AfterUpdate != nil {
			if err := e.AfterUpdate(out); err != nil {
				return nil, false, err
			}
		}
	}
	if e.Decorator != nil {
		if err := e.Decorator(out); err != nil {
			return nil, false, err
		}
	}
	return out, creating, nil
}

// Get retrieves the item from storage.
func (e *Store) Get(ctx api.Context, name string) (runtime.Object, error) {
	key, err := e.KeyFunc(ctx, name)
	if err != nil {
		return nil, err
	}

	//obj := e.NewFunc()
	//if err := e.Storage.Get(ctx, key, obj, false); err != nil {
	//	return nil, storeerr.InterpretGetError(err, e.QualifiedResource, name)
	//}
	obj, err := Get(e.Storage, ctx, key, name, e.QualifiedResource, nil, false)
	if err != nil {
		return nil, err
	}

	if e.Decorator != nil {
		if err := e.Decorator(obj); err != nil {
			return nil, err
		}
	}
	return obj, nil
}

func Get(s storage.Interface, ctx api.Context, key, name string, resource unversioned.GroupResource, fresh runtime.Object, ignNotFound bool) (runtime.Object, error) {
	obj, err := s.Get(key)
	if obj == nil && err == nil {
		if ignNotFound {
			return fresh, nil
		}
		// Note: Compatibile with old get behavior which would return not found error
		err = storage.NewKeyNotFoundError(key, 0)
	}
	if err != nil {
		return nil, storeerr.InterpretGetError(err, resource, name)
	}
	return obj, nil
}

var (
	errAlreadyDeleting   = fmt.Errorf("abort delete")
	errDeleteNow         = fmt.Errorf("delete now")
	errEmptiedFinalizers = fmt.Errorf("emptied finalizers")
)

// return if we need to update the finalizers of the object, and the desired list of finalizers
func shouldUpdateFinalizers(accessor meta.Object, options *api.DeleteOptions) (shouldUpdate bool, newFinalizers []string) {
	if options == nil || options.OrphanDependents == nil {
		return false, accessor.GetFinalizers()
	}
	shouldOrphan := *options.OrphanDependents
	alreadyOrphan := false
	finalizers := accessor.GetFinalizers()
	newFinalizers = make([]string, 0, len(finalizers))
	for _, f := range finalizers {
		if f == api.FinalizerOrphan {
			alreadyOrphan = true
			if !shouldOrphan {
				continue
			}
		}
		newFinalizers = append(newFinalizers, f)
	}
	if shouldOrphan && !alreadyOrphan {
		newFinalizers = append(newFinalizers, api.FinalizerOrphan)
	}
	shouldUpdate = shouldOrphan != alreadyOrphan
	return shouldUpdate, newFinalizers
}

// markAsDeleting sets the obj's DeletionGracePeriodSeconds to 0, and sets the
// DeletionTimestamp to "now". Finalizers are watching for such updates and will
// finalize the object if their IDs are present in the object's Finalizers list.
func markAsDeleting(obj runtime.Object) (err error) {
	objectMeta, kerr := api.ObjectMetaFor(obj)
	if kerr != nil {
		return kerr
	}
	now := unversioned.NewTime(time.Now())
	// This handles Generation bump for resources that don't support graceful deletion. For resources that support graceful deletion is handle in pkg/api/rest/delete.go
	if objectMeta.DeletionTimestamp == nil && objectMeta.Generation > 0 {
		objectMeta.Generation++
	}
	objectMeta.DeletionTimestamp = &now
	var zero int64 = 0
	objectMeta.DeletionGracePeriodSeconds = &zero
	return nil
}

// this functions need to be kept synced with updateForGracefulDeletionAndFinalizers.
func (e *Store) updateForGracefulDeletion(ctx api.Context, name, key string, options *api.DeleteOptions, preconditions storage.Preconditions, in runtime.Object) (err error, ignoreNotFound, deleteImmediately bool, out, lastExisting runtime.Object) {
	lastGraceful := int64(0)
	//out = e.NewFunc()
	//err = e.Storage.GuaranteedUpdate(
	//	ctx, key, out, false, &preconditions,
	out, err = GuaranteedUpdate(
		e.Storage, key, e.NewFunc(), false, &preconditions,
		storage.SimpleUpdate(func(existing runtime.Object) (runtime.Object, error) {
			graceful, pendingGraceful, err := rest.BeforeDelete(e.DeleteStrategy, ctx, existing, options)
			if err != nil {
				return nil, err
			}
			if pendingGraceful {
				return nil, errAlreadyDeleting
			}
			if !graceful {
				return nil, errDeleteNow
			}
			lastGraceful = *options.GracePeriodSeconds
			lastExisting = existing
			return existing, nil
		}),
	)
	switch err {
	case nil:
		if lastGraceful > 0 {
			return nil, false, false, out, lastExisting
		}
		// If we are here, the registry supports grace period mechanism and
		// we are intentionally delete gracelessly. In this case, we may
		// enter a race with other k8s components. If other component wins
		// the race, the object will not be found, and we should tolerate
		// the NotFound error. See
		// https://github.com/kubernetes/kubernetes/issues/19403 for
		// details.
		return nil, true, true, out, lastExisting
	case errDeleteNow:
		// we've updated the object to have a zero grace period, or it's already at 0, so
		// we should fall through and truly delete the object.
		return nil, false, true, out, lastExisting
	case errAlreadyDeleting:
		out, err = e.finalizeDelete(in, true)
		return err, false, false, out, lastExisting
	default:
		return storeerr.InterpretUpdateError(err, e.QualifiedResource, name), false, false, out, lastExisting
	}
}

// this functions need to be kept synced with updateForGracefulDeletion.
func (e *Store) updateForGracefulDeletionAndFinalizers(ctx api.Context, name, key string, options *api.DeleteOptions, preconditions storage.Preconditions, in runtime.Object) (err error, ignoreNotFound, deleteImmediately bool, out, lastExisting runtime.Object) {
	lastGraceful := int64(0)
	var pendingFinalizers bool
	//out = e.NewFunc()
	//err = e.Storage.GuaranteedUpdate(
	//	ctx, key, out, false, &preconditions,
	out, err = GuaranteedUpdate(
		e.Storage, key, e.NewFunc(), false, &preconditions,
		storage.SimpleUpdate(func(existing runtime.Object) (runtime.Object, error) {
			graceful, pendingGraceful, err := rest.BeforeDelete(e.DeleteStrategy, ctx, existing, options)
			if err != nil {
				return nil, err
			}
			if pendingGraceful {
				return nil, errAlreadyDeleting
			}

			// Add/remove the orphan finalizer as the options dictates.
			// Note that this occurs after checking pendingGraceufl, so
			// finalizers cannot be updated via DeleteOptions if deletion has
			// started.
			existingAccessor, err := meta.Accessor(existing)
			if err != nil {
				return nil, err
			}
			shouldUpdate, newFinalizers := shouldUpdateFinalizers(existingAccessor, options)
			if shouldUpdate {
				existingAccessor.SetFinalizers(newFinalizers)
			}

			if !graceful {
				// set the DeleteGracePeriods to 0 if the object has pendingFinalizers but not supporting graceful deletion
				pendingFinalizers = len(existingAccessor.GetFinalizers()) != 0
				if pendingFinalizers {
					glog.V(6).Infof("update the DeletionTimestamp to \"now\" and GracePeriodSeconds to 0 for object %s, because it has pending finalizers", name)
					err = markAsDeleting(existing)
					if err != nil {
						return nil, err
					}
					return existing, nil
				}
				return nil, errDeleteNow
			}
			lastGraceful = *options.GracePeriodSeconds
			lastExisting = existing
			return existing, nil
		}),
	)
	switch err {
	case nil:
		// If there are pending finalizers, we never delete the object immediately.
		if pendingFinalizers {
			return nil, false, false, out, lastExisting
		}
		if lastGraceful > 0 {
			return nil, false, false, out, lastExisting
		}
		// If we are here, the registry supports grace period mechanism and
		// we are intentionally delete gracelessly. In this case, we may
		// enter a race with other k8s components. If other component wins
		// the race, the object will not be found, and we should tolerate
		// the NotFound error. See
		// https://github.com/kubernetes/kubernetes/issues/19403 for
		// details.
		return nil, true, true, out, lastExisting
	case errDeleteNow:
		// we've updated the object to have a zero grace period, or it's already at 0, so
		// we should fall through and truly delete the object.
		return nil, false, true, out, lastExisting
	case errAlreadyDeleting:
		out, err = e.finalizeDelete(in, true)
		return err, false, false, out, lastExisting
	default:
		return storeerr.InterpretUpdateError(err, e.QualifiedResource, name), false, false, out, lastExisting
	}
}

// Delete removes the item from storage.
func (e *Store) Delete(ctx api.Context, name string, options *api.DeleteOptions) (runtime.Object, error) {
	key, err := e.KeyFunc(ctx, name)
	if err != nil {
		return nil, err
	}

	obj, err := Get(e.Storage, ctx, key, name, e.QualifiedResource, nil, false)
	if err != nil {
		return nil, err
	}
	//obj := e.NewFunc()
	//if err := e.Storage.Get(ctx, key, obj, false); err != nil {
	//	return nil, storeerr.InterpretDeleteError(err, e.QualifiedResource, name)
	//}
	// support older consumers of delete by treating "nil" as delete immediately
	if options == nil {
		options = api.NewDeleteOptions(0)
	}
	var preconditions storage.Preconditions
	if options.Preconditions != nil {
		preconditions.UID = options.Preconditions.UID
	}
	graceful, pendingGraceful, err := rest.BeforeDelete(e.DeleteStrategy, ctx, obj, options)
	if err != nil {
		return nil, err
	}
	// this means finalizers cannot be updated via DeleteOptions if a deletion is already pending
	if pendingGraceful {
		return e.finalizeDelete(obj, false)
	}
	// check if obj has pending finalizers
	accessor, err := meta.Accessor(obj)
	if err != nil {
		return nil, kubeerr.NewInternalError(err)
	}
	pendingFinalizers := len(accessor.GetFinalizers()) != 0
	var ignoreNotFound bool
	var deleteImmediately bool = true
	var lastExisting, out runtime.Object
	if !EnableGarbageCollector {
		// TODO: remove the check on graceful, because we support no-op updates now.
		if graceful {
			err, ignoreNotFound, deleteImmediately, out, lastExisting = e.updateForGracefulDeletion(ctx, name, key, options, preconditions, obj)
		}
	} else {
		shouldUpdateFinalizers, _ := shouldUpdateFinalizers(accessor, options)
		// TODO: remove the check, because we support no-op updates now.
		if graceful || pendingFinalizers || shouldUpdateFinalizers {
			err, ignoreNotFound, deleteImmediately, out, lastExisting = e.updateForGracefulDeletionAndFinalizers(ctx, name, key, options, preconditions, obj)
		}
	}
	// !deleteImmediately covers all cases where err != nil. We keep both to be future-proof.
	if !deleteImmediately || err != nil {
		return out, err
	}

	// delete immediately, or no graceful deletion supported
	glog.V(6).Infof("going to delete %s from regitry: ", name)
	//out = e.NewFunc()
	//if err := e.Storage.Delete(ctx, key, out, &preconditions); err != nil {
	out, err = e.delete(ctx, key, &preconditions)
	if err != nil {
		// Please refer to the place where we set ignoreNotFound for the reason
		// why we ignore the NotFound error .
		if storage.IsNotFound(err) && ignoreNotFound && lastExisting != nil {
			// The lastExisting object may not be the last state of the object
			// before its deletion, but it's the best approximation.
			return e.finalizeDelete(lastExisting, true)
		}
		return nil, storeerr.InterpretDeleteError(err, e.QualifiedResource, name)
	}
	return e.finalizeDelete(out, true)
}

func (e *Store) delete(ctx api.Context, key string, c *storage.Preconditions) (runtime.Object, error) {
	if c == nil || c.UID == nil { // easy path..
		return e.Storage.Delete(key, -1)
	}

	out, err := e.Storage.Get(key)
	if err != nil {
		return nil, err
	}
	for {
		if out == nil {
			return nil, storage.NewKeyNotFoundError(key, 0)
		}

		if err := CheckPreconditions(key, c, out); err != nil {
			return nil, err
		}

		rev, err := versioner.GlobalVersioner.ObjectResourceVersion(out)
		if err != nil {
			return nil, err
		}

		out, err = e.Storage.Delete(key, int64(rev))
		if err == nil {
			return out, nil
		}
		if storage.IsTestFailed(err) {
			continue
		}
		return nil, err
	}
}

// DeleteCollection remove all items returned by List with a given ListOptions from storage.
//
// DeleteCollection is currently NOT atomic. It can happen that only subset of objects
// will be deleted from storage, and then an error will be returned.
// In case of success, the list of deleted objects will be returned.
//
// TODO: Currently, there is no easy way to remove 'directory' entry from storage (if we
// are removing all objects of a given type) with the current API (it's technically
// possibly with storage API, but watch is not delivered correctly then).
// It will be possible to fix it with v3 etcd API.
func (e *Store) DeleteCollection(ctx api.Context, options *api.DeleteOptions, listOptions *api.ListOptions) (runtime.Object, error) {
	listObj, err := e.List(ctx, listOptions)
	if err != nil {
		return nil, err
	}
	items, err := meta.ExtractList(listObj)
	if err != nil {
		return nil, err
	}
	// Spawn a number of goroutines, so that we can issue requests to storage
	// in parallel to speed up deletion.
	// TODO: Make this proportional to the number of items to delete, up to
	// DeleteCollectionWorkers (it doesn't make much sense to spawn 16
	// workers to delete 10 items).
	workersNumber := e.DeleteCollectionWorkers
	if workersNumber < 1 {
		workersNumber = 1
	}
	wg := sync.WaitGroup{}
	toProcess := make(chan int, 2*workersNumber)
	errs := make(chan error, workersNumber+1)

	go func() {
		defer utilruntime.HandleCrash(func(panicReason interface{}) {
			errs <- fmt.Errorf("DeleteCollection distributor panicked: %v", panicReason)
		})
		for i := 0; i < len(items); i++ {
			toProcess <- i
		}
		close(toProcess)
	}()

	wg.Add(workersNumber)
	for i := 0; i < workersNumber; i++ {
		go func() {
			// panics don't cross goroutine boundaries
			defer utilruntime.HandleCrash(func(panicReason interface{}) {
				errs <- fmt.Errorf("DeleteCollection goroutine panicked: %v", panicReason)
			})
			defer wg.Done()

			for {
				index, ok := <-toProcess
				if !ok {
					return
				}
				accessor, err := meta.Accessor(items[index])
				if err != nil {
					errs <- err
					return
				}
				if _, err := e.Delete(ctx, accessor.GetName(), options); err != nil && !kubeerr.IsNotFound(err) {
					glog.V(4).Infof("Delete %s in DeleteCollection failed: %v", accessor.GetName(), err)
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	select {
	case err := <-errs:
		return nil, err
	default:
		return listObj, nil
	}
}

func (e *Store) finalizeDelete(obj runtime.Object, runHooks bool) (runtime.Object, error) {
	if runHooks && e.AfterDelete != nil {
		if err := e.AfterDelete(obj); err != nil {
			return nil, err
		}
	}
	if e.ReturnDeletedObject {
		if e.Decorator != nil {
			if err := e.Decorator(obj); err != nil {
				return nil, err
			}
		}
		return obj, nil
	}
	return &unversioned.Status{Status: unversioned.StatusSuccess}, nil
}

// Watch makes a matcher for the given label and field, and calls
// WatchPredicate. If possible, you should customize PredicateFunc to produre a
// matcher that matches by key. generic.SelectionPredicate does this for you
// automatically.
func (e *Store) Watch(ctx api.Context, options *api.ListOptions) (watch.Interface, error) {
	label := labels.Everything()
	if options != nil && options.LabelSelector != nil {
		label = options.LabelSelector
	}
	field := fields.Everything()
	if options != nil && options.FieldSelector != nil {
		field = options.FieldSelector
	}
	resourceVersion := ""
	if options != nil {
		resourceVersion = options.ResourceVersion
	}

	key := e.KeyRootFunc(ctx) + "/"
	ss := append(
		e.convertLSToSS(label.ToSelectorFields(), MidFieldLabel),
		e.convertLSToSS(field.ToSelectorFields(), MidFieldField)...)

	for _, s := range ss {
		if s.Op == labels.NoneOperator {
			glog.Warningf("This is stupid. Why doing this? key: %s", key)
			return noResultWatcher{}, nil
		}
	}

	ver, err := storage.ParseWatchResourceVersion(resourceVersion)
	if err != nil {
		return nil, err
	}
	rev := int64(ver)
	var initialObjects []runtime.Object
	if ver == 0 {
		// All initial events will be ADDED.
		initialObjects, rev, err = e.Storage.List(key, 0, ss...)
		if err != nil {
			return nil, err
		}
	}

	w, err := e.Storage.WatchPrefix(ctx, key, rev, ss...)
	if err != nil {
		return nil, err
	}
	return newWatcher(ctx, initialObjects, w, ss), nil
	//return e.WatchPredicate(ctx, e.PredicateFunc(label, field), resourceVersion)
}

// Stupid hack for Nothing selector.
type noResultWatcher struct{}

func (w noResultWatcher) Stop()                          {}
func (w noResultWatcher) ResultChan() <-chan watch.Event { return nil }

func newWatcher(apiCtx api.Context, initialObjects []runtime.Object, wch storage.WatchChan, ss []selector.Selector) watch.Interface {
	ctx, cancel := context.WithCancel(apiCtx)
	w := &watcher{
		cancel: cancel,
		wch:    wch,
		// This is a consumer, we don't buffer here. All buffering should be handled/managed by storage system.
		resultCh: make(chan watch.Event),
		ss:       ss,
	}
	go w.run(ctx, initialObjects)
	return w
}

type watcher struct {
	cancel   context.CancelFunc
	wch      storage.WatchChan
	resultCh chan watch.Event
	ss       []selector.Selector
}

func (w *watcher) run(ctx context.Context, initialObjects []runtime.Object) {
	for _, o := range initialObjects {
		e := watch.Event{
			Type:   watch.Added,
			Object: o,
		}
		select {
		case w.resultCh <- e:
		case <-ctx.Done():
			return
		}
	}
	for r := range w.wch {
		if r.Type == watch.Error {
			glog.Errorf("watcher stopped with err: %v", r.Err)
			return
		}
		res := transform(r, w.ss)
		if res == nil {
			continue
		}
		select {
		case w.resultCh <- *res:
		case <-ctx.Done():
			return
		}
	}
}

func (w *watcher) Stop() {
	w.cancel()
}
func (w *watcher) ResultChan() <-chan watch.Event {
	return w.resultCh
}

func transform(r storage.WatchResponse, ss []selector.Selector) (res *watch.Event) {
	switch r.Type {
	case watch.Deleted:
		if !selector.IsSelected(r.PrevObject, ss) {
			return nil
		}
		res = &watch.Event{
			Type:   watch.Deleted,
			Object: r.PrevObject,
		}
	case watch.Added:
		if !selector.IsSelected(r.Object, ss) {
			return nil
		}
		res = &watch.Event{
			Type:   watch.Added,
			Object: r.Object,
		}
	case watch.Modified:
		curObjPasses := selector.IsSelected(r.Object, ss)
		oldObjPasses := selector.IsSelected(r.PrevObject, ss)
		switch {
		case curObjPasses && oldObjPasses:
			res = &watch.Event{
				Type:   watch.Modified,
				Object: r.Object,
			}
		case curObjPasses && !oldObjPasses:
			res = &watch.Event{
				Type:   watch.Added,
				Object: r.Object,
			}
		case !curObjPasses && oldObjPasses:
			res = &watch.Event{
				Type:   watch.Deleted,
				Object: r.PrevObject,
			}
		}
	default:
		panic("")
	}
	return res
}

// WatchPredicate starts a watch for the items that m matches.
//func (e *Store) WatchPredicate(ctx api.Context, m generic.Matcher, resourceVersion string) (watch.Interface, error) {
//	filterFunc := e.filterAndDecorateFunction(m)
//	if name, ok := m.MatchesSingle(); ok {
//		if key, err := e.KeyFunc(ctx, name); err == nil {
//			if err != nil {
//				return nil, err
//			}
//			return e.Storage.Watch(ctx, key, resourceVersion, filterFunc)
//		}
//		// if we cannot extract a key based on the current context, the optimization is skipped
//	}
//	return e.Storage.WatchList(ctx, e.KeyRootFunc(ctx), resourceVersion, filterFunc)
//}

func (e *Store) createFilter(m generic.Matcher) storage.Filter {
	filterFunc := func(obj runtime.Object) bool {
		matches, err := m.Matches(obj)
		if err != nil {
			glog.Errorf("unable to match watch: %v", err)
			return false
		}
		if matches && e.Decorator != nil {
			if err := e.Decorator(obj); err != nil {
				glog.Errorf("unable to decorate watch: %v", err)
				return false
			}
		}
		return matches
	}
	return storage.NewSimpleFilter(filterFunc, m.MatcherIndex)
}

// calculateTTL is a helper for retrieving the updated TTL for an object or returning an error
// if the TTL cannot be calculated. The defaultTTL is changed to 1 if less than zero. Zero means
// no TTL, not expire immediately.
func (e *Store) calculateTTL(obj runtime.Object, defaultTTL int64, update bool) (ttl uint64, err error) {
	// TODO: validate this is assertion is still valid.
	// etcd may return a negative TTL for a node if the expiration has not occurred due
	// to server lag - we will ensure that the value is at least set.
	if defaultTTL < 0 {
		defaultTTL = 1
	}
	ttl = uint64(defaultTTL)
	if e.TTLFunc != nil {
		ttl, err = e.TTLFunc(obj, ttl, update)
	}
	return ttl, err
}

func exportObjectMeta(accessor meta.Object, exact bool) {
	accessor.SetUID("")
	if !exact {
		accessor.SetNamespace("")
	}
	accessor.SetCreationTimestamp(unversioned.Time{})
	accessor.SetDeletionTimestamp(nil)
	accessor.SetResourceVersion("")
	accessor.SetSelfLink("")
	if len(accessor.GetGenerateName()) > 0 && !exact {
		accessor.SetName("")
	}
}

// Implements the rest.Exporter interface
func (e *Store) Export(ctx api.Context, name string, opts unversioned.ExportOptions) (runtime.Object, error) {
	obj, err := e.Get(ctx, name)
	if err != nil {
		return nil, err
	}
	if accessor, err := meta.Accessor(obj); err == nil {
		exportObjectMeta(accessor, opts.Exact)
	} else {
		glog.V(4).Infof("Object of type %v does not have ObjectMeta: %v", reflect.TypeOf(obj), err)
	}

	if e.ExportStrategy != nil {
		if err = e.ExportStrategy.Export(obj, opts.Exact); err != nil {
			return nil, err
		}
	} else {
		e.CreateStrategy.PrepareForCreate(obj)
	}
	return obj, nil
}

// Note: The precondition is duplicate to updatefunc unfortunately..
// NOTE!!: This code is very broken. We should try to simplify it getting rid of precondition, ignNotFound and so on.
func GuaranteedUpdate(s storage.Interface, key string, fresh runtime.Object, ignNotFound bool, c *storage.Preconditions,
	uf storage.UpdateFunc) (runtime.Object, error) {

	out, err := s.Get(key)
	if err != nil {
		return nil, err
	}
	for {
		if out == nil && !ignNotFound {
			return nil, storage.NewKeyNotFoundError(key, 0)
		}

		// setup ResponseMeta. Very stupid:
		// - Parsing between string and uint64 a lot.
		// - if no object exists, get fresh one and make version in ResponseMeta to be 0.
		rm := storage.ResponseMeta{}
		rev := uint64(0)
		if out != nil {
			rev, err = versioner.GlobalVersioner.ObjectResourceVersion(out)
			if err != nil {
				return nil, err
			}
			rm.ResourceVersion = rev
		} else {
			out = fresh
		}

		// check precondition...
		// This is stupid. Why not check this also in user update func?
		if c != nil && c.UID != nil {
			if err := CheckPreconditions(key, c, out); err != nil {
				return nil, err
			}
		}

		// user update func. The only useful thing. User should put checkPrecondition into here.
		uo, _, err := uf(out, rm)
		if err != nil {
			return nil, err
		}

		out, err = s.Put(key, uo, int64(rev))
		if err == nil {
			return out, nil
		}
		if storage.IsTestFailed(err) {
			continue
		}
		return nil, err
	}
}

func CheckPreconditions(key string, preconditions *storage.Preconditions, out runtime.Object) error {
	objMeta, err := api.ObjectMetaFor(out)
	if err != nil {
		return storage.NewInternalErrorf("can't enforce preconditions %v on un-introspectable object %v, got error: %v", *preconditions, out, err)
	}
	if *preconditions.UID != objMeta.UID {
		errMsg := fmt.Sprintf("Precondition failed: UID in precondition: %v, UID in object meta: %v", preconditions.UID, objMeta.UID)
		return storage.NewInvalidObjError(key, errMsg)
	}
	return nil
}
