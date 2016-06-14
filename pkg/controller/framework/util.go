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
	"net/http"

	"k8s.io/kubernetes/pkg/api"
	apierrors "k8s.io/kubernetes/pkg/api/errors"
	"k8s.io/kubernetes/pkg/api/meta"
	"k8s.io/kubernetes/pkg/client/cache"
	"k8s.io/kubernetes/pkg/runtime"
)

func listAndConvert(lw cache.ListerWatcher, options api.ListOptions) ([]runtime.Object, string, error) {
	list, err := lw.List(options)
	if err != nil {
		return nil, "", err
	}
	listMetaInterface, err := meta.ListAccessor(list)
	if err != nil {
		return nil, "", err
	}
	resourceVersion := listMetaInterface.GetResourceVersion()
	items, err := meta.ExtractList(list)
	if err != nil {
		return nil, "", err
	}
	return items, resourceVersion, nil
}

func isVersionCompactedErr(err error) bool {
	se, ok := err.(*apierrors.StatusError)
	if !ok {
		return false
	}
	// See pkg/api/errors.NewGone()
	return se.ErrStatus.Code == http.StatusGone
}

func getResourceVersion(obj runtime.Object) (string, error) {
	meta, err := meta.Accessor(obj)
	if err != nil {
		return "", err
	}
	return meta.GetResourceVersion(), nil
}
