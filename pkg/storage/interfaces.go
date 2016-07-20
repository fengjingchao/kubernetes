/*
Copyright 2015 The Kubernetes Authors.

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

package storage

import (
	"golang.org/x/net/context"
	"k8s.io/kubernetes/pkg/runtime"
	"k8s.io/kubernetes/pkg/storage/selector"
	"k8s.io/kubernetes/pkg/types"
	"k8s.io/kubernetes/pkg/watch"
)

// Versioner abstracts setting and retrieving metadata fields from database response
// onto the object ot list.
type Versioner interface {
	// UpdateObject sets storage metadata into an API object. Returns an error if the object
	// cannot be updated correctly. May return nil if the requested object does not need metadata
	// from database.
	UpdateObject(obj runtime.Object, resourceVersion uint64) error
	// UpdateList sets the resource version into an API list object. Returns an error if the object
	// cannot be updated correctly. May return nil if the requested object does not need metadata
	// from database.
	UpdateList(obj runtime.Object, resourceVersion uint64) error
	// ObjectResourceVersion returns the resource version (for persistence) of the specified object.
	// Should return an error if the specified object does not have a persistable version.
	ObjectResourceVersion(obj runtime.Object) (uint64, error)
}

// ResponseMeta contains information about the database metadata that is associated with
// an object. It abstracts the actual underlying objects to prevent coupling with concrete
// database and to improve testability.
type ResponseMeta struct {
	// TTL is the time to live of the node that contained the returned object. It may be
	// zero or negative in some cases (objects may be expired after the requested
	// expiration time due to server lag).
	TTL int64
	// The resource version of the node that contained the returned object.
	ResourceVersion uint64
}

// MatchValue defines a pair (<index name>, <value for that index>).
type MatchValue struct {
	IndexName string
	Value     string
}

// TriggerPublisherFunc is a function that takes an object, and returns a list of pairs
// (<index name>, <index value for the given object>) for all indexes known
// to that function.
type TriggerPublisherFunc func(obj runtime.Object) []MatchValue

// Filter is interface that is used to pass filtering mechanism.
type Filter interface {
	// Filter is a predicate which takes an API object and returns true
	// if and only if the object should remain in the set.
	Filter(obj runtime.Object) bool
	// For any triggers known to the Filter, if Filter() can return only
	// (a subset of) objects for which indexing function returns <value>,
	// (<index name>, <value> pair would be returned.
	//
	// This is optimization to avoid computing Filter() function (which are
	// usually relatively expensive) in case we are sure they will return
	// false anyway.
	Trigger() []MatchValue
}

// Everything is a Filter which accepts all objects.
var Everything Filter = everything{}

// everything is implementation of Everything.
type everything struct {
}

func (e everything) Filter(runtime.Object) bool {
	return true
}

func (e everything) Trigger() []MatchValue {
	return nil
}

// Pass an UpdateFunc to Interface.GuaranteedUpdate to make an update
// that is guaranteed to succeed.
// See the comment for GuaranteedUpdate for more details.
type UpdateFunc func(input runtime.Object, res ResponseMeta) (output runtime.Object, ttl *uint64, err error)

// Preconditions must be fulfilled before an operation (update, delete, etc.) is carried out.
type Preconditions struct {
	// Specifies the target UID.
	UID *types.UID `json:"uid,omitempty"`
}

// NewUIDPreconditions returns a Preconditions with UID set.
func NewUIDPreconditions(uid string) *Preconditions {
	u := types.UID(uid)
	return &Preconditions{UID: &u}
}

// ===========================================================================
//
//     quu..__
//      $$$b  `---.__
//       "$$b        `--.                          ___.---uuudP
//        `$$b           `.__.------.__     __.---'      $$$$"              .
//          "$b          -' _          `-.-'            $$$"              .'|
//            ".           | |                         d$"             _.'  |
//              `.   /     | |                      ..."             .'     |
//                `./      | |___               ..::-'            _.'       |
//                 /       |_____|           .:::-'            .-'         .'
//                :                          ::''\          _.'            |
//               .' .-.             .-.           `.      .'               |
//               : /'$$|           .@"$\           `.   .'              _.-'
//              .'|$u$$|          |$$,$$|           |  <            _.-'
//              | `:$$:'          :$$$$$:           `.  `.       .-'
//              :                  `"--'             |    `-.     \
//             :##.       ==             .###.       `.      `.    `\
//             |##:                      :###:        |        >     >
//             |#'     `..'`..'          `###'        x:      /     /
//              \                                   xXX|     /    ./
//               \                                xXXX'|    /   ./
//               /`-.                                  `.  /   /
//              :    `-  ...........,                   | /  .'
//              |         ``:::::::'       .            |<    `.
//              |             ```          |           x| \ `.:``.
//              |                         .'    /'   xXX|  `:`M`M':.
//              |    |                    ;    /:' xXXX'|  -'MMMMM:'
//              `.  .'                   :    /:'       |-'MMMM.-'
//               |  |                   .'   /'        .'MMM.-'
//               `'`'                   :  ,'          |MMM<
//                 |                     `'            |tbap\
//                  \                                  :MM.-'
//                   \                 |              .''
//                    \.               `.            /
//                     /     .:::::::.. :           /
//                    |     .:::::::::::`.         /
//                    |   .:::------------\       /
//                   /   .''               >::'  /
//                   `',:                 :    .'
//                                        `:.:'  Pikachu!
//
// ===========================================================================

// Interface offers a common interface for object marshaling/unmarshling operations and
// hides all the storage-related operations behind it.
type Interface interface {

	// Put...
	// if prevVersion < 0, we will set the object with current largest resource version,
	// put the object on the key, and returns the stored object.
	// If prevVersion >= 0, we will use it to do Compare-And-Swap against existing object's resource version.
	// Note that prevVersion = 0 means no previous object on given key.
	// - If compare failed, it will return current object (nil if non-exist) and StorageError of VersionConflicts.
	// - If compare succeed, it will behave like "prevVersion < 0".
	Put(key string, obj runtime.Object, prevVersion int64) (cur runtime.Object, err error)

	// Delete...
	// If prevVersion < 0, we will try to delete the object and return the deleted object. If no object existed
	// on given key, we returns nil object and nil error.
	// If prevVersion > 0, we will use it to do Compare-And-Delete against existing object's resource version.
	// - If compare failed, it will return current object (nil if non-exist) and StorageError of VersionConflicts.
	// - If compare succeed, it will behave like "prevVersion < 0".
	Delete(key string, prevRev int64) (cur runtime.Object, err error)

	// Get gets the most recent version of a key.
	// What is "most recent version" of a key? -- A key that was modified by this interface
	// should be reflected on read after write succeeded.
	// If no object exists on the key, it will return nil object and nil error.
	Get(key string) (cur runtime.Object, err error)

	// if version >=0, list should wait until we have seen object with version >= given version.
	List(prefix string, version int64, ss ...selector.Selector) (objects []runtime.Object, globalRev int64, err error)

	// WatchPrefix watches a prefix after given rev. If rev is 0, we will watch from current state.
	// It returns notifications of any keys that has given prefix.
	// Given selectors, it returns events that contained object of interest, either of current and previous.
	// If there is any problem establishing the watch channel, it will return error. After channel is established,
	// any error that happened will be returned from WatchChan immediately before it's closed.
	WatchPrefix(ctx context.Context, prefix string, version int64, ss ...selector.Selector) (WatchChan, error)

	//
	AddIndex(idxName, field string, g selector.FieldValueGetFunc) error
	DeleteIndex(idxName string)
}

type WatchChan <-chan WatchResponse

type WatchResponse struct {
	Type       watch.EventType
	Object     runtime.Object
	PrevObject runtime.Object
	Err        error
}
