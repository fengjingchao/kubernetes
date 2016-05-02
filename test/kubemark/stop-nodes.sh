#!/usr/bin/env bash

# Copyright 2015 The Kubernetes Authors All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

KUBE_ROOT=$(dirname "${BASH_SOURCE}")/../..

source "${KUBE_ROOT}/test/kubemark/common.sh"

kubectl delete -f ${KUBE_ROOT}/test/kubemark/hollow-kubelet.json &> /dev/null || true
kubectl delete -f ${KUBE_ROOT}/test/kubemark/kubemark-ns.json &> /dev/null || true

GCLOUD_COMMON_ARGS="--project ${PROJECT} --zone ${ZONE} --quiet"

rm -rf "${KUBE_ROOT}/test/kubemark/kubeconfig.loc" &> /dev/null || true
rm "ca.crt" "kubecfg.crt" "kubecfg.key" "${KUBE_ROOT}/test/kubemark/hollow-node.json" &> /dev/null || true

echo "PROJECT:${PROJECT}"
echo "ZONE:${ZONE}"

gcloud compute instances reset "${MASTER_NAME}" ${GCLOUD_COMMON_ARGS} || true
