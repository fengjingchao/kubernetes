#!/usr/bin/env bash

set -o errexit
set -o nounset
set -o pipefail

gcloud compute --project "coreos-k8s-scale-testing" instances stop --zone "us-central1-b" "e2e-test-ubuntu-master"
./wan/clean_etcd.sh
gcloud compute --project "coreos-k8s-scale-testing" instances start --zone "us-central1-b" "e2e-test-ubuntu-master"

