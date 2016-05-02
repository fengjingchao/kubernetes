#!/usr/bin/env bash

gcloud compute ssh --zone us-central1-b ubuntu@etcdv3-single --command "sudo bash $HOME/bin/clean_etcd.sh"
gcloud compute ssh --zone us-central1-b ubuntu@etcdv3-single-event --command "sudo bash $HOME/bin/clean_etcd.sh"

sleep 2

ETCDCTL_API=3 etcdctl --endpoints etcdv3-single:4001 get --prefix /
ETCDCTL_API=3 etcdctl --endpoints etcdv3-single-event:4001 get --prefix /

echo "Done"
