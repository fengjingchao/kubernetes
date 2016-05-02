gcloud compute ssh --zone us-central1-b ubuntu@etcd-static-1 --command "sudo bash /root/bin/cleanup_etcd.sh"
gcloud compute ssh --zone us-central1-b ubuntu@etcd-eventstore --command "sudo bash /root/bin/cleanup_etcd.sh"

sleep 2
etcdctl --peers "http://etcd-static-1:2379" ls / && etcdctl --peers "http://etcd-eventstore:2379" ls /
echo "Done"
