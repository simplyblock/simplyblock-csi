#!/bin/bash

set -ex

CLUSTER_ID='cf50c029-212f-4c01-8e80-fcd3947ff7c3'
MGMT_IP='44.203.108.107'
CLUSTER_SECRET='ziVjCH713s4sjZPTZK30'

# list in creation order
files=(driver config-map nodeserver-config-map secret controller-rbac node-rbac controller node storageclass rbac-snapshot-controller setup-snapshot-controller snapshot.storage.k8s.io_volumesnapshotclasses snapshot.storage.k8s.io_volumesnapshotcontents snapshot.storage.k8s.io_volumesnapshots)

if [ "$1" = "teardown" ]; then
	# delete in reverse order
	for ((i = ${#files[@]} - 1; i >= 0; i--)); do
		echo "=== kubectl delete -f ${files[i]}.yaml"
		kubectl delete -f "${files[i]}.yaml"
	done
	exit 0
else
	for ((i = 0; i <= ${#files[@]} - 1; i++)); do
		echo "=== kubectl apply -f ${files[i]}.yaml"
		kubectl apply -f "${files[i]}.yaml"
	done
fi

