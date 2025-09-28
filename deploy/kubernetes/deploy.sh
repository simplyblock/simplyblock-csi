#!/bin/bash

set -ex

CLUSTER_ID='cf50c029-212f-4c01-8e80-fcd3947ff7c3'
MGMT_IP='44.203.108.107'
CLUSTER_SECRET='ziVjCH713s4sjZPTZK30'

# list in creation order
files=(crd-simplyblockclusterconfig simplyblock-clusterconfig driver config-map nodeserver-config-map secret controller-rbac node-rbac controller node storageclass caching-node rbac-snapshot-controller setup-snapshot-controller snapshot.storage.k8s.io_volumesnapshotclasses snapshot.storage.k8s.io_volumesnapshotcontents snapshot.storage.k8s.io_volumesnapshots)

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

echo ""
echo "Deploying Caching node..."

output=$(kubectl get nodes -l type=cache | wc -l)

if [ $output -lt 2 ]; then
    echo "No caching nodes found. Exiting..."
    exit 0
fi


## check if the caching nodes has required huge pages
echo "-- caching nodes --"
kubectl get nodes -l type=cache


kubectl apply -f caching-node.yaml
kubectl wait --timeout=3m --for=condition=ready pod -l app=caching-node

for node in $(kubectl get pods -l app=caching-node -owide | awk 'NR>1 {print $(NF-3)}'); do
	echo "adding caching node: $node"

	curl --location "http://${MGMT_IP}/cachingnode/" \
		--header "Content-Type: application/json" \
		--header "Authorization: ${CLUSTER_ID} ${CLUSTER_SECRET}" \
		--data '{
		"cluster_id": "'"${CLUSTER_ID}"'",
		"node_ip": "'"${node}:5000"'",
		"iface_name": "eth0",
		"spdk_mem": "2g"
	}
	'
done
