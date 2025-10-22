### Multi Cluster Support

The Simplyblock CSI driver now offers **multi-cluster support**, allowing it to connect with multiple Simplyblock clusters. Previously, the CSI driver could only connect to a single cluster.

To enable interaction with multiple clusters, we rely on two building blocks:

1. **Topology-aware cluster selection (`zone_cluster_map` parameter):** A StorageClass can now expose a single parameter, `zone_cluster_map`, that maps Kubernetes zones to Simplyblock cluster IDs. When a PersistentVolumeClaim is created, the CSI controller inspects the topology selected by the scheduler (using `volumeBindingMode: WaitForFirstConsumer`) and automatically provisions the volume on the mapped cluster. This enables you to present **one** StorageClass that works across all Availability Zones.
2. **`simplyblock-csi-secret-v2` Secret:** A Kubernetes secret that stores credentials for each configured Simplyblock cluster. The driver reads this secret to establish connections on demand.


#### Adding new cluster

When the Simplyblock CSI driver is initially installed, typically using Helm:
```
helm install simplyblock-csi ./ \
    --set csiConfig.simplybk.uuid=${CLUSTER_ID} \
    --set csiConfig.simplybk.ip=${CLUSTER_IP} \
    --set csiSecret.simplybk.secret=${CLUSTER_SECRET} \
```

The `CLUSTER_ID` (UUID), Gateway Endpoint (`CLUSTER_IP`), and Secret (`CLUSTER_SECRET`) of the initial cluster must be provided. This command automatically creates the `simplyblock-csi-secret-v2` secret.

The structure of the simplyblock-csi-secret-v2 secret looks like this:

```yaml
apiVersion: v1
data:
  secret.json: <base64 encoded secret>
kind: Secret
metadata:
  name: simplyblock-csi-secret-v2
type: Opaque
```

and the decoded secret looks something like this
```
{
   "clusters": [
     {
       "cluster_id": "4ec308a1-61cf-4ec6-bff9-aa837f7bc0ea",
       "cluster_endpoint": "http://127.0.0.1",
       "cluster_secret": "super_secret"
     }
   ]
}
```

To add a new cluster, we will need to edit this secret and add a new cluster


```sh
# save cluster secret to a file
kubectl get secret simplyblock-csi-secret-v2 -o jsonpath='{.data.secret\.json}' | base64 --decode > secret.yaml

# edit the clusters and add the new cluster's cluster_id, cluster_endpoint, cluster_secret
# vi secret.json 

cat secret.json | base64 | tr -d '\n' > secret-encoded.json

# Replace data.secret.json with the content of secret-encoded.json
# kubectl -n simplyblock edit secret simplyblock-csi-secret-v2
```


```yaml
apiVersion: v1
data:
  secret.json: <new content of the secret-encoded.json>
kind: Secret
metadata:
  name: simplyblock-csi-secret-v2
type: Opaque
```

### Using multi cluster

With the `zone_cluster_map` parameter you can publish a single StorageClass that targets multiple Simplyblock clusters. A minimal example:

```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: simplyblock-csi
provisioner: csi.simplyblock.io
parameters:
  pool_name: production
  zone_cluster_map: |
    {"us-east-1a":"cluster-uuid-a","us-east-1b":"cluster-uuid-b"}
volumeBindingMode: WaitForFirstConsumer
allowVolumeExpansion: true
allowedTopologies:
- matchLabelExpressions:
  - key: topology.kubernetes.io/zone
    values:
    - us-east-1a
    - us-east-1b
```

> **Tip:** The keys inside `zone_cluster_map` must match the zone labels present on your Kubernetes nodes (typically `topology.kubernetes.io/zone`). You can include as many zones as needed, each pointing to the cluster ID defined in `simplyblock-csi-secret-v2`.

Stateful workloads can then rely on standard pod topology hints, for example a StatefulSet with `podAntiAffinity` that spreads replicas across zones. When a PVC is created, the scheduler selects the desired zone, the CSI driver resolves the cluster ID from the map, and the volume is provisioned on the correct Simplyblock backend.
