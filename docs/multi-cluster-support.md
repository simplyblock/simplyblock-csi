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

With the `zone_cluster_map` or `region_cluster_map` parameter you can publish a single StorageClass that targets multiple Simplyblock clusters. A minimal example:

#### Zone-based mapping (zone → cluster)

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


#### Region-based mapping (region → cluster)

Use this when your Simplyblock backend is accessible across all zones within a region, or when you want a coarser placement policy than zones.

```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: simplyblock-csi
provisioner: csi.simplyblock.io
parameters:
  pool_name: production
  region_cluster_map: |
    {"us-east-1":"cluster-uuid-a","us-west-2":"cluster-uuid-b"}
volumeBindingMode: WaitForFirstConsumer
allowVolumeExpansion: true
allowedTopologies:
- matchLabelExpressions:
  - key: topology.kubernetes.io/region
    values:
    - us-east-1
    - us-west-2
```

> **Tip:** The keys inside `region_cluster_map` must match the region labels present on your Kubernetes nodes (typically `topology.kubernetes.io/region`). You can include as many region as needed, each pointing to the cluster ID defined in `simplyblock-csi-secret-v2`.

Stateful workloads can then rely on standard pod topology hints, for example a StatefulSet with `podAntiAffinity` that spreads replicas across zones or regions. When a PVC is created, the scheduler selects the desired zone or region, the CSI driver resolves the cluster ID from the map, and the volume is provisioned on the correct Simplyblock backend.

### Configuring Multi Storage Cluster Support
In addition to multi-cluster connectivity via the `simplyblock-csi-secret-v2` secret, the Simplyblock Storage Controller also supports multi storage cluster configuration using a dedicated ConfigMap named `simplyblock-clusters`.

This ConfigMap defines all Simplyblock clusters known to the Storage Controller and can be updated dynamically to include additional clusters as your environment grows.

#### Default ConfigMap

By default, the Helm chart installs a single-cluster configuration that looks like this:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: simplyblock-clusters
data:
  clusters.json: |
    {
      "clusters": [
        {
          "cluster_id": "{{ .Values.csiConfig.simplybk.uuid }}",
          "workers": []
        }
      ]
    }
```

- cluster_id — The UUID of the Simplyblock cluster.

- workers — A list of worker nodes associated with the cluster.

**NB:** This can be left empty if the storage controller should auto-discover or manage workers dynamically.

#### Adding Additional Clusters

To enable multi storage cluster support, edit the simplyblock-clusters ConfigMap and append the new clusters to the clusters array.

Example for multiple clusters:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: simplyblock-clusters
data:
  clusters.json: |
    {
      "clusters": [
        {
          "cluster_id": "cluster-uuid-a",
          "workers": ["k8-worker-node-1", "k8-worker-node-2", "k8-worker-node-3"]
        },
        {
          "cluster_id": "cluster-uuid-b",
          "workers": ["k8-worker-node-4", "k8-worker-node-5", "k8-worker-node-6"]
        }
      ]
    }

```
