### Multi Cluster Support

Simplyblock CSI now stores all multicluster wiring in a dedicated
`SimplyBlockClusterConfig` Custom Resource. Operators edit one manifest to
declare every Simplyblock cluster (credentials, endpoints, and topology
segments), and the controller automatically selects the correct backend based on
the scheduled zone.

#### Defining clusters with SimplyBlockClusterConfig

Install the CRD and create an instance that lists each Simplyblock cluster you
want the driver to use:

```yaml
apiVersion: csi.simplyblock.io/v1alpha1
kind: SimplyBlockClusterConfig
metadata:
  name: simplyblock-clusters
spec:
  clusters:
  - clusterID: "4ec308a1-61cf-4ec6-bff9-aa837f7bc0ea"
    clusterEndpoint: "https://cluster-1.simplyblock.example"
    clusterSecret: "super_secret_1"
    topology:
      topology.kubernetes.io/region: us-east-1
    zones:
    - us-east-1a
  - clusterID: "bd4bdf29-12ba-40d6-8f99-6b4988ad6c5e"
    clusterEndpoint: "https://cluster-2.simplyblock.example"
    clusterSecret: "super_secret_2"
    topology:
      topology.kubernetes.io/region: us-east-1
    zones:
    - us-east-1b
```

Each entry provides the UUID, management endpoint, and secret previously stored
in `simplyblock-csi-secret`. The optional `topology` map lets you pin additional
segments (for example, a region). The `zones` list enumerates the Kubernetes
zones handled by the cluster; the controller creates the zone → cluster mapping
from this list.

The driver watches this Custom Resource (cached with a short TTL). Updating the
CR is all that is required to add or remove clusters—no more editing multiple
secrets or StorageClasses.

#### StorageClass configuration

With zone mappings defined in the CR, a StorageClass no longer needs a
`cluster_id` or `zone_cluster_map` parameter. Instead, enable
`WaitForFirstConsumer`, keep any Simplyblock-specific QoS options, and restrict
allowed topologies to the zones you want to expose:

```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: simplyblock-csi
provisioner: csi.simplyblock.io
parameters:
  pool_name: production
  compression: "False"
  encryption: "False"
volumeBindingMode: WaitForFirstConsumer
allowVolumeExpansion: true
allowedTopologies:
- matchLabelExpressions:
  - key: topology.kubernetes.io/zone
    values:
    - us-east-1a
    - us-east-1b
```

When a pod is scheduled into `us-east-1a`, the external-provisioner forwards the
topology requirement to the CSI controller. The controller looks up the zone in
the `SimplyBlockClusterConfig`, resolves the corresponding cluster, and returns
the correct credentials to the node plugin.

> **Fallback:** If you still have legacy StorageClasses with a
> `zone_cluster_map` parameter or the `simplyblock-csi-secret` file, the driver
> honours them as a compatibility path. The CRD is the recommended long-term
> interface because it keeps cluster definitions, credentials, and topology in a
> single, versioned resource.
