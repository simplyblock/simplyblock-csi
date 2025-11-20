# Installation with Helm 3

Follow this guide to install the SPDK-CSI Driver for Kubernetes.

## Prerequisites

### [Install Helm](https://helm.sh/docs/intro/quickstart/#install-helm)

### Build image

```console
make image
cd deploy/spdk
sudo docker build -t spdkdev .
```
 **_NOTE:_**
Kubernetes nodes must pre-allocate hugepages in order for the node to report its hugepage capacity.
A node can pre-allocate huge pages for multiple sizes.

## Install latest CSI Driver via `helm install`

```console

helm repo add spdk-csi https://raw.githubusercontent.com/simplyblock-io/spdk-csi/master/charts/spdk-csi

helm repo update

helm install -n simplyblk --create-namespace spdk-csi spdk-csi/spdk-csi \
  --set csiConfig.simplybk.uuid=ace14718-81eb-441f-9d4c-d71ce6904196 \
  --set csiConfig.simplybk.ip=https://96xdzb9ne7.execute-api.us-east-1.amazonaws.com \
  --set csiSecret.simplybk.secret=k6U5moyrY5vCVtSiCcKo \
  --set logicalVolume.pool_name=testing1
```

## After installation succeeds, you can get a status of Chart

```console
helm status "spdk-csi" --namespace "simplyblk"
```

## Delete Chart

If you want to delete your Chart, use this command

```bash
helm uninstall "spdk-csi" --namespace "simplyblk"
```

If you want to delete the namespace, use this command

```bash
kubectl delete namespace simplyblk
```

## driver parameters

The following table lists the configurable parameters of the latest Simplyblock CSI Driver chart and default values.

| Parameter                              | Description                                                                                                              | Default                                                                 |
| -------------------------------------- | ------------------------------------------------------------------------------------------------------------------------ | ----------------------------------------------------------------------- |
| `driverName`                           | alternative driver name                                                                                                  | `csi.simplyblock.io`                                                           |
| `image.csi.repository`             | simplyblock-csi-driver image                                                                                             | `simplyblock/spdkcsi`                                                   |
| `image.csi.tag`                    | simplyblock-csi-driver image tag                                                                                         | `v0.1.0`                                                                |
| `image.csi.pullPolicy`             | simplyblock-csi-driver image pull policy                                                                                 | `Always`                                                                |
| `image.csiProvisioner.repository`      | csi-provisioner docker image                                                                                             | `registry.k8s.io/sig-storage/csi-provisioner`                           |
| `image.csiProvisioner.tag`             | csi-provisioner docker image tag                                                                                         | `v4.0.1`                                                                |
| `image.csiProvisioner.pullPolicy`      | csi-provisioner image pull policy                                                                                        | `Always`                                                                |
| `image.csiAttacher.repository`         | csi-attacher docker image                                                                                                 | `gcr.io/k8s-staging-sig-storage/csi-attacher`                           |
| `image.csiAttacher.tag`                | csi-attacher docker image tag                                                                                             | `v4.5.1`                                                                |
| `image.csiAttacher.pullPolicy`         | csi-attacher image pull policy                                                                                            | `Always`                                                                |
| `image.nodeDriverRegistrar.repository` | csi-node-driver-registrar docker image                                                                                   | `registry.k8s.io/sig-storage/csi-node-driver-registrar`                 |
| `image.nodeDriverRegistrar.tag`        | csi-node-driver-registrar docker image tag                                                                               | `v2.10.1`                                                               |
| `image.nodeDriverRegistrar.pullPolicy` | csi-node-driver-registrar image pull policy                                                                              | `Always`                                                                |
| `image.csiSnapshotter.repository`      | csi-snapshotter docker image                                                                                             | `registry.k8s.io/sig-storage/csi-snapshotter`                           |
| `image.csiSnapshotter.tag`             | csi-snapshotter docker image tag                                                                                         | `v8.2.0`                                                                |
| `image.csiSnapshotter.pullPolicy`      | csi-snapshotter image pull policy                                                                                        | `Always`                                                                |
| `image.csiSnapshotterController.repository`      | csi-snapshotter-controller docker image                                                                                             | `registry.k8s.io/sig-storage/snapshot-controller`                           |
| `image.csiSnapshotterController.tag`             | csi-snapshotter-controller docker image tag                                                                                         | `v8.2.0`                                                                |
| `image.csiSnapshotterController.pullPolicy`      | csi-snapshotter-controller image pull policy                                                                                        | `Always`                                                                |
| `image.csiResizer.repository`          | csi-resizer  docker image                                                                                                | `gcr.io/k8s-staging-sig-storage/csi-resizer`                            |
| `image.csiResizer.tag`                 | csi-resizer docker image tag                                                                                             | `v1.10.1`                                                               |
| `image.csiResizer.pullPolicy`          | csi-resizer image pull policy                                                                                            | `Always`                                                                |
| `image.csiHealthMonitor.repository`    | csi-external-health-monitor-controller docker image                                                                      | `gcr.io/k8s-staging-sig-storage/csi-external-health-monitor-controller` |
| `image.csiHealthMonitor.tag`           | csi-external-health-monitor-controller docker image tag                                                                  | `v0.11.0`                                                               |
| `image.csiHealthMonitor.pullPolicy`    | csi-external-health-monitor-controller image pull policy                                                                 | `Always`                                                                |
| `image.simplyblock.repository`         | simplyblock mgmt docker image                                                                                            | `simplyblock/simplyblock`                                               |
| `image.simplyblock.tag`                | simplyblock mgmt docker image tag                                                                                        | `R25.5-Hotfix`                                                            |
| `image.simplyblock.pullPolicy`         | csi-snapshotter image pull policy                                                                                        | `Always`                                                                |
| `image.storageNode.repository`         | simplyblock storage-node controller docker image                                                                                            | `simplyblock/simplyblock`                                               |
| `image.storageNode.tag`                | simplyblock storage-node controller docker image tag                                                                                        | `v0.1.0`                                                            |
| `image.storageNode.pullPolicy`         | simplyblock storage-node controller image pull policy                                                                                        | `Always`                                                                |
| `image.cachingNode.repository`         | simplyblock caching-node controller docker image                                                                                            | `simplyblock/simplyblock`                                               |
| `image.cachingNode.tag`                | simplyblock caching-node controller docker image tag                                                                                        | `v0.1.0`                                                            |
| `image.cachingNode.pullPolicy`         | simplyblock caching-node controller image pull policy                                                                                        | `Always`                                                                |
| `image.mgmtAPI.repository`             | simplyblock mgmt api image                                                                                            | `python`                                               |
| `image.mgmtAPI.tag`                    | simplyblock mgmt api image tag                                                                                        | `3.10`                                                            |
| `image.mgmtAPI.pullPolicy`             | simplyblock mgmt api image pull policy                                                                                        | `Always`                                                                |
| `serviceAccount.create`                | whether to create service account of spdkcsi-controller                                                                  | `true`                                                                  |
| `rbac.create`                          | whether to create rbac of spdkcsi-controller                                                                                | `true`                                                                  |
| `controller.replicas`                  | replica number of spdkcsi-controller                                                                                     | `1`                                                                     |
| `serviceAccount.create`                | whether to create service account of csi controller                                                                  | `true`                                                                  |
| `rbac.create`                          | whether to create rbac of csi controller                                                                                | `true`                                                                  |
| `controller.replicas`                  | replica number of csi controller                                                                                     | `1`                                                                     |
| `controller.tolerations.create`       | Whether to create tolerations for the csi controller                                                                       | `false`                                                                     |  |
| `controller.tolerations.list[0].effect`       | The effect of tolerations on the csi controller	                                                                          | `<empty>`                                                               |  |
| `controller.tolerations.list[0].key	`        | The key of tolerations for the csi controller	                                                                            | `<empty>`                                                                |  |
| `controller.tolerations.list[0].operator	`    | The operator for the csi controller tolerations	                                                                          |                                            `Exists`                                                                    |  |
| `controller.tolerations.list[0].value	`      | The value of tolerations for the csi controller	                                                                          |                                            `<empty>`                                                        |  |
| `controller.nodeSelector.create`       | Whether to create nodeSelector for the csi controller                                                                       | `false`                                                                     |  |
| `controller.nodeSelector.key	`        | The key of nodeSelector for the csi controller	                                                                            | `<empty>`                                                                |  |
| `controller.nodeSelector.value	`      | The value of nodeSelector for the csi controller	                                                                          |                                            `<empty>`                                                        |  |
| `storageclass.create`                  | create storageclass                                                                                                      | `true`                                                                  |  |
| `snapshotclass.create`                  | create snapshotclass                                                                                                   | `true`                                                                  |  |
| `snapshotcontroller.create`             | create snapshot controller and CRD for snasphot support it                                                                                                    | `true`                                                                  |  |
| `externallyManagedConfigmap.create`    | Specifies whether a externallyManagedConfigmap should be created                                                         | `true`                                                                  |  |
| `externallyManagedSecret.create`       | Specifies whether a externallyManagedSecret should be created                                                            | `true`                                                                  |  |
| `csiConfig.simplybk.uuid`              | the simplyblock cluster UUID on which the volumes are provisioned                                                                 | ``                                                                      |  |
| `csiConfig.simplybk.ip`                | the HTTPS API Gateway endpoint connected to the management node                                                          | `https://o5ls1ykzbb.execute-api.eu-central-1.amazonaws.com`             |  |
| `csiSecret.simplybk.secret`            | the cluster secret associated with the cluster                                                                           | ``                                                                      |  |
| `csiSecret.simplybkPvc.crypto_key1`    | if an encrypted PVC is to be created, value of `crypto_key1`                                                             | ``                                                                      |  |
| `csiSecret.simplybkPvc.crypto_key2`    | if an encrypted PVC is to be created, value of `crypto_key2`                                                             | ``                                                                      |  |
| `logicalVolume.pool_name`              | the name of the pool against which the lvols needs to be provisioned. This Pool needs to be created before its passed here. | `testing1`                                                              |  |
| `logicalVolume.qos_rw_iops`            | the value of lvol parameter qos_rw_iops                                                                                  | `0`                                                                     |  |
| `logicalVolume.qos_rw_mbytes`          | the value of lvol parameter qos_rw_mbytes                                                                                | `0`                                                                     |  |
| `logicalVolume.qos_r_mbytes`           | the value of lvol parameter qos_r_mbytes                                                                                 | `0`                                                                     |  |
| `logicalVolume.qos_w_mbytes`           | the value of lvol parameter qos_w_mbytes                                                                                 | `0`                                                                     |  |
| `logicalVolume.encryption`             | set to `True` if encryption needs be enabled on lvols.                                                                   | `False`                                                                 |  |
| `logicalVolume.numDataChunks`          | The number of Erasure coding schema parameter k (distributed raid)                                                       | `1`                                                                     |  |
| `logicalVolume.numParityChunks`        | The number of Erasure coding schema parameter n (distributed raid)                                                                                                 | `1`                                                                     |  |
| `logicalVolume.lvol_priority_class`     | the value of lvol parameter lvol_priority_class                                                                               | `0`                                                                     |  |
| `logicalVolume.max_namespace_per_subsys` | the maximum namespace per subsystem                                                                               | `1`                                                                     |  |
| `podAnnotations`                        | Annotations to apply to all pods in the chart                                                                   | `{}`                                                                     |  |
| `simplyBlockAnnotations`                | Annotations to apply to Simplyblock kubernetes resources like DaemonSets, Deployments, or StatefulSets                                                                                         | `{}`                                                                     |  |
| `benchmarks`                           | the number of benchmarks to run                                                                                          | `0`                                                                     |  |
| `node.tolerations.create`       | Whether to create tolerations for the csi node                                                                       | `false`                                                                     |  |
| `node.tolerations.list[0].effect`       | The effect of tolerations on the csi node	                                                                          | `<empty>`                                                               |  |
| `node.tolerations.list[0].key	`        | The key of tolerations for the csi node	                                                                            | `<empty>`                                                                |  |
| `node.tolerations.list[0].operator	`    | The operator for the csi node tolerations	                                                                          |                                            `Exists`                                                                    |  |
| `node.tolerations.list[0].value	`      | The value of tolerations for the csi node	                                                                          |                                            `<empty>`                                                        |  |
| `node.nodeSelector.create`       | Whether to create nodeSelector for the csi node                                                                       | `false`                                                                     |  |
| `node.nodeSelector.key	`        | The key of nodeSelector for the csi node	                                                                            | `<empty>`                                                                |  |
| `node.nodeSelector.value	`      | The value of nodeSelector for the csi node	                                                                          |                                            `<empty>`                                                        |  |
| `cachingnode.tolerations.create`       | Whether to create tolerations for the caching node                                                                       | `false`                                                                     |  |
| `cachingnode.tolerations.list[0].effect`       | The effect of tolerations on the caching node	                                                                          | `<empty>`                                                               |  |
| `cachingnode.tolerations.list[0].key	`        | The key of tolerations for the caching node	                                                                            | `<empty>`                                                                |  |
| `cachingnode.tolerations.list[0].operator	`    | The operator for the caching node tolerations	                                                                          |                                            `Exists`                                                                    |  |
| `cachingnode.tolerations.list[0].value	`      | The value of tolerations for the caching node	                                                                          |                                            `<empty>`                                                        |  |
| `cachingnode.create`                   |  Whether to create caching node on kubernetes worker node                                                                              | `false`                                                        |  |
| `cachingnode.nodeSelector.key`         | The key used in the nodeSelector to constrain which nodes the DaemonSet should run on                                         | `type`                                                        |  |
| `cachingnode.nodeSelector.value`       | The value for the nodeSelector key to match against specific nodes                                         | `simplyblock-cache`                                                        |  |
| `cachingnode.ifname`                   | the default interface to be used for binding the caching node to host interface                                          | `eth0`                                                                     |  |
| `cachingnode.cpuMask`                  | the cpu mask for the spdk app to use for caching node                                                                    | `<empty>`                                                                  |  |
| `cachingnode.spdkMem`                  |  the amount of hugepage memory to allocate for caching node                                                                                                                        | `<empty>`                                                                  |  |
| `cachingnode.spdkImage`                | SPDK image uri for caching node                                                                                                                     | `<empty>`                                                                  |  |
| `cachingnode.multipathing`             | Enable multipathing for lvol connection                                                                                                                        | `true`                                                                     |  |
| `storagenode.daemonsets[0].name`                   | The name of the storage node DaemonSet	                                                                                    |                                              `storage-node-ds`                                                          |  |
| `storagenode.daemonsets[0].appLabel`               | The label applied to the storage node DaemonSet for identification	                                                                    | `storage-node`                                                                     |  |
| `storagenode.daemonsets[0].nodeSelector.key`       | The key used in the nodeSelector to constrain which nodes the DaemonSet should run on		                                                                    | `io.simplyblock.node-type`                                                                     |  |
| `storagenode.daemonsets[0].nodeSelector.value`     | The value for the nodeSelector key to match against specific nodes		                                                                    | `simplyblock-storage-plane`                                                                     |  |
| `storagenode.daemonsets[0].tolerations.create`     | Whether to create tolerations for the storage node                                                                       | `false`                                                                     |  |
| `storagenode.daemonsets[0].tolerations.list[0].effect`     | the effect of tolerations on the storage node	                                                                          | `<empty>`                                                               |  |
| `storagenode.daemonsets[0].tolerations.list[0].key	`      | the key of tolerations for the storage node	                                                                            | `<empty>`                                                                |  |
| `storagenode.daemonsets[0].tolerations.list[0].operator	`  | the operator for the storage node tolerations	                                                                          |                                            `Exists`                                                                    |  |
| `storagenode.daemonsets[0].tolerations.list[0].value	`    | the value of tolerations for the storage node	                                                                          |                                            `<empty>`                                                        |  |
| `storagenode.daemonsets[1].name`                   | The name of the restart storage node DaemonSet	                                                                                    |                                              `storage-node-ds-restart`                                                          |  |
| `storagenode.daemonsets[1].appLabel`               | The label applied to the restart storage node DaemonSet for identification	                                                                    | `storage-node-restart`                                                                     |  |
| `storagenode.daemonsets[1].nodeSelector.key`       | The key used in the nodeSelector to constrain which nodes the DaemonSet should run on		                                                                    | `type`                                                                     |  |
| `storagenode.daemonsets[1].nodeSelector.value`     | The value for the nodeSelector key to match against specific nodes		                                                                    | `simplyblock-storage-plane-restart`                                                                     |  |
| `storagenode.daemonsets[1].tolerations.create`     | Whether to create tolerations for the restart storage node                                                                       | `false`                                                                     |  |
| `storagenode.daemonsets[1].tolerations.list[0].effect`     | the effect of tolerations on the restart storage node	                                                                          | `<empty>`                                                               |  |
| `storagenode.daemonsets[1].tolerations.list[0].key	`      | the key of tolerations for the restart storage node	                                                                            | `<empty>`                                                                |  |
| `storagenode.daemonsets[1].tolerations.list[0].operator	`  | the operator for the restart storage node tolerations	                                                                          |                                            `Exists`                                                                    |  |
| `storagenode.daemonsets[1].tolerations.list[0].value	`    | the value of tolerations for the restart storage node	                                                                          |                                            `<empty>`                                                        |  |
| `storagenode.create`                   |  Whether to create storage node on kubernetes worker node                                                                           | `false`                                                        |  |
| `storagenode.ifname`                   | the default interface to be used for binding the storage node to host interface                                          | `eth0`                                                                     |  |
| `storagenode.spdkImage`                | SPDK image uri for storage node                                                                                                                        | `<empty>`                                                                  |  |
| `storagenode.maxLogicalVolumes`        | the default max lvol per storage node	                                                                                  | `10`                                                                       |  |
| `storagenode.maxSnapshots`             | the default max snapshot per storage node	                                                                              | `10`                                                                       |  |
| `storagenode.maxSize`                  | the max provisioning size of all storage nodes	                                                                          | `<empty>`                                                                     |  |
| `storagenode.numPartitions`            | the number of partitions to create per device                                                                            | `1`                                                                        |  |
| `storagenode.numDevices`               | the number of devices per storage node	                                                                                  | `1`                                                                        |  |
| `storagenode.numDistribs`              | the number of distribs per storage node	                                                                                  | `2`                                                                        |  |
| `storagenode.isolateCores`             | Enable core Isolation                                                                                                  | `false`                                                                  |  |
| `storagenode.haJMCount`                | the number of ha Journal managers                                                                                                  | `<empty>`                                                                  |  |
| `storagenode.dataNics`                  | Data interface names                                                                                                  | `<empty>`                                                                |  |
| `storagenode.pciAllowed`                 | the list of allowed nvme pcie addresses                                                                                                  | `<empty>`                                                                |  |
| `storagenode.pciBlocked`                 | the list of blocked nvme pcie addresses                                                                                                  | `<empty>`                                                                |  |
| `storagenode.socketsToUse`               | the list of sockets to use                                                                                                  | `<empty>`                                                                |  |
| `storagenode.nodesPerSocket`             | The number of nodes to use per socket                                                                                                  | `<empty>`                                                                |  |
| `storagenode.coresPercentage`            | The percentage of cores to be used for spdk                                                                                                 | `<empty>`                                                                |  |
| `storagenode.ubuntuHost`                 | Set to true if the worker node runs Ubuntu and needs the nvme-tcp kernel module installed                                                                                                 | `false`                                                                |  |
| `storagenode.openShiftCluster`           | Set to true if the worker node runs OpenShift and needs core isolation                                                                                                 | `false`                                                                |  |


## Install latest Simplyblock Storage Controller via `helm install`

```console

helm repo add sb-controller https://raw.githubusercontent.com/simplyblock-io/spdk-csi/master/charts/sb-controller

helm repo update

helm install -n simplyblk --create-namespace sb-controller sb-controller/sb-controller \
  --set storagenode.create=true \
  --set cachingnode.create=false 
```

## After installation succeeds, you can get a status of Chart

```console
helm status "sb-controller" --namespace "simplyblk"
```

## Delete Chart

If you want to delete your Chart, use this command

```bash
helm uninstall "sb-controller" --namespace "simplyblk"
```

If you want to delete the namespace, use this command

```bash
kubectl delete namespace simplyblk
```

## Controller parameters

The following table lists the configurable parameters of the latest Simplyblock Storage Controller chart and default values.

| Parameter                              | Description                                                                                                              | Default                                                                 |
| -------------------------------------- | ------------------------------------------------------------------------------------------------------------------------ | ----------------------------------------------------------------------- |
| `image.storageNode.repository`         | simplyblock storage-node controller docker image                                                                                            | `simplyblock/simplyblock`                                               |
| `image.storageNode.tag`                | simplyblock storage-node controller docker image tag                                                                                        | `v0.1.0`                                                            |
| `image.storageNode.pullPolicy`         | simplyblock storage-node controller image pull policy                                                                                        | `Always`                                                                |
| `image.cachingNode.repository`         | simplyblock caching-node controller docker image                                                                                            | `simplyblock/simplyblock`                                               |
| `image.cachingNode.tag`                | simplyblock caching-node controller docker image tag                                                                                        | `v0.1.0`                                                            |
| `image.cachingNode.pullPolicy`         | simplyblock caching-node controller image pull policy                                                                                        | `Always`                                                                |
| `image.mgmtAPI.repository`             | simplyblock mgmt api image                                                                                            | `python`                                               |
| `image.mgmtAPI.tag`                    | simplyblock mgmt api image tag                                                                                        | `3.10`                                                            |
| `image.mgmtAPI.pullPolicy`             | simplyblock mgmt api image pull policy                                                                                        | `Always`                                                                |
| `serviceAccount.create`                | whether to create service account of spdkcsi-controller                                                                  | `true`                                                                  |
| `rbac.create`                          | whether to create rbac of spdkcsi-controller                                                                                | `true`                                                                  | |
| `cachingnode.create`                   |  Whether to create caching node on kubernetes worker node                                                                              | `false`                                                        |  |
| `cachingnode.ifname`                   | the default interface to be used for binding the caching node to host interface                                          | `eth0`                                                                     |  |
| `cachingnode.cpuMask`                  | the cpu mask for the spdk app to use for caching node                                                                    | `<empty>`                                                                  |  |
| `cachingnode.spdkMem`                  |  the amount of hugepage memory to allocate for caching node                                                                                                                        | `<empty>`                                                                  |  |
| `cachingnode.spdkImage`                | SPDK image uri for caching node                                                                                                                     | `<empty>`                                                                  |  |
| `cachingnode.multipathing`             | Enable multipathing for lvol connection                                                                                                                        | `true`                                                                     |  |
| `storagenode.create`                   |  Whether to create storage node on kubernetes worker node                                                                           | `false`                                                        |  |
| `storagenode.ifname`                   | The interface(s) used for binding the storage node to the host network. Can be a single value (e.g. `eth0`) or a list of interfaces (e.g. `{eth0,eth1}`), in which case the order defines the priority.                                         | `eth0`                                                                     |  |
| `storagenode.spdkImage`                | SPDK image uri for storage node                                                                                                                        | `<empty>`                                                                  |  |
| `storagenode.maxSnap`                  | the default max snapshot per storage node	                                                                              | `10`                                                                       |  |
| `storagenode.jmPercent`                | the number in percent to use for JM from each device	                                                                    | `3`                                                                        |  |
| `storagenode.numPartitions`            | the number of partitions to create per device                                                                            | `0`                                                                        |  |
| `storagenode.numDevices`               | the number of devices per storage node	                                                                                  | `1`                                                                        |  |
| `storagenode.numDistribs`              | the number of distribs per storage node	                                                                                  | `2`                                                                        |  |
| `storagenode.disableHAJM`              | Disable ha Journal Manager	                                                                                                  | `false`                                                                  |  |
| `storagenode.enableTestDevice`         | Enable creation of test device                                                                                                  | `false`                                                                  |  |
| `storagenode.dataNics`                 | Data interface names                                                                                                  | `<empty>`                                                                |  |


## troubleshooting
 - Add `--wait -v=5 --debug` in `helm install` command to get detailed error
 - Use `kubectl describe` to acquire more info
