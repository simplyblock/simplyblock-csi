### Storage nodes volume provisioning

Apart from a disaggregated storage cluster deployment, storage-plane pods can now also be deployed onto k8s workers and they may-coexist with any compute workload (storage consumers). 
Depending on the type of the storage node, it has to come with either at least one locally attached nvme drive or ebs block storage volumes are auto-attached in the during the deployment (aws only). 

### Preparing nodes

#### Step 0: Networking & tools

Make sure that the Kubernetes worker nodes running storage-plane pods have nvme-oF access to each other and - if needed - external storage nodes in the simplyblock cluster. They also need connectivity to/from the simplyblock control plane. If you are using terraform to deploy the cluster. Please attach `container-instance-sg` security group to all the instances.

#### Step1: Install nvme cli tools and nbd

To attach NVMe device to the host machine, the CSI driver uses [nvme-cli]([url](https://github.com/linux-nvme/nvme-cli)). So lets install that
```
sudo yum install -y nvme-cli
sudo modprobe nvme-tcp
sudo modprobe nbd
```

#### Step1: Setup hugepages

Simplyblock uses huge page memory. It is necessary to reserve an amount of huge page memory early on. 
The simplyblock storage plane pod allocates huge page memory from the reserved pool when the pod is added or restarted. 
The amount reserved is based on parameters provided to the storage node add, such as the maximum amount of logical volumes and snapshots and the max. provisioning size of the node (see helm chart parameters).
The minimum amount to reserve is 2 GiB, but try to reserve at least 25% of the node's total RAM. 
It is fine to reserve more than needed, as Simplyblock will allocate only the amount required from that pool and the rest can be used by the system. 

>[!IMPORTANT]
>One huge page is 2 MiB. So e.g. a value of 4096 reserves 8 GiB of huge page memory.

```
sudo sysctl -w vm.nr_hugepages=4096
```

confirm the hugepage changes by running
cat /proc/meminfo | grep -i hug


and restart kubelet
```
sudo systemctl restart kubelet
```

conform if huge pages are added to the cluster or not.
```
kubectl describe node ip-10-0-2-184.us-east-2.compute.internal | grep hugepages-2Mi
```
this output should show 8GB. This worker node can allocate 8GB of hugepages to pods which is required in case of SPDK pods.

#### Step2: Mount the SSD or EBS to be used by the storage node
If the instance comes with a default NVMe disk, it can be used with minimum of 2 partitions and 2 device where one is used for Journal manager and the other storage node. Or 2 additional EBS one for Journal Manager and the other for the Storage. the disks can be viewed by running:

```
sudo yum install pciutils
lspci
```


#### Step3: Tag the kubernetes nodes

After the nodes are prepared, label the kubernetes nodes
```
kubectl label nodes ip-10-0-4-118.us-east-2.compute.internal ip-10-0-4-176.us-east-2.compute.internal io.simplyblock.node-type=simplyblock-storage-plane
```
Now the nodes are ready to deploy storage nodes.
