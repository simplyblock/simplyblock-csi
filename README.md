# Simplyblock CSI Driver for Kubernetes

**High-performance NVMe/TCP (NVMe-over-Fabrics) CSI Driver for Kubernetes**

[![Documentation](https://img.shields.io/badge/Docs-simplyblock-blue)](https://docs.simplyblock.io/latest/deployments/kubernetes/) [![Issues](https://img.shields.io/github/issues/simplyblock/simplyblock-csi)](https://github.com/simplyblock/simplyblock-csi/issues)

![](assets/simplyblock-logo.svg)

---

## 🚀 Overview

`simplyblock-csi` is the official **simplyblock storage plugin for Kubernetes**. 

The **simplyblock CSI** extension delivers **high-performance block storage** to Kubernetes. Simplyblock leverages SPDK and NVMe-over-TCP
to implement a high-performance and ultra-low latency storage solution.

Simplyblock's CSI driver enables **enterprise-grade, NVMe/TCP-powered block storage** directly inside Kubernetes, offering high performance,
scalability, and resilience without the need for specialized hardware or vendor lock-in. It supports dynamic provisioning, snapshots, and
seamless integration via the Container Storage Interface (CSI).

With simplyblock, you can seamlessly integrate **software-defined storage (SDS)** into your Kubernetes environment, enabling support for
advanced features like:

- ⚡ **Ultra-low latency**: Unlock performance with NVMe-over-TCP
- 🧩 **Native Proxmox integration**: Manage volumes directly in Kubernetes
- 🛡️ **Enterprise data services**: Snapshots, clones, erasure coding, multi-tenancy
- 🔒 **Secure & robust**: Cluster authentication and Quality of Service (QoS)
- ☁️ **Cloud & on-prem flexibility**: Deploy into any Kubernetes-based distribution

👉 For full documentation, see the [Simplyblock Kubernetes Deployment Guide](https://docs.simplyblock.io/latest/deployments/kubernetes/).

---

## ✨ Features

| Feature                           | Benefit                                                                 |Add a comment on lines R31 to R33Add diff commentMarkdown input: edit mode selected.WritePreviewAdd a suggestionHeadingBoldItalicQuoteCodeLinkUnordered listNumbered listTask listMentionReferenceSaved repliesAdd FilesPaste, drop, or click to add filesCancelCommentStart a reviewReturn to code
|----------------------------------|-------------------------------------------------------------------------|
| **Dynamic Volume Provisioning**   | Dynamically provision and lifecycle-manage persistent volumes in Kubernetes |
| **NVMe/TCP Support**              | High throughput, low latency storage over standard Ethernet              |
| **Snapshots & Clones**           | Efficient data protection and instant provisioning                      |
| **Erasure Coding**                | Fault-tolerant, space-efficient redundancy                             |
| **Multi-tenancy & QoS**          | Isolated tenants with guaranteed IOPS, bandwidth, and latency           |
| **Resilient Networking**         | Supports redundant or isolated storage/control networks                  |
| **Auto-Reconnect**               | Automatic reconnection of NVMe devices after network or host failures   |

---

## Kubernetes Version Support

| Branch/Tag     | Kubernetes Version | Stability |
|----------------|--------------------|-----------|
| `master`       | 1.21+              | Stable    |
| `v0.1.0`       | 1.21+              | Beta      |
| `v0.1.1`       | 1.21+              | Stable    |

---

## 📦 Getting Started

The following section describes the installation of the CSI driver only. If you want to install a full simplyblock storage cluster into Kubernetes, please refer to the full documentation: [Simplyblock Kubernetes Deployment Guide](https://docs.simplyblock.io/latest/deployments/kubernetes/).

### 1. Prerequisites

Ensure your cluster meets the requirements:

- Kernel NVMe/TCP driver is loaded:

  ```bash
  modprobe nvme-tcp
  lsmod | grep 'nvme_'
````

You should see `nvme_tcp`, `nvme_fabrics`, and related modules.
To make it persistent, add to `/etc/modules-load.d/nvme-tcp.conf` or `/etc/modules` depending on your distro.

### 2. Install via Helm (Recommended)

```bash
helm repo add simplyblock-csi https://install.simplyblock.io/helm
helm repo update

export CLUSTER_UUID="<CLUSTER_ID>"
export CLUSTER_SECRET="<CLUSTER_SECRET>"
export CNTR_ADDR="<CONTROL_PLANE_ADDR>"
export POOL_NAME="<STORAGE_POOL_NAME>"

helm install -n simplyblock-csi --create-namespace simplyblock-csi simplyblock-csi/spdk-csi \
  --set csiConfig.simplybk.uuid=${CLUSTER_UUID} \
  --set csiConfig.simplybk.ip=${CNTR_ADDR} \
  --set csiSecret.simplybk.secret=${CLUSTER_SECRET} \
  --set logicalVolume.pool_name=${POOL_NAME}
```

Verify the deployment:

```bash
kubectl -n simplyblock-csi get pods -l release=simplyblock-csi
```

You should see controller and node pods in the `Running` state.

## Storage Cluster Installation in Kubernetes

Simplyblock supports different Kubernetes deployment topologies:

* **Disaggregated Setup**
  Storage nodes on dedicated worker nodes or separate clusters; CSI driver connects client clusters to storage.
  📖 [Docs](https://docs.simplyblock.io/latest/deployments/kubernetes/k8s-disaggregated/)

* **Hyper-Converged Setup**
  CSI driver runs alongside storage and workloads in the same cluster, enabling local storage affinity and simplified topology.
  📖 [Docs](https://docs.simplyblock.io/latest/deployments/kubernetes/k8s-hyperconverged/)

For details, see the [Kubernetes Deployment Guide](https://docs.simplyblock.io/latest/deployments/kubernetes/).

---

## 🖥️ Usage

Once deployed, you can manage persistent volumes using standard Kubernetes objects:

* **StorageClasses**
* **PersistentVolumeClaims**
* **Volume Snapshots**
* **Clones and resizing**
* **QoS policies via CSI driver**

See full examples in the [Usage Guide](https://docs.simplyblock.io/latest/usage/simplyblock-csi/).

---

## 🛠️ Troubleshooting & Operations

* Verify CSI pods are running:

  ```bash
  kubectl -n simplyblock-csi get pods
  ```
* Check logs for a node or controller pod:

  ```bash
  kubectl -n simplyblock-csi logs <pod-name>
  ```

For advanced troubleshooting, monitoring, and maintenance guidance, see the [Operations Docs](https://docs.simplyblock.io/25.7.1/usage/simplyblock-csi/).

---

## 📄 License

This project is licensed under the **Apache 2.0 License** — see the [LICENSE](LICENSE) file for details.

---

## 📚 Documentation

* [Kubernetes Deployment Guide](https://docs.simplyblock.io/latest/deployments/kubernetes/)
* [Install CSI Driver Only](https://docs.simplyblock.io/latest/deployments/kubernetes/install-csi/)
* [Disaggregated Setup](https://docs.simplyblock.io/latest/deployments/kubernetes/k8s-disaggregated/)
* [Hyper-Converged Setup](https://docs.simplyblock.io/latest/deployments/kubernetes/k8s-hyperconverged/)

---

## 🤝 Contributing

We welcome contributions!

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/my-feature`)
3. Commit your changes with a clear message
4. Push to your fork and open a Pull Request

Please review the [CONTRIBUTING.md](CONTRIBUTING.md) for details.

---

## 📬 Support

* 📖 [Documentation](https://docs.simplyblock.io/latest/deployments/kubernetes/)
* 🐞 [GitHub Issues](https://github.com/simplyblock/simplyblock-csi/issues)
* 🌐 [Simplyblock Website](https://www.simplyblock.io)

Maintained by the **simplyblock team**.

---

**Unlock NVMe-grade performance for your Kubernetes workloads with Simplyblock CSI.**
