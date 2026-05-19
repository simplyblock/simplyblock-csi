## Encrypted LVols

Simplyblock logical volumes support encryption at rest by leveraging the
[crypto bdev](https://spdk.io/doc/bdev.html) module via the SPDK Software
Accel framework, which uses AES‑XTS internally.

Encryption is opted into per volume by setting `encryption: "True"` on the
StorageClass. Key management is handled entirely by the storage cluster —
the CSI driver never sees or transports key material, and PVCs do not need
to reference a Kubernetes Secret.

```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: spdkcsi-sc
provisioner: csi.simplyblock.io
parameters:
  ...
  encryption: "True"
```

```yaml
kind: PersistentVolumeClaim
apiVersion: v1
metadata:
  name: spdkcsi-pvc
spec:
  storageClassName: spdkcsi-sc
  ...
```

How the cluster sources keys (e.g. HashiCorp Vault, an internal KMS, etc.)
is an operator‑side configuration concern and transparent to CSI users.
