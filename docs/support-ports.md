# Supported Port for eks or ks3 for storage-node

|  Port          |   Protocol    | Description 
| -------------- | ------------- | -------------
| 6443           | TCP           | Kubernetes API server. Required for communication between the Kubernetes control plane and the nodes in the cluster.
| 22             | TCP           | SSH access to the instances. Necessary for administrative access and management.
| 8080           | TCP           | SPDK Proxy for the storage node. Facilitates communication between the storage nodes and the management node.
| 2375           | TCP           | Docker Engine API. Allows the management node to communicate with Docker engines running on other nodes.
|  -             | ICMP          | Allows ICMP Echo requests. Used for ping operations to check the availability and responsiveness of management nodes.
|  5000          | TCP           | Storage node. Enables communication with storage-node services running on the node.
|  4420          | TCP           | Storage node logical volume (lvol) connection - this port must be open (1) btw. all of the workers hosting storage plane pods (2) from all workers with pods connecting to storage to any workers hosting storage plane pods and any external storage nodes.
|  53            | UDP           | DNS resolution from worker nodes. Necessary for resolving internal DNS queries within the cluster.
|  10250-10255   | TCP           | Kubernetes node communication. Used for kubelet API communication between the nodes.
|  1025-65535	 | UDP           | Ephemeral ports for UDP traffic. Required for certain network protocols and services.
