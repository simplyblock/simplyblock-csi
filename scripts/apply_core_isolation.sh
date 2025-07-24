#!/bin/bash

LABEL_KEY="io.simplyblock.node-type"
LABEL_VALUE="simplyblock-storage-plane"
NAMESPACE="${1:-simplyblock}" 
NODES=$(kubectl get nodes -l "${LABEL_KEY}=${LABEL_VALUE}" -o jsonpath='{.items[*].metadata.name}')

for NODE in $NODES; do
  NODE_IP=$(kubectl get node "$NODE" -o jsonpath='{.status.addresses[?(@.type=="InternalIP")].address}')
  SANITIZED_NODE=$(echo "$NODE" | tr '.' '-')
  JOB_NAME="apply-config-${SANITIZED_NODE}"

  cat <<EOF | kubectl apply -f -
apiVersion: batch/v1
kind: Job
metadata:
  name: ${JOB_NAME}
  namespace: ${NAMESPACE}
spec:
  template:
    spec:
      restartPolicy: OnFailure
      nodeSelector:
        kubernetes.io/hostname: ${NODE}
      hostNetwork: true
      hostPID: true
      serviceAccountName: simplyblock-storage-node-sa
      tolerations:
        - effect: NoSchedule
          operator: Exists
        - effect: NoExecute
          operator: Exists
      volumes:
        - name: boot
          hostPath:
            path: /boot
        - name: usr-bin
          hostPath:
            path: /usr/bin
        - name: usr-sbin
          hostPath:
            path: /usr/sbin
        - name: lib
          hostPath:
            path: /lib
        - name: lib64
          hostPath:
            path: /lib64
        - name: usr-lib
          hostPath:
            path: /usr/lib
        - name: dev
          hostPath:
            path: /dev
        - name: run
          hostPath:
            path: /run
        - name: proc
          hostPath:
            path: /proc
        - name: sys
          hostPath:
            path: /sys
        - name: usr-share
          hostPath:
            path: /usr/share
        - name: etc
          hostPath:
            path: /etc
        - name: rootfs
          hostPath:
            path: /
        - name: var-simplyblock
          hostPath:
            path: /var/simplyblock
      containers:
        - name: init-setup
          image: ubuntu:22.04
          securityContext:
            privileged: true
          volumeMounts:
            - name: boot
              mountPath: /boot
            - name: usr-bin
              mountPath: /usr/bin
            - name: usr-sbin
              mountPath: /usr/sbin
            - name: lib
              mountPath: /lib
            - name: lib64
              mountPath: /lib64
            - name: usr-lib
              mountPath: /usr/lib
            - name: dev
              mountPath: /dev
            - name: run
              mountPath: /run
            - name: proc
              mountPath: /proc
            - name: sys
              mountPath: /sys
            - name: usr-share
              mountPath: /usr/share
            - name: etc
              mountPath: /etc
            - name: rootfs
              mountPath: /
            - name: var-simplyblock
              mountPath: /var/simplyblock
          command: ["/bin/bash", "-c"]
          args:
            - |
              set -e

              if [[ -f /etc/os-release ]]; then
                  source /etc/os-release
                  OS_ID=\$ID
              else
                  echo "[ERROR] Unable to detect OS"
                  exit 1
              fi
              echo "[INFO] Detected OS: \$OS_ID"

              case "\$OS_ID" in
                  debian|ubuntu)
                      apt update && apt install -y grep jq nvme-cli tuned
                      ;;
                  centos|rhel|rocky|almalinux)
                      export YUM_RELEASEVER=$(awk -F'=' '/^VERSION_ID=/{gsub(/"/,"",$2); print $2}' /etc/os-release)
                      export DNF_RELEASEVER=$(awk -F'=' '/^VERSION_ID=/{gsub(/"/,"",$2); print $2}' /etc/os-release)
                      dnf --releasever=9 install -y grep jq nvme-cli kernel-modules-extra tuned \
                          --setopt=tsflags=nocontexts,noscripts --setopt=install_weak_deps=False
                      ;;
                  *)
                      echo "[WARN] OS \$OS_ID not supported for automatic NVMe package installation"
                      ;;
              esac

              echo "--- Reading isolated cores from config ---"
              CONFIG_FILE="/var/simplyblock/sn_config_file"

              if [[ ! -f "\$CONFIG_FILE" ]]; then
                  echo "[ERROR] Config file \$CONFIG_FILE not found."
                  exit 1
              fi

              ISOLATED_CORES=\$(jq -r '.isolated_cores | join(",")' "\$CONFIG_FILE")
              if [[ -z "\$ISOLATED_CORES" ]]; then
                  echo "[ERROR] Could not extract isolated cores from \$CONFIG_FILE"
                  exit 1
              fi

              echo "[INFO] Isolated cores to apply: \$ISOLATED_CORES"

              modprobe nvme-tcp
              echo "[INFO] Loaded nvme-tcp module"

              TUNED_PROFILE="isolated-cpu"
              TUNED_PROFILE_DIR="/etc/tuned/\$TUNED_PROFILE"
              TUNED_PROFILE_DIR2="/etc/tuned/profiles/\$TUNED_PROFILE"

              mkdir -p "\$TUNED_PROFILE_DIR"
              mkdir -p "\$TUNED_PROFILE_DIR2"

              cat <<TUNEDCONF > "\$TUNED_PROFILE_DIR/tuned.conf"
              [main]
              include=throughput-performance

              [cpu]
              isolated_cores=\$ISOLATED_CORES

              [bootloader]
              cmdline=isolcpus=\$ISOLATED_CORES nohz_full=\$ISOLATED_CORES rcu_nocbs=\$ISOLATED_CORES
              TUNEDCONF

              cat <<TUNEDCONF2 > "\$TUNED_PROFILE_DIR2/tuned.conf"
              [main]
              include=throughput-performance

              [cpu]
              isolated_cores=\$ISOLATED_CORES

              [bootloader]
              cmdline=isolcpus=\$ISOLATED_CORES nohz_full=\$ISOLATED_CORES rcu_nocbs=\$ISOLATED_CORES
              TUNEDCONF2

              echo "[INFO] Tuned profile created."

              echo "[INFO] Starting tuned daemon in background..."
              /usr/sbin/tuned -l -P &
              TUNED_PID=\$!

              for i in {1..10}; do
                  if tuned-adm active &>/dev/null; then
                      echo "[INFO] Tuned is running."
                      break
                  else
                      echo "[INFO] Waiting for tuned to start... (\$i/10)"
                      sleep 1
                  fi
              done

              if ! tuned-adm active &>/dev/null; then
                  echo "[ERROR] Tuned failed to start. Please check logs."
                  ps aux | grep tuned
                  exit 1
              fi

              echo "[INFO] Applying tuned profile: \$TUNED_PROFILE"
              tuned-adm profile "\$TUNED_PROFILE"
              case "\$OS_ID" in
                  centos|rhel|rocky|almalinux)
                      grubby --update-kernel=ALL --args="isolcpus=\$ISOLATED_CORES nohz_full=\$ISOLATED_CORES rcu_nocbs=\$ISOLATED_CORES"
                      ;;
                  *)
                      echo ""
                      ;;
              esac


              echo "[INFO] Init setup and CPU isolation complete."
              echo "--- Init setup complete ---"
EOF

  echo "Waiting for job ${JOB_NAME} to complete..."
  kubectl wait --for=condition=complete --timeout=300s -n "${NAMESPACE}" job/"${JOB_NAME}"

  echo "Fetching logs from job ${JOB_NAME}..."
  POD_NAME=$(kubectl get pod -n "${NAMESPACE}" --selector=job-name="${JOB_NAME}" -o jsonpath='{.items[0].metadata.name}')
  kubectl logs -n "${NAMESPACE}" "${POD_NAME}"

  echo "Deleting job ${JOB_NAME}..."
  kubectl delete job "${JOB_NAME}" -n "${NAMESPACE}" --ignore-not-found
done
