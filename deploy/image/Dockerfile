# SPDX-License-Identifier: Apache-2.0
# Copyright (c) Arm Limited and Contributors
# Copyright (c) Intel Corporation
#
# XXX: pin alpine to 3.8 with e2fsprogs-1.44
# e2fsprogs-1.45+ crashes my test vm when running mkfs.ext4
FROM alpine:3.18
LABEL maintainers="SPDK-CSI Authors"
LABEL description="SPDK-CSI Plugin"

COPY spdkcsi /usr/local/bin/spdkcsi

RUN apk update && \
    apk add nvme-cli open-iscsi e2fsprogs xfsprogs blkid xfsprogs-extra e2fsprogs-extra util-linux

RUN if [ -f /var/lib/nvme/hostid ]; then \
        cp /var/lib/nvme/hostid /etc/nvme/hostid; \
    else \
        uuidgen > /var/lib/nvme/hostid && \
        cp /var/lib/nvme/hostid /etc/nvme/hostid; \
    fi && \
    echo "nqn.2014-08.org.nvmexpress:uuid:$(cat /etc/nvme/hostid)" > /etc/nvme/hostnqn

ENTRYPOINT ["/usr/local/bin/spdkcsi"]
