# SPDX-License-Identifier: Apache-2.0
# Copyright (c) Arm Limited and Contributors
# Copyright (c) Intel Corporation
#
FROM alpine:3.21
LABEL maintainers="SPDK-CSI Authors"
LABEL description="SPDK-CSI Plugin"

COPY spdkcsi /usr/local/bin/spdkcsi

RUN apk update && \
    apk add nvme-cli open-iscsi e2fsprogs xfsprogs blkid xfsprogs-extra e2fsprogs-extra util-linux

ENTRYPOINT ["/usr/local/bin/spdkcsi"]

