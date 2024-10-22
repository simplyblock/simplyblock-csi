# SPDX-License-Identifier: Apache-2.0
# Copyright (c) Arm Limited and Contributors
# Copyright (c) Intel Corporation
#
# XXX: pin alpine to 3.8 with e2fsprogs-1.44
# e2fsprogs-1.45+ crashes my test vm when running mkfs.ext4
FROM ubuntu:20.04
LABEL maintainers="SPDK-CSI Authors"
LABEL description="SPDK-CSI Plugin"

COPY spdkcsi /usr/local/bin/spdkcsi

# Update package lists and install necessary packages
RUN apt-get update && \
    apt-get install -y nvme-cli open-iscsi e2fsprogs xfsprogs blkid && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

ENTRYPOINT ["/usr/local/bin/spdkcsi"]

