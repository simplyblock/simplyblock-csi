# SPDX-License-Identifier: Apache-2.0
# Copyright (c) Arm Limited and Contributors
# Copyright (c) Intel Corporation
#
# XXX: pin alpine to 3.8 with e2fsprogs-1.44
# e2fsprogs-1.45+ crashes my test vm when running mkfs.ext4
FROM registry.access.redhat.com/ubi9/ubi-minimal:9.3

LABEL name="simplyblock"
LABEL vendor="Simplyblock"
LABEL version="1.0.0"
LABEL release="1"
LABEL summary="Simplyblock csi driver component"
LABEL description="Simplyblock csi driver container"
LABEL maintainer="developers@simplyblock.io"

COPY LICENSE /licenses/LICENSE

COPY spdkcsi /usr/local/bin/spdkcsi

RUN microdnf install -y \
        nvme-cli \
        iscsi-initiator-utils \
        e2fsprogs \
        xfsprogs \
        util-linux \
        sudo \
    && microdnf clean all

RUN useradd -ms /bin/bash simplyblock
RUN echo "simplyblock  ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers
USER simplyblock

ENTRYPOINT ["/usr/local/bin/spdkcsi"]
