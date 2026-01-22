# SPDX-License-Identifier: Apache-2.0
# Copyright (c) Arm Limited and Contributors
# Copyright (c) Intel Corporation
#
# XXX: pin alpine to 3.8 with e2fsprogs-1.44
# e2fsprogs-1.45+ crashes my test vm when running mkfs.ext4
FROM fedora:43
LABEL name="simplyblock"
LABEL vendor="Simplyblock"
LABEL version="1.0.0"
LABEL release="1"
LABEL summary="Simplyblock csi driver component"
LABEL description="Simplyblock csi driver container"
LABEL maintainer="developers@simplyblock.io"

COPY LICENSE /licenses/LICENSE

COPY spdkcsi /usr/local/bin/spdkcsi

RUN dnf -y update && \
    dnf -y install \
        nvme-cli \
        iscsi-initiator-utils \
        e2fsprogs \
        xfsprogs \
        util-linux \
        sudo \
    && dnf clean all

RUN useradd -ms /bin/bash simplyblock
RUN echo "simplyblock  ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers
USER simplyblock

ENTRYPOINT ["/usr/local/bin/spdkcsi"]
