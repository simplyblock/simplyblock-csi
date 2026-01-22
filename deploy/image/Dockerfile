# SPDX-License-Identifier: Apache-2.0
# Copyright (c) Arm Limited and Contributors
# Copyright (c) Intel Corporation
#
# XXX: pin alpine to 3.8 with e2fsprogs-1.44
# e2fsprogs-1.45+ crashes my test vm when running mkfs.ext4
FROM alpine:3.18
LABEL name="simplyblock"
LABEL vendor="Simplyblock"
LABEL version="1.0.0"
LABEL release="1"
LABEL summary="Simplyblock csi driver component"
LABEL description="Simplyblock csi driver container"
LABEL maintainer="developers@simplyblock.io"

COPY LICENSE /licenses/LICENSE

COPY spdkcsi /usr/local/bin/spdkcsi

RUN apk update && \
    apk add nvme-cli open-iscsi e2fsprogs xfsprogs blkid xfsprogs-extra e2fsprogs-extra util-linux

RUN useradd -r -u 10001 -g root simplyblock && \
    chown -R 10001:0 /usr/local/bin/spdkcsi /licenses && \
    chmod -R g+rwX /usr/local/bin/spdkcsi /licenses

USER 10001

ENTRYPOINT ["/usr/local/bin/spdkcsi"]
