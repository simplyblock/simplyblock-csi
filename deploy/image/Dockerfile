# SPDX-License-Identifier: Apache-2.0
# Copyright (c) Arm Limited and Contributors
# Copyright (c) Intel Corporation
#
# XXX: pin alpine to 3.8 with e2fsprogs-1.44
# e2fsprogs-1.45+ crashes my test vm when running mkfs.ext4
FROM simplyblock/spdkcsi:base_image_rhel


USER simplyblock
WORKDIR /app

COPY spdkcsi .
RUN sudo chown simplyblock:simplyblock /app/spdkcsi
RUN sudo chmod +x /app/spdkcsi

ENTRYPOINT ["sudo", "-E", "/app/spdkcsi"]
