FROM quay.io/centos/centos:stream9 AS builder
RUN dnf -y update && \
    dnf -y install \
        nvme-cli \
        iscsi-initiator-utils \
        e2fsprogs \
        xfsprogs \
        util-linux \
        && dnf -y clean all

RUN mkdir -p /out/bin /out/lib && \
    for bin in \
        /usr/sbin/nvme \
        /usr/sbin/iscsiadm \
        /usr/sbin/iscsid \
        /usr/sbin/mkfs.ext4 \
        /usr/sbin/fsck.ext4 \
        /usr/sbin/resize2fs \
        /usr/sbin/mkfs.xfs \
        /usr/sbin/xfs_growfs \
        /usr/sbin/blkid \
        /usr/bin/uuidgen \
    ; do \
        cp -v $bin /out/bin/ ; \
        ldd $bin | awk '/=>/ {print $3}' | xargs -I{} cp -v {} /out/lib/ || true ; \
        interp=$(readelf -l $bin | awk '/interpreter/ {print $NF}' | tr -d ']'); \
        [ -n "$interp" ] && cp -v "$interp" /out/lib/ || true ; \
    done
    
FROM registry.access.redhat.com/ubi9/ubi-minimal

LABEL name="simplyblock" \
      vendor="Simplyblock" \
      version="1.0.0" \
      release="1" \
      summary="Simplyblock csi driver component" \
      description="Simplyblock csi driver container" \
      maintainer="developers@simplyblock.io"

RUN microdnf -y update && \
    microdnf -y install \
        kmod \
        util-linux \
        sudo \
        bash \
        rpm \
        && microdnf -y clean all

COPY --from=builder /out/bin/ /usr/sbin/
COPY --from=builder /out/lib/ /lib64/

RUN /usr/sbin/nvme version || true

COPY LICENSE /licenses/LICENSE

COPY spdkcsi /usr/local/bin/spdkcsi

RUN useradd -m -s /bin/bash simplyblock && \
    echo "simplyblock ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

USER simplyblock

ENTRYPOINT ["/usr/local/bin/spdkcsi"]
