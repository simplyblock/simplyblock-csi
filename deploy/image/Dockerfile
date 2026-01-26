FROM quay.io/centos/centos:stream9 AS rpms

RUN dnf -y install dnf-plugins-core && mkdir -p /rpms
ARG PKGS="nvme-cli iscsi-initiator-utils e2fsprogs xfsprogs util-linux"
RUN dnf -y download --destdir=/rpms --arch=x86_64,noarch $PKGS && dnf -y clean all

FROM registry.access.redhat.com/ubi9/ubi

LABEL name="simplyblock" \
      vendor="Simplyblock" \
      version="1.0.0" \
      release="1" \
      summary="Simplyblock csi driver component" \
      description="Simplyblock csi driver container" \
      maintainer="developers@simplyblock.io"

RUN dnf -y update && \
    dnf -y install kmod sudo bash util-linux && \
    dnf -y clean all

COPY --from=rpms /rpms /tmp/rpms
RUN dnf -y install --allowerasing /tmp/rpms/*.rpm && \
    rm -rf /tmp/rpms && \
    dnf -y clean all

COPY LICENSE /licenses/LICENSE
COPY spdkcsi /usr/local/bin/spdkcsi

RUN useradd -m -s /bin/bash simplyblock && \
    echo "simplyblock ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

USER simplyblock
ENTRYPOINT ["/usr/local/bin/spdkcsi"]
