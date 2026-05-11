# SPDX-License-Identifier: Apache-2.0
# Copyright (c) Arm Limited and Contributors
# Copyright (c) Intel Corporation
#
FROM --platform=$BUILDPLATFORM golang:1.26 AS builder
ARG TARGETOS
ARG TARGETARCH

WORKDIR /workspace
COPY go.mod go.sum ./
RUN go mod download
COPY cmd/ cmd/
COPY pkg/ pkg/
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} go build -buildvcs=false -o spdkcsi ./cmd/

FROM alpine:3.21
LABEL maintainers="SPDK-CSI Authors"
LABEL description="SPDK-CSI Plugin"

COPY --from=builder /workspace/spdkcsi /usr/local/bin/spdkcsi

RUN apk update && \
    apk add nvme-cli open-iscsi e2fsprogs xfsprogs blkid xfsprogs-extra e2fsprogs-extra util-linux

ENTRYPOINT ["/usr/local/bin/spdkcsi"]

