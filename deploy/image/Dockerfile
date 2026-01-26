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

FROM simplyblock/spdkcsi:base_image_rhel

USER simplyblock
WORKDIR /app

COPY --from=builder /workspace/spdkcsi spdkcsi
RUN sudo chown simplyblock:simplyblock /app/spdkcsi
RUN sudo chmod +x /app/spdkcsi

ENTRYPOINT ["sudo", "-E", "/app/spdkcsi"]
