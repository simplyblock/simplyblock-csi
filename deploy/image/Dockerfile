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

FROM simplyblock/spdkcsi:base_image_e2fsprogs

USER root
WORKDIR /app

COPY --from=builder --chmod=0755 /workspace/spdkcsi spdkcsi

ENTRYPOINT ["/app/spdkcsi"]
