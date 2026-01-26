# syntax=docker/dockerfile:1
FROM simplyblock/spdkcsi:base_image

COPY LICENSE /licenses/LICENSE

COPY spdkcsi /usr/local/bin/spdkcsi

USER simplyblock

ENTRYPOINT ["/usr/local/bin/spdkcsi"]
