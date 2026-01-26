# syntax=docker/dockerfile:1
#FROM simplyblock/spdkcsi:base_image
FROM simplyblock/simplyblock:base_image_313
COPY LICENSE /licenses/LICENSE

COPY spdkcsi /usr/local/bin/spdkcsi

USER simplyblock

ENTRYPOINT ["/usr/local/bin/spdkcsi"]
