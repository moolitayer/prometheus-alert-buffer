FROM        quay.io/prometheus/busybox:latest
MAINTAINER  The Prometheus Authors <prometheus-developers@googlegroups.com>

COPY         message-buffer /bin/message-buffer

EXPOSE       9099
VOLUME       [ "/message-buffer" ]
WORKDIR      /message-buffer
ENTRYPOINT   [ "/bin/message-buffer" ]
