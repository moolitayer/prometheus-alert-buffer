#!/bin/sh
set -e
IMAGENAME=${IMAGENAME:-juliusv/message-buffer}
EXTRACTNAME=${EXTRACTNAME:-message-buffer-extract}

echo Building ${IMAGENAME}:build

docker build -t juliusv/message-buffer:build . -f Dockerfile.build

docker create --name ${EXTRACTNAME} juliusv/message-buffer:build
docker cp ${EXTRACTNAME}:/go/bin/message-buffer message-buffer
docker rm -f ${EXTRACTNAME}

echo Building ${IMAGENAME}:latest

docker build --no-cache -t ${IMAGENAME:-juliusv/message-buffer}:latest .
