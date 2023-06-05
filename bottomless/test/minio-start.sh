#!/usr/bin/env bash

DIRECTORY=$(cd `dirname $0` && pwd)
docker run -dt                                  \
  -p 9000:9000 -p 9090:9090                     \
  -v $DIRECTORY/minio-mnt/      \
  --name "minio_local"                          \
  quay.io/minio/minio server $DIRECTORY/minio-mnt/ --console-address ":9090"