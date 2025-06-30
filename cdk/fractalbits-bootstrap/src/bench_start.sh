#!/bin/bash

set -ex

CONF=/opt/fractalbits/etc/bench_workload.yaml
WARP=/opt/fractalbits/bin/warp
region=$(cat $CONF | grep region: | awk '{print $2}')
host=$(cat $CONF | grep host: | awk '{print $2}')
bucket=$(cat $CONF | grep bucket: | awk '{print $2}')
export AWS_DEFAULT_REGION=$region
export AWS_ENDPOINT_URL_S3=http://$host
export AWS_ACCESS_KEY_ID=test_api_key
export AWS_SECRET_ACCESS_KEY=test_api_secret


if ! aws s3api head-bucket --bucket $bucket &>/dev/null; then
  aws s3api create-bucket --bucket $bucket
fi
$WARP run $CONF
