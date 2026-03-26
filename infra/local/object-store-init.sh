#!/bin/sh
set -eu

: "${AWS_DEFAULT_REGION:?AWS_DEFAULT_REGION is required}"
: "${OBJECT_STORE_BUCKET:?OBJECT_STORE_BUCKET is required}"
: "${OBJECT_STORE_ENDPOINT:?OBJECT_STORE_ENDPOINT is required}"

aws_config_dir="$(mktemp -d)"
trap 'rm -rf "$aws_config_dir"' EXIT

cat > "${aws_config_dir}/config" <<EOF
[default]
region = ${AWS_DEFAULT_REGION}
s3 =
  addressing_style = path
EOF

export AWS_CONFIG_FILE="${aws_config_dir}/config"
export AWS_EC2_METADATA_DISABLED=true

if aws --endpoint-url "${OBJECT_STORE_ENDPOINT}" s3api head-bucket --bucket "${OBJECT_STORE_BUCKET}" >/dev/null 2>&1; then
  exit 0
fi

aws --endpoint-url "${OBJECT_STORE_ENDPOINT}" s3api create-bucket \
  --bucket "${OBJECT_STORE_BUCKET}" \
  --create-bucket-configuration "LocationConstraint=${AWS_DEFAULT_REGION}" >/dev/null

aws --endpoint-url "${OBJECT_STORE_ENDPOINT}" s3api wait bucket-exists --bucket "${OBJECT_STORE_BUCKET}"
