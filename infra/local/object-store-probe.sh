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

probe_key="_iceflow_probe/raw-s3.txt"
probe_body="$(mktemp)"
probe_copy="$(mktemp)"
trap 'rm -rf "$aws_config_dir" "$probe_body" "$probe_copy"' EXIT
printf 'iceflow object store probe\n' > "$probe_body"

aws --endpoint-url "${OBJECT_STORE_ENDPOINT}" s3api put-object \
  --bucket "${OBJECT_STORE_BUCKET}" \
  --key "${probe_key}" \
  --body "${probe_body}" >/dev/null

aws --endpoint-url "${OBJECT_STORE_ENDPOINT}" s3api get-object \
  --bucket "${OBJECT_STORE_BUCKET}" \
  --key "${probe_key}" \
  "${probe_copy}" >/dev/null

cmp -s "${probe_body}" "${probe_copy}"

aws --endpoint-url "${OBJECT_STORE_ENDPOINT}" s3api list-objects-v2 \
  --bucket "${OBJECT_STORE_BUCKET}" \
  --prefix "_iceflow_probe/" | grep -q "\"Key\": \"${probe_key}\""

aws --endpoint-url "${OBJECT_STORE_ENDPOINT}" s3api delete-object \
  --bucket "${OBJECT_STORE_BUCKET}" \
  --key "${probe_key}" >/dev/null

deleted_listing="$(aws --endpoint-url "${OBJECT_STORE_ENDPOINT}" s3api list-objects-v2 \
  --bucket "${OBJECT_STORE_BUCKET}" \
  --prefix "_iceflow_probe/")"
! printf '%s' "${deleted_listing}" | grep -q "\"Key\": \"${probe_key}\""
