#!/bin/sh
set -eu

: "${POLARIS_ROOT_CLIENT_ID:?POLARIS_ROOT_CLIENT_ID is required}"
: "${POLARIS_ROOT_CLIENT_SECRET:?POLARIS_ROOT_CLIENT_SECRET is required}"
: "${POLARIS_REALM:?POLARIS_REALM is required}"
: "${POLARIS_CATALOG_NAME:?POLARIS_CATALOG_NAME is required}"
: "${OBJECT_STORE_BUCKET:?OBJECT_STORE_BUCKET is required}"
: "${OBJECT_STORE_API_PORT:?OBJECT_STORE_API_PORT is required}"

apk add --no-cache jq >/dev/null

TOKEN="$(curl --fail-with-body -s -S -X POST http://polaris:8181/api/catalog/v1/oauth/tokens \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  -d "grant_type=client_credentials&client_id=${POLARIS_ROOT_CLIENT_ID}&client_secret=${POLARIS_ROOT_CLIENT_SECRET}&scope=PRINCIPAL_ROLE:ALL" \
  | jq -r '.access_token')"

if [ -z "$TOKEN" ] || [ "$TOKEN" = "null" ]; then
  echo "failed to obtain Polaris token" >&2
  exit 1
fi

PAYLOAD="$(cat <<EOF
{
  "catalog": {
    "name": "${POLARIS_CATALOG_NAME}",
    "type": "INTERNAL",
    "readOnly": false,
    "properties": {
      "default-base-location": "s3://${OBJECT_STORE_BUCKET}"
    },
    "storageConfigInfo": {
      "storageType": "S3",
      "allowedLocations": ["s3://${OBJECT_STORE_BUCKET}"],
      "endpoint": "http://localhost:${OBJECT_STORE_API_PORT}",
      "endpointInternal": "http://object-store:9000",
      "pathStyleAccess": true
    }
  }
}
EOF
)"

STATUS="$(curl -s -o /tmp/polaris-bootstrap.json -w '%{http_code}' -X POST http://polaris:8181/api/management/v1/catalogs \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Accept: application/json" \
  -H "Content-Type: application/json" \
  -H "Polaris-Realm: ${POLARIS_REALM}" \
  -d "$PAYLOAD")"

if [ "$STATUS" != "200" ] && [ "$STATUS" != "201" ] && [ "$STATUS" != "409" ]; then
  echo "failed to create catalog: status ${STATUS}" >&2
  cat /tmp/polaris-bootstrap.json >&2
  exit 1
fi
