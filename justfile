set shell := ["sh", "-eu", "-c"]

test-fast:
    cargo test -p greytl-types
    cargo test -p greytl-source --test source_adapter

stack-config:
    test -f infra/local/.env || cp infra/local/.env.example infra/local/.env
    docker compose --env-file infra/local/.env -f infra/local/docker-compose.yml config >/dev/null

stack-up:
    test -f infra/local/.env || cp infra/local/.env.example infra/local/.env
    docker compose --env-file infra/local/.env -f infra/local/docker-compose.yml up -d --wait

stack-down:
    test -f infra/local/.env || cp infra/local/.env.example infra/local/.env
    docker compose --env-file infra/local/.env -f infra/local/docker-compose.yml down -v

test-real-stack:
    test -f infra/local/.env || cp infra/local/.env.example infra/local/.env
    . infra/local/.env
    POLARIS_CATALOG_URI="http://127.0.0.1:${POLARIS_API_PORT}/api/catalog" \
    POLARIS_CATALOG_NAME="${POLARIS_CATALOG_NAME}" \
    POLARIS_NAMESPACE="${POLARIS_NAMESPACE}" \
    POLARIS_CLIENT_ID="${POLARIS_ROOT_CLIENT_ID}" \
    POLARIS_CLIENT_SECRET="${POLARIS_ROOT_CLIENT_SECRET}" \
    cargo test -p greytl-sink real_stack_ -- --ignored
