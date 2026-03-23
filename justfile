set shell := ["sh", "-eu", "-c"]

test-fast:
    cargo test -p greytl-types
    cargo test -p greytl-source --test source_adapter
