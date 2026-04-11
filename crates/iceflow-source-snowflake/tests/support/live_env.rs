use anyhow::{Error, Result};
use std::path::{Path, PathBuf};

pub fn quote_string_literal(value: &str) -> String {
    value.replace('\'', "''")
}

pub fn load_repo_dotenv() -> Result<()> {
    let path = repo_root().join(".env");
    if !path.exists() {
        return Ok(());
    }

    load_dotenv_from(&path)
}

pub fn required_env(name: &str) -> Result<String> {
    std::env::var(name).map_err(|err| Error::msg(format!("missing env var {name}: {err}")))
}

pub fn optional_env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|value| !value.is_empty())
}

pub fn apply_live_auth_env_overrides() {
    let _guard = env_lock().lock().expect("env lock");
    apply_live_auth_env_overrides_locked();
}

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .unwrap_or_else(|_| Path::new(env!("CARGO_MANIFEST_DIR")).join("../.."))
}

fn apply_live_auth_env_overrides_locked() {
    if let Ok(private_key_contents) = load_private_key_pkcs8_value() {
        std::env::set_var("ADBC_SNOWFLAKE_SQL_AUTH_TYPE", "auth_jwt");
        std::env::remove_var("ADBC_SNOWFLAKE_SQL_CLIENT_OPTION_JWT_PRIVATE_KEY");
        std::env::set_var(
            "ADBC_SNOWFLAKE_SQL_CLIENT_OPTION_JWT_PRIVATE_KEY_PKCS8_VALUE",
            private_key_contents,
        );

        if let Some(passphrase) = optional_env("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE") {
            std::env::set_var(
                "ADBC_SNOWFLAKE_SQL_CLIENT_OPTION_JWT_PRIVATE_KEY_PKCS8_PASSWORD",
                passphrase,
            );
        }
    }
}

fn load_private_key_pkcs8_value() -> Result<String> {
    let private_key_path = required_env("SNOWFLAKE_PRIVATE_KEY_PATH")?;
    std::fs::read_to_string(&private_key_path).map_err(|err| {
        Error::msg(format!(
            "failed to read private key file {private_key_path}: {err}"
        ))
    })
}

fn load_dotenv_from(path: &Path) -> Result<()> {
    let _guard = env_lock().lock().expect("env lock");
    dotenvy::from_path_override(path)
        .map_err(|err| Error::msg(format!("failed to load {}: {err}", path.display())))
}

fn env_lock() -> &'static std::sync::Mutex<()> {
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    &ENV_LOCK
}
