use anyhow::{Error, Result};
use std::ffi::OsString;
use std::path::{Path, PathBuf};

// This mirrors the existing cross-crate test-support #[path] pattern. If more
// shared helpers accumulate, move them into an iceflow-test-support dev crate.
#[path = "../../src/env_test_support.rs"]
mod env_test_support;

const ADBC_AUTH_ENV_VARS: &[&str] = &[
    "ADBC_SNOWFLAKE_SQL_AUTH_TYPE",
    "ADBC_SNOWFLAKE_SQL_CLIENT_OPTION_JWT_PRIVATE_KEY",
    "ADBC_SNOWFLAKE_SQL_CLIENT_OPTION_JWT_PRIVATE_KEY_PKCS8_VALUE",
    "ADBC_SNOWFLAKE_SQL_CLIENT_OPTION_JWT_PRIVATE_KEY_PKCS8_PASSWORD",
];

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

#[must_use]
pub struct LiveAuthEnvGuard {
    values: Option<Vec<(&'static str, Option<OsString>)>>,
}

impl LiveAuthEnvGuard {
    fn capture(names: &[&'static str]) -> Self {
        Self {
            values: Some(
                names
                    .iter()
                    .map(|name| (*name, std::env::var_os(name)))
                    .collect(),
            ),
        }
    }

    // Caller must hold env_lock(); Drop handles locking for normal guard cleanup.
    fn restore_under_held_lock(&mut self) {
        if let Some(values) = self.values.take() {
            for (name, value) in values {
                if let Some(value) = value {
                    std::env::set_var(name, value);
                } else {
                    std::env::remove_var(name);
                }
            }
        }
    }
}

impl Drop for LiveAuthEnvGuard {
    fn drop(&mut self) {
        if self.values.is_none() {
            return;
        }

        let _guard = env_lock()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        self.restore_under_held_lock();
    }
}

pub fn apply_live_auth_env_overrides() -> LiveAuthEnvGuard {
    let _guard = env_lock().lock().expect("env lock");
    let auth_env = LiveAuthEnvGuard::capture(ADBC_AUTH_ENV_VARS);
    apply_live_auth_env_overrides_locked();
    auth_env
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

#[cfg(test)]
mod tests {
    use super::*;
    use env_test_support::SavedEnv;

    #[test]
    fn live_auth_env_override_guard_restores_adbc_env_vars() -> Result<()> {
        let _env_lock = env_lock().lock().expect("env lock");
        let names = [
            "SNOWFLAKE_PRIVATE_KEY_PATH",
            "SNOWFLAKE_PRIVATE_KEY_PASSPHRASE",
            "ADBC_SNOWFLAKE_SQL_AUTH_TYPE",
            "ADBC_SNOWFLAKE_SQL_CLIENT_OPTION_JWT_PRIVATE_KEY",
            "ADBC_SNOWFLAKE_SQL_CLIENT_OPTION_JWT_PRIVATE_KEY_PKCS8_VALUE",
            "ADBC_SNOWFLAKE_SQL_CLIENT_OPTION_JWT_PRIVATE_KEY_PKCS8_PASSWORD",
        ];
        let _restore = SavedEnv::capture(&names);
        let key_path = std::env::temp_dir().join(format!(
            "iceflow-live-env-key-{}-{}.p8",
            std::process::id(),
            line!()
        ));
        std::fs::write(
            &key_path,
            "-----BEGIN ENCRYPTED PRIVATE KEY-----\nabc123\n-----END ENCRYPTED PRIVATE KEY-----\n",
        )
        .map_err(|err| Error::msg(format!("failed to write test key: {err}")))?;

        std::env::set_var("SNOWFLAKE_PRIVATE_KEY_PATH", &key_path);
        std::env::set_var("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE", "topsecret");
        std::env::set_var("ADBC_SNOWFLAKE_SQL_AUTH_TYPE", "previous_auth");
        std::env::remove_var("ADBC_SNOWFLAKE_SQL_CLIENT_OPTION_JWT_PRIVATE_KEY");
        std::env::remove_var("ADBC_SNOWFLAKE_SQL_CLIENT_OPTION_JWT_PRIVATE_KEY_PKCS8_VALUE");
        std::env::remove_var("ADBC_SNOWFLAKE_SQL_CLIENT_OPTION_JWT_PRIVATE_KEY_PKCS8_PASSWORD");

        let mut guard = LiveAuthEnvGuard::capture(ADBC_AUTH_ENV_VARS);
        apply_live_auth_env_overrides_locked();
        assert_eq!(
            std::env::var("ADBC_SNOWFLAKE_SQL_AUTH_TYPE")
                .ok()
                .as_deref(),
            Some("auth_jwt")
        );
        assert_eq!(
            std::env::var("ADBC_SNOWFLAKE_SQL_CLIENT_OPTION_JWT_PRIVATE_KEY_PKCS8_VALUE")
                .ok()
                .as_deref(),
            Some(
                "-----BEGIN ENCRYPTED PRIVATE KEY-----\nabc123\n-----END ENCRYPTED PRIVATE KEY-----\n"
            )
        );

        guard.restore_under_held_lock();

        assert_eq!(
            std::env::var("ADBC_SNOWFLAKE_SQL_AUTH_TYPE")
                .ok()
                .as_deref(),
            Some("previous_auth")
        );
        assert_eq!(
            std::env::var("ADBC_SNOWFLAKE_SQL_CLIENT_OPTION_JWT_PRIVATE_KEY_PKCS8_VALUE")
                .ok()
                .as_deref(),
            None
        );
        Ok(())
    }
}
