use anyhow::{Error, Result};
use iceflow_source_snowflake::{
    client::{qualified_table_name, AdbcSnowflakeClient},
    SnowflakeAuthMethod, SnowflakeSourceConfig,
};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

pub struct LiveSnowflakeHarness {
    root: PathBuf,
    destination_root: PathBuf,
    config: SnowflakeSourceConfig,
    schema: String,
}

impl LiveSnowflakeHarness {
    pub fn new() -> Result<Self> {
        load_repo_dotenv()?;
        apply_live_auth_env_overrides();

        let root = next_temp_path("config");
        let destination_root = next_temp_path("warehouse");
        reset_dir(&root)?;
        reset_dir(&destination_root)?;
        fs::create_dir_all(root.join("sources"))
            .map_err(|err| Error::msg(format!("failed to create sources dir: {err}")))?;
        fs::create_dir_all(root.join("destinations"))
            .map_err(|err| Error::msg(format!("failed to create destinations dir: {err}")))?;
        fs::create_dir_all(root.join("connectors"))
            .map_err(|err| Error::msg(format!("failed to create connectors dir: {err}")))?;

        let schema = optional_env("SNOWFLAKE_SCHEMA").unwrap_or_else(|| "PUBLIC".to_string());
        write(
            &root.join("sources/local_snowflake.toml"),
            r#"version = 1
kind = "snowflake"

[properties]
account = "$SNOWFLAKE_ACCOUNT"
user = "$SNOWFLAKE_USER"
warehouse = "$SNOWFLAKE_WAREHOUSE"
role = "$SNOWFLAKE_ROLE"
database = "$SNOWFLAKE_DATABASE"
auth_method = "password"
"#,
        )?;
        write(
            &root.join("destinations/local_fs.toml"),
            &format!(
                "version = 1\nkind = \"filesystem\"\n\n[properties]\nroot_uri = \"{}\"\n",
                destination_root.display()
            ),
        )?;
        write(
            &root.join("connectors/snowflake_customer_state_append.toml"),
            &format!(
                r#"version = 1
source = "local_snowflake"
destination = "local_fs"

[[tables]]
source_schema = "{schema}"
source_table = "CUSTOMER_STATE"
destination_namespace = "customer_state"
destination_table = "customer_state"
table_mode = "append_only"
"#
            ),
        )?;

        Ok(Self {
            root,
            destination_root,
            config: SnowflakeSourceConfig {
                source_label: "local_snowflake".to_string(),
                account: required_env("SNOWFLAKE_ACCOUNT")?,
                user: required_env("SNOWFLAKE_USER")?,
                password: optional_env("SNOWFLAKE_PASSWORD").unwrap_or_default(),
                warehouse: required_env("SNOWFLAKE_WAREHOUSE")?,
                role: required_env("SNOWFLAKE_ROLE")?,
                database: required_env("SNOWFLAKE_DATABASE")?,
                auth_method: SnowflakeAuthMethod::Password,
            },
            schema,
        })
    }

    pub fn config_root(&self) -> PathBuf {
        self.root.clone()
    }

    pub fn connector_config(&self) -> PathBuf {
        self.root
            .join("connectors/snowflake_customer_state_append.toml")
    }

    pub fn reset_table(&self) -> Result<()> {
        let client = self.client()?;
        let table = self.table_name();
        client.exec(&format!(
            "CREATE OR REPLACE TABLE {table} (customer_id STRING NOT NULL, status STRING, updated_at TIMESTAMP_NTZ, CONSTRAINT CUSTOMER_STATE_PK PRIMARY KEY (customer_id))",
        )).map_err(local_error)?;
        client
            .exec(&format!("ALTER TABLE {table} SET CHANGE_TRACKING = TRUE"))
            .map_err(local_error)?;
        client.exec(&format!(
            "INSERT INTO {table} VALUES ('customer-001', 'active', CURRENT_TIMESTAMP()), ('customer-002', 'trial', CURRENT_TIMESTAMP())",
        )).map_err(local_error)?;
        Ok(())
    }

    pub fn insert_customer(&self, customer_id: &str, status: &str) -> Result<()> {
        let client = self.client()?;
        client
            .exec(&format!(
                "INSERT INTO {} VALUES ('{}', '{}', CURRENT_TIMESTAMP())",
                self.table_name(),
                quote_string_literal(customer_id),
                quote_string_literal(status),
            ))
            .map_err(local_error)?;
        Ok(())
    }

    fn client(&self) -> Result<AdbcSnowflakeClient> {
        AdbcSnowflakeClient::connect(self.config.clone()).map_err(local_error)
    }

    fn table_name(&self) -> String {
        qualified_table_name(&self.schema, "CUSTOMER_STATE")
    }
}

impl Drop for LiveSnowflakeHarness {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
        let _ = fs::remove_dir_all(&self.destination_root);
    }
}

fn next_temp_path(label: &str) -> PathBuf {
    static NEXT_ID: AtomicU64 = AtomicU64::new(0);

    std::env::temp_dir().join(format!(
        "iceflow-cli-snowflake-live-{label}-{}-{}",
        std::process::id(),
        NEXT_ID.fetch_add(1, Ordering::Relaxed)
    ))
}

fn reset_dir(path: &Path) -> Result<()> {
    if path.exists() {
        fs::remove_dir_all(path)
            .map_err(|err| Error::msg(format!("failed to reset {}: {err}", path.display())))?;
    }
    fs::create_dir_all(path)
        .map_err(|err| Error::msg(format!("failed to create {}: {err}", path.display())))
}

fn write(path: &Path, contents: &str) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|err| Error::msg(format!("failed to create {}: {err}", parent.display())))?;
    }
    fs::write(path, contents)
        .map_err(|err| Error::msg(format!("failed to write {}: {err}", path.display())))
}

fn quote_string_literal(value: &str) -> String {
    value.replace('\'', "''")
}

fn local_error(err: impl std::fmt::Display) -> Error {
    Error::msg(err.to_string())
}

fn load_repo_dotenv() -> Result<()> {
    let path = repo_root().join(".env");
    if !path.exists() {
        return Ok(());
    }

    load_dotenv_from(&path)
}

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .unwrap_or_else(|_| Path::new(env!("CARGO_MANIFEST_DIR")).join("../.."))
}

fn required_env(name: &str) -> Result<String> {
    std::env::var(name).map_err(|err| Error::msg(format!("missing env var {name}: {err}")))
}

fn optional_env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|value| !value.is_empty())
}

fn apply_live_auth_env_overrides() {
    let _guard = env_lock().lock().expect("env lock");
    apply_live_auth_env_overrides_locked();
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
    fs::read_to_string(&private_key_path).map_err(|err| {
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
