use adbc_core::{Connection, Database, Statement};
use adbc_snowflake::{database, Driver};
use anyhow::{Error, Result};
use arrow_cast::display::{ArrayFormatter, FormatOptions};
use iceflow_source_snowflake::{
    client::{
        is_row_returning_statement, qualified_table_name, quote_identifier, AdbcSnowflakeClient,
    },
    config::SnowflakeSourceConfig,
};
use std::path::{Path, PathBuf};

#[test]
#[ignore = "requires live Snowflake credentials"]
fn current_timestamp_query_id_is_usable_for_changes_end_statement() -> Result<()> {
    let client = AdbcSnowflakeClient::connect(live_config()?)?;
    let table = live_probe_table_name();

    client.exec(&format!(
        "CREATE OR REPLACE TABLE {table} (id INT, v STRING)"
    ))?;
    client.exec(&format!("ALTER TABLE {table} SET CHANGE_TRACKING = TRUE"))?;
    let start = client.exec("SELECT CURRENT_TIMESTAMP()")?;
    client.exec(&format!("INSERT INTO {table} VALUES (1, 'between')"))?;
    let end = client.exec("SELECT CURRENT_TIMESTAMP()")?;

    let rows = client.query_rows(&format!(
        "SELECT id, v FROM {table} CHANGES(INFORMATION => DEFAULT) AT (STATEMENT => '{}') END(STATEMENT => '{}')",
        start.query_id,
        end.query_id,
    ))?;

    assert_eq!(
        rows.rows,
        vec![vec!["1".to_string(), "between".to_string()]]
    );
    Ok(())
}

#[test]
#[ignore = "requires live Snowflake credentials"]
fn client_exec_returns_query_id_of_executed_statement() -> Result<()> {
    let client = AdbcSnowflakeClient::connect(live_config()?)?;
    let marker = format!("exec-marker-{}", std::process::id());

    let outcome = client.exec(&format!("SELECT '{marker}' AS marker"))?;
    let rows = client.query_rows(&format!(
        "SELECT marker FROM TABLE(RESULT_SCAN('{}'))",
        outcome.query_id,
    ))?;

    assert_eq!(rows.rows, vec![vec![marker]]);
    Ok(())
}

fn live_probe_table_name() -> String {
    qualified_table_name(
        optional_env("SNOWFLAKE_SCHEMA")
            .as_deref()
            .unwrap_or("PUBLIC"),
        "ICEFLOW_TEST_BOUNDARY",
    )
}

#[test]
#[ignore = "requires live Snowflake credentials"]
fn explicit_jwt_builder_can_open_connection() -> Result<()> {
    load_repo_dotenv()?;
    let mut driver =
        Driver::try_load().map_err(|err| Error::msg(format!("adbc driver load failed: {err}")))?;

    let mut builder = database::Builder::default()
        .with_account(required_env("SNOWFLAKE_ACCOUNT")?)
        .with_username(required_env("SNOWFLAKE_USER")?)
        .with_database(required_env("SNOWFLAKE_DATABASE")?)
        .with_warehouse(required_env("SNOWFLAKE_WAREHOUSE")?)
        .with_role(required_env("SNOWFLAKE_ROLE")?)
        .with_parse_auth_type("auth_jwt")
        .map_err(|err| Error::msg(format!("auth type parse failed: {err}")))?
        .with_jwt_private_key_pkcs8_value(load_private_key_pkcs8_value()?);

    if let Some(passphrase) = optional_env("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE") {
        builder = builder.with_jwt_private_key_pkcs8_password(passphrase);
    }

    let database = builder
        .build(&mut driver)
        .map_err(|err| Error::msg(format!("adbc explicit database build failed: {err}")))?;
    database
        .new_connection()
        .map_err(|err| Error::msg(format!("adbc explicit connection open failed: {err}")))?;

    Ok(())
}

#[test]
#[ignore = "requires live Snowflake credentials"]
fn list_accessible_schemas_for_live_database() -> Result<()> {
    load_repo_dotenv()?;
    let database = required_env("SNOWFLAKE_DATABASE")?;
    let rows = query_rows_without_query_id(&format!(
        "SELECT SCHEMA_NAME FROM {}.INFORMATION_SCHEMA.SCHEMATA ORDER BY SCHEMA_NAME",
        quote_identifier(&database),
    ))?;

    for row in rows {
        println!("{}", row.join(" | "));
    }

    Ok(())
}

#[test]
#[ignore = "requires live Snowflake credentials"]
fn diagnose_last_query_id_offsets() -> Result<()> {
    let mut connection = open_explicit_connection()?;
    let table = live_probe_table_name();

    execute_without_query_id(
        &mut connection,
        &format!("CREATE OR REPLACE TABLE {table} (id INT, v STRING)"),
    )?;
    execute_without_query_id(
        &mut connection,
        &format!("INSERT INTO {table} VALUES (1, 'before')"),
    )?;
    execute_without_query_id(&mut connection, "SELECT CURRENT_TIMESTAMP()")?;

    for offset in [-1, -2, -3, -4] {
        let rows = query_rows_from_connection_without_query_id(
            &mut connection,
            &format!("SELECT LAST_QUERY_ID({offset})"),
        )?;
        println!(
            "offset {offset}: {}",
            rows.first().map(|row| row.join(" | ")).unwrap_or_default()
        );
    }

    Ok(())
}

#[test]
#[ignore = "requires live Snowflake credentials"]
fn diagnose_current_session_is_stable_across_statements() -> Result<()> {
    let mut connection = open_explicit_connection()?;

    let first = query_single_value_from_connection_without_query_id(
        &mut connection,
        "SELECT CURRENT_SESSION()",
    )?;
    let second = query_single_value_from_connection_without_query_id(
        &mut connection,
        "SELECT CURRENT_SESSION()",
    )?;

    execute_without_query_id(&mut connection, "SELECT CURRENT_TIMESTAMP()")?;

    let third = query_single_value_from_connection_without_query_id(
        &mut connection,
        "SELECT CURRENT_SESSION()",
    )?;

    println!("current_session values: {first}, {second}, {third}");
    assert_eq!(first, second);
    assert_eq!(first, third);
    Ok(())
}

#[test]
#[ignore = "requires live Snowflake credentials"]
fn diagnose_last_query_id_candidates_from_single_follow_up_query() -> Result<()> {
    let mut connection = open_explicit_connection()?;
    let table = live_probe_table_name();

    execute_without_query_id(
        &mut connection,
        &format!("CREATE OR REPLACE TABLE {table} (id INT, v STRING)"),
    )?;
    execute_without_query_id(
        &mut connection,
        &format!("ALTER TABLE {table} SET CHANGE_TRACKING = TRUE"),
    )?;
    execute_without_query_id(
        &mut connection,
        &format!("INSERT INTO {table} VALUES (1, 'before')"),
    )?;
    execute_without_query_id(&mut connection, "SELECT CURRENT_TIMESTAMP()")?;

    let rows = query_rows_from_connection_without_query_id(
        &mut connection,
        "SELECT CURRENT_SESSION(), LAST_QUERY_ID(-1), LAST_QUERY_ID(-2), LAST_QUERY_ID(-3), LAST_QUERY_ID(-4)",
    )?;
    let row = rows
        .into_iter()
        .next()
        .ok_or_else(|| Error::msg("LAST_QUERY_ID diagnostic returned no rows"))?;

    println!("candidate row: {}", row.join(" | "));

    let candidates = row
        .into_iter()
        .skip(1)
        .enumerate()
        .filter(|(_, value)| !value.is_empty())
        .collect::<Vec<_>>();

    if candidates.is_empty() {
        return Err(Error::msg(
            "LAST_QUERY_ID diagnostic returned no candidate IDs",
        ));
    }

    for (index, query_id) in candidates {
        let offset = -((index as i32) + 1);
        let result = query_rows_from_connection_without_query_id(
            &mut connection,
            &format!(
                "SELECT id, v FROM {table} CHANGES(INFORMATION => DEFAULT) AT (OFFSET => -60*5) END(STATEMENT => '{}')",
                query_id,
            ),
        );

        match result {
            Ok(rows) => println!(
                "candidate offset {offset} succeeded with {} row(s)",
                rows.len()
            ),
            Err(err) => println!("candidate offset {offset} failed: {err}"),
        }
    }

    Ok(())
}

#[test]
#[ignore = "requires live Snowflake credentials"]
fn diagnose_last_query_id_result_scan_mapping() -> Result<()> {
    let mut connection = open_explicit_connection()?;
    let before_marker = format!("before-boundary-{}", std::process::id());
    let boundary_marker = format!("boundary-{}", std::process::id());

    execute_without_query_id(
        &mut connection,
        &format!("SELECT '{before_marker}' AS marker"),
    )?;
    execute_without_query_id(
        &mut connection,
        &format!("SELECT '{boundary_marker}' AS marker"),
    )?;

    let rows = query_rows_from_connection_without_query_id(
        &mut connection,
        "SELECT LAST_QUERY_ID(-1), LAST_QUERY_ID(-2), LAST_QUERY_ID(-3)",
    )?;
    let row = rows
        .into_iter()
        .next()
        .ok_or_else(|| Error::msg("LAST_QUERY_ID result-scan diagnostic returned no rows"))?;

    println!("candidate ids: {}", row.join(" | "));

    for (index, query_id) in row
        .into_iter()
        .enumerate()
        .filter(|(_, value)| !value.is_empty())
    {
        let offset = -((index as i32) + 1);
        let result = query_rows_from_connection_without_query_id(
            &mut connection,
            &format!("SELECT marker FROM TABLE(RESULT_SCAN('{query_id}'))"),
        )?;
        println!(
            "candidate offset {offset} result_scan: {}",
            result
                .into_iter()
                .map(|row| row.join(" | "))
                .collect::<Vec<_>>()
                .join(", ")
        );
    }

    Ok(())
}

fn query_rows_without_query_id(sql: &str) -> Result<Vec<Vec<String>>> {
    let mut connection = open_explicit_connection()?;
    query_rows_from_connection_without_query_id(&mut connection, sql)
}

fn query_single_value_from_connection_without_query_id(
    connection: &mut adbc_snowflake::connection::Connection,
    sql: &str,
) -> Result<String> {
    let rows = query_rows_from_connection_without_query_id(connection, sql)?;
    let value = rows
        .into_iter()
        .next()
        .and_then(|mut row| row.drain(..).next())
        .ok_or_else(|| Error::msg(format!("query returned no values: {sql}")))?;
    Ok(value)
}

fn open_explicit_connection() -> Result<adbc_snowflake::connection::Connection> {
    load_repo_dotenv()?;
    let mut driver =
        Driver::try_load().map_err(|err| Error::msg(format!("adbc driver load failed: {err}")))?;

    let mut builder = database::Builder::default()
        .with_account(required_env("SNOWFLAKE_ACCOUNT")?)
        .with_username(required_env("SNOWFLAKE_USER")?)
        .with_database(required_env("SNOWFLAKE_DATABASE")?)
        .with_warehouse(required_env("SNOWFLAKE_WAREHOUSE")?)
        .with_role(required_env("SNOWFLAKE_ROLE")?)
        .with_parse_auth_type("auth_jwt")
        .map_err(|err| Error::msg(format!("auth type parse failed: {err}")))?
        .with_jwt_private_key_pkcs8_value(load_private_key_pkcs8_value()?);

    if let Some(passphrase) = optional_env("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE") {
        builder = builder.with_jwt_private_key_pkcs8_password(passphrase);
    }

    let database = builder
        .build(&mut driver)
        .map_err(|err| Error::msg(format!("adbc explicit database build failed: {err}")))?;
    database
        .new_connection()
        .map_err(|err| Error::msg(format!("adbc explicit connection open failed: {err}")))
}

fn execute_without_query_id(
    connection: &mut adbc_snowflake::connection::Connection,
    sql: &str,
) -> Result<()> {
    let mut statement = connection
        .new_statement()
        .map_err(|err| Error::msg(format!("adbc new_statement failed: {err}")))?;
    statement
        .set_sql_query(sql)
        .map_err(|err| Error::msg(format!("adbc set_sql_query failed: {err}")))?;

    if is_row_returning_statement(sql) {
        let reader = statement
            .execute()
            .map_err(|err| Error::msg(format!("adbc execute failed: {err}")))?;
        for batch in reader {
            batch.map_err(|err| Error::msg(format!("adbc row read failed: {err}")))?;
        }
        return Ok(());
    }

    statement
        .execute_update()
        .map_err(|err| Error::msg(format!("adbc execute_update failed: {err}")))?;
    Ok(())
}

fn query_rows_from_connection_without_query_id(
    connection: &mut adbc_snowflake::connection::Connection,
    sql: &str,
) -> Result<Vec<Vec<String>>> {
    let mut statement = connection
        .new_statement()
        .map_err(|err| Error::msg(format!("adbc new_statement failed: {err}")))?;
    statement
        .set_sql_query(sql)
        .map_err(|err| Error::msg(format!("adbc set_sql_query failed: {err}")))?;

    let reader = statement
        .execute()
        .map_err(|err| Error::msg(format!("adbc execute failed: {err}")))?;
    let format_options = FormatOptions::default();
    let mut rows = Vec::new();

    for batch in reader {
        let batch = batch.map_err(|err| Error::msg(format!("adbc row read failed: {err}")))?;
        let formatters = batch
            .columns()
            .iter()
            .map(|column| {
                ArrayFormatter::try_new(column.as_ref(), &format_options)
                    .map_err(|err| Error::msg(format!("adbc formatter creation failed: {err}")))
            })
            .collect::<Result<Vec<_>>>()?;

        for row_index in 0..batch.num_rows() {
            let mut row = Vec::with_capacity(formatters.len());
            for formatter in &formatters {
                row.push(format!("{}", formatter.value(row_index)));
            }
            rows.push(row);
        }
    }

    Ok(rows)
}

fn live_config() -> Result<SnowflakeSourceConfig> {
    load_repo_dotenv()?;
    apply_live_auth_env_overrides();

    Ok(SnowflakeSourceConfig {
        source_label: "live".to_string(),
        account: required_env("SNOWFLAKE_ACCOUNT")?,
        user: required_env("SNOWFLAKE_USER")?,
        password: optional_env("SNOWFLAKE_PASSWORD").unwrap_or_default(),
        warehouse: required_env("SNOWFLAKE_WAREHOUSE")?,
        role: required_env("SNOWFLAKE_ROLE")?,
        database: required_env("SNOWFLAKE_DATABASE")?,
        auth_method: iceflow_source_snowflake::config::SnowflakeAuthMethod::Password,
    })
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
    std::fs::read_to_string(&private_key_path).map_err(|err| {
        Error::msg(format!(
            "failed to read private key file {private_key_path}: {err}"
        ))
    })
}

#[test]
fn loads_env_values_from_dotenv_file() -> Result<()> {
    let unique = format!("ICEFLOW_TEST_{}_{}", std::process::id(), line!());
    let root = std::env::temp_dir().join(format!("iceflow-dotenv-{unique}"));
    std::fs::create_dir_all(&root).map_err(|err| {
        Error::msg(format!(
            "failed to create temp dir {}: {err}",
            root.display()
        ))
    })?;
    let dotenv_path = root.join(".env");
    std::fs::write(&dotenv_path, format!("{unique}=loaded-from-dotenv\n"))
        .map_err(|err| Error::msg(format!("failed to write {}: {err}", dotenv_path.display())))?;

    load_dotenv_from(&dotenv_path)?;

    assert_eq!(
        std::env::var(&unique).ok().as_deref(),
        Some("loaded-from-dotenv")
    );
    Ok(())
}

fn load_dotenv_from(path: &Path) -> Result<()> {
    // This mutates process-global environment variables; keep lock ownership here
    // so call sites cannot accidentally load .env concurrently.
    let _guard = env_lock().lock().expect("env lock");
    dotenvy::from_path_override(path)
        .map_err(|err| Error::msg(format!("failed to load {}: {err}", path.display())))
}

#[test]
fn maps_private_key_path_to_pkcs8_value_env_vars() {
    let _guard = env_lock().lock().expect("env lock");

    let key_path = std::env::temp_dir().join(format!(
        "iceflow-test-key-{}-{}.p8",
        std::process::id(),
        line!()
    ));
    std::fs::write(
        &key_path,
        "-----BEGIN ENCRYPTED PRIVATE KEY-----\nabc123\n-----END ENCRYPTED PRIVATE KEY-----\n",
    )
    .expect("write temp key");

    std::env::set_var("SNOWFLAKE_PRIVATE_KEY_PATH", &key_path);
    std::env::set_var("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE", "topsecret");
    std::env::remove_var("ADBC_SNOWFLAKE_SQL_AUTH_TYPE");
    std::env::remove_var("ADBC_SNOWFLAKE_SQL_CLIENT_OPTION_JWT_PRIVATE_KEY");
    std::env::remove_var("ADBC_SNOWFLAKE_SQL_CLIENT_OPTION_JWT_PRIVATE_KEY_PKCS8_VALUE");
    std::env::remove_var("ADBC_SNOWFLAKE_SQL_CLIENT_OPTION_JWT_PRIVATE_KEY_PKCS8_PASSWORD");

    apply_live_auth_env_overrides_locked();

    assert_eq!(
        std::env::var("ADBC_SNOWFLAKE_SQL_AUTH_TYPE")
            .ok()
            .as_deref(),
        Some("auth_jwt")
    );
    assert_eq!(
        std::env::var("ADBC_SNOWFLAKE_SQL_CLIENT_OPTION_JWT_PRIVATE_KEY")
            .ok()
            .as_deref(),
        None
    );
    assert_eq!(
        std::env::var("ADBC_SNOWFLAKE_SQL_CLIENT_OPTION_JWT_PRIVATE_KEY_PKCS8_VALUE")
            .ok()
            .as_deref(),
        Some(
            "-----BEGIN ENCRYPTED PRIVATE KEY-----\nabc123\n-----END ENCRYPTED PRIVATE KEY-----\n"
        )
    );
    assert_eq!(
        std::env::var("ADBC_SNOWFLAKE_SQL_CLIENT_OPTION_JWT_PRIVATE_KEY_PKCS8_PASSWORD")
            .ok()
            .as_deref(),
        Some("topsecret")
    );
}

fn env_lock() -> &'static std::sync::Mutex<()> {
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    &ENV_LOCK
}
