use crate::config::SnowflakeSourceConfig;
use adbc_core::{Connection, Statement};
use adbc_snowflake::{connection, database, Driver};
use anyhow::{Error, Result};
use arrow_array::RecordBatchReader;
use arrow_cast::display::{ArrayFormatter, FormatOptions};
use std::sync::{Mutex, MutexGuard};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatementOutcome {
    pub query_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowSet {
    pub query_id: String,
    pub columns: Vec<String>,
    pub rows: Vec<Vec<String>>,
}

// v1 intentionally normalizes ADBC record batches into a string-based RowSet so the
// rest of the source crate can land incrementally. This is a temporary boundary,
// not the long-term Arrow-native design target.
pub trait SnowflakeClient: Send + Sync {
    fn exec(&self, sql: &str) -> Result<StatementOutcome>;
    fn query_rows(&self, sql: &str) -> Result<RowSet>;
}

struct AdbcSession {
    connection: connection::Connection,
    _database: database::Database,
    _driver: Driver,
}

pub struct AdbcSnowflakeClient {
    inner: Mutex<AdbcSession>,
}

impl AdbcSnowflakeClient {
    pub fn connect(config: SnowflakeSourceConfig) -> Result<Self> {
        let mut driver = Driver::try_load()
            .map_err(|err| Error::msg(format!("adbc driver load failed: {err}")))?;
        let database = apply_config_to_database_builder(
            database::Builder::from_env()
                .map_err(|err| Error::msg(format!("adbc database builder init failed: {err}")))?,
            &config,
        )
        .build(&mut driver)
        .map_err(|err| Error::msg(format!("adbc database build failed: {err}")))?;
        let connection = connection::Builder::from_env()
            .map_err(|err| Error::msg(format!("adbc connection builder init failed: {err}")))?
            .build(&database)
            .map_err(|err| Error::msg(format!("adbc connection open failed: {err}")))?;

        Ok(Self {
            inner: Mutex::new(AdbcSession {
                connection,
                _database: database,
                _driver: driver,
            }),
        })
    }

    pub fn exec(&self, sql: &str) -> Result<StatementOutcome> {
        let mut session = self.lock_session()?;
        execute_statement(&mut session.connection, sql)?;
        Ok(StatementOutcome {
            query_id: lookup_last_query_id(&mut session.connection)?,
        })
    }

    pub fn query_rows(&self, sql: &str) -> Result<RowSet> {
        let mut session = self.lock_session()?;
        let (columns, rows) = execute_query_rows(&mut session.connection, sql)?;
        Ok(RowSet {
            query_id: lookup_last_query_id(&mut session.connection)?,
            columns,
            rows,
        })
    }

    fn lock_session(&self) -> Result<MutexGuard<'_, AdbcSession>> {
        self.inner
            .lock()
            .map_err(|_| Error::msg("snowflake ADBC session mutex poisoned"))
    }
}

impl SnowflakeClient for AdbcSnowflakeClient {
    fn exec(&self, sql: &str) -> Result<StatementOutcome> {
        Self::exec(self, sql)
    }

    fn query_rows(&self, sql: &str) -> Result<RowSet> {
        Self::query_rows(self, sql)
    }
}

pub fn quote_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

pub fn qualified_table_name(schema: &str, table: &str) -> String {
    format!("{}.{}", quote_identifier(schema), quote_identifier(table))
}

fn apply_config_to_database_builder(
    builder: database::Builder,
    config: &SnowflakeSourceConfig,
) -> database::Builder {
    let builder = builder
        .with_account(config.account.clone())
        .with_username(config.user.clone())
        .with_database(config.database.clone())
        .with_warehouse(config.warehouse.clone())
        .with_role(config.role.clone());

    if config.password.is_empty() {
        builder
    } else {
        builder.with_password(config.password.clone())
    }
}

fn execute_statement(connection: &mut connection::Connection, sql: &str) -> Result<()> {
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

fn execute_query_rows(
    connection: &mut connection::Connection,
    sql: &str,
) -> Result<(Vec<String>, Vec<Vec<String>>)> {
    let mut statement = connection
        .new_statement()
        .map_err(|err| Error::msg(format!("adbc new_statement failed: {err}")))?;
    statement
        .set_sql_query(sql)
        .map_err(|err| Error::msg(format!("adbc set_sql_query failed: {err}")))?;

    let reader = statement
        .execute()
        .map_err(|err| Error::msg(format!("adbc execute failed: {err}")))?;
    drain_reader_to_rows(reader)
}

fn lookup_last_query_id(connection: &mut connection::Connection) -> Result<String> {
    execute_query_rows_without_query_id(connection, "SELECT LAST_QUERY_ID(-1)")?
        .into_iter()
        .next()
        .and_then(|row| row.into_iter().next())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| Error::msg("Snowflake did not return a query ID"))
}

fn execute_query_rows_without_query_id(
    connection: &mut connection::Connection,
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
    drain_reader_to_rows(reader).map(|(_, rows)| rows)
}

fn drain_reader_to_rows<R>(reader: R) -> Result<(Vec<String>, Vec<Vec<String>>)>
where
    R: RecordBatchReader,
{
    let columns = reader
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect::<Vec<_>>();
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

    Ok((columns, rows))
}

pub fn is_row_returning_statement(sql: &str) -> bool {
    let leading = sql.trim_start();
    let upper = leading
        .split_whitespace()
        .next()
        .unwrap_or_default()
        .to_ascii_uppercase();
    matches!(upper.as_str(), "SELECT" | "SHOW" | "DESCRIBE" | "WITH")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::SnowflakeAuthMethod;

    #[test]
    fn quotes_identifiers() {
        assert_eq!(quote_identifier("A\"B"), "\"A\"\"B\"");
        assert_eq!(
            qualified_table_name("PUBLIC", "CUSTOMER_STATE"),
            "\"PUBLIC\".\"CUSTOMER_STATE\""
        );
    }

    #[test]
    fn applies_config_fields_without_forcing_empty_password() {
        let config = SnowflakeSourceConfig {
            source_label: "local_snowflake".to_string(),
            account: "xy12345.us-east-1".to_string(),
            user: "ICEFLOW_DEMO".to_string(),
            password: String::new(),
            warehouse: "ICEFLOW_WH".to_string(),
            role: "ICEFLOW_ROLE".to_string(),
            database: "SOURCE_DB".to_string(),
            auth_method: SnowflakeAuthMethod::Password,
        };

        let builder = apply_config_to_database_builder(database::Builder::default(), &config);

        assert_eq!(builder.account.as_deref(), Some("xy12345.us-east-1"));
        assert_eq!(builder.username.as_deref(), Some("ICEFLOW_DEMO"));
        assert_eq!(builder.database.as_deref(), Some("SOURCE_DB"));
        assert_eq!(builder.warehouse.as_deref(), Some("ICEFLOW_WH"));
        assert_eq!(builder.role.as_deref(), Some("ICEFLOW_ROLE"));
        assert!(builder.password.is_none());
    }

    #[test]
    fn detects_row_returning_statements() {
        assert!(is_row_returning_statement("SELECT 1"));
        assert!(is_row_returning_statement(" show tables"));
        assert!(is_row_returning_statement(
            "WITH cte AS (SELECT 1) SELECT * FROM cte"
        ));
        assert!(!is_row_returning_statement("INSERT INTO t VALUES (1)"));
    }
}
