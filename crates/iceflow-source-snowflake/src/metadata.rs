use anyhow::{Error, Result};
use std::collections::{BTreeSet, VecDeque};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableMetadata {
    pub primary_keys: Vec<String>,
    pub columns: Vec<String>,
    pub schema_fingerprint: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StreamGrantValidation {
    pub database_privilege: String,
    pub schema_privilege: String,
    pub table_privilege: String,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RoleGrant {
    privilege: String,
    granted_on: String,
    name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum GrantPrincipal {
    Role(String),
    DatabaseRole(String),
}

pub fn load_table_metadata(
    client: &dyn crate::client::SnowflakeClient,
    binding: &crate::binding::SnowflakeConnectorBinding,
) -> Result<TableMetadata> {
    let primary_keys = load_primary_keys(client, &binding.source_schema, &binding.source_table)?;
    if primary_keys.is_empty() {
        return Err(Error::msg("Snowflake v1 requires a declared primary key"));
    }

    let described = describe_table(client, binding)?;
    let columns = described
        .rows
        .iter()
        .filter_map(|row| row.first().cloned())
        .filter(|column| !column.trim().is_empty())
        .collect::<Vec<_>>();
    if columns.is_empty() {
        return Err(Error::msg("Snowflake table metadata returned no columns"));
    }
    let schema_fingerprint = schema_fingerprint(binding, &described.rows);

    Ok(TableMetadata {
        primary_keys,
        columns,
        schema_fingerprint,
    })
}

pub(crate) fn load_schema_fingerprint(
    client: &dyn crate::client::SnowflakeClient,
    binding: &crate::binding::SnowflakeConnectorBinding,
) -> Result<String> {
    let described = describe_table(client, binding)?;
    Ok(schema_fingerprint(binding, &described.rows))
}

pub(crate) fn validate_stream_grants(
    client: &dyn crate::client::SnowflakeClient,
    role: &str,
    database: &str,
    binding: &crate::binding::SnowflakeConnectorBinding,
) -> Result<StreamGrantValidation> {
    let grants = load_effective_role_grants(client, role)?;
    let database_names = database_name_candidates(database);
    let schema_names = object_name_candidates(database, &binding.source_schema, None);
    let table_names = object_name_candidates(
        database,
        &binding.source_schema,
        Some(&binding.source_table),
    );

    let database_privilege = matching_privilege(
        &grants,
        "DATABASE",
        &database_names,
        &["USAGE", "OWNERSHIP"],
    );
    let schema_privilege = matching_privilege(
        &grants,
        "SCHEMA",
        &schema_names,
        &["CREATE STREAM", "OWNERSHIP"],
    );
    let table_privilege =
        matching_privilege(&grants, "TABLE", &table_names, &["SELECT", "OWNERSHIP"]);

    let (database_privilege, schema_privilege, table_privilege) =
        match (database_privilege, schema_privilege, table_privilege) {
            (Some(database_privilege), Some(schema_privilege), Some(table_privilege)) => {
                (database_privilege, schema_privilege, table_privilege)
            }
            (database_privilege, schema_privilege, table_privilege) => {
                let mut missing = Vec::new();
                if database_privilege.is_none() {
                    missing.push(format!("database {database} requires USAGE or OWNERSHIP"));
                }
                if schema_privilege.is_none() {
                    missing.push(format!(
                        "schema {} requires CREATE STREAM or OWNERSHIP",
                        binding.source_schema
                    ));
                }
                if table_privilege.is_none() {
                    missing.push(format!(
                        "table {}.{} requires SELECT or OWNERSHIP",
                        binding.source_schema, binding.source_table
                    ));
                }
                return Err(Error::msg(format!(
                    "Snowflake role '{}' lacks stream bootstrap grants: {}",
                    role,
                    missing.join("; ")
                )));
            }
        };

    let mut warnings = Vec::new();
    if table_privilege != "OWNERSHIP" {
        warnings.push(
            "Snowflake grant check is side-effect-free and cannot prove Snowflake change tracking is already enabled; table OWNERSHIP may be required for initial stream creation when change tracking is disabled"
                .to_string(),
        );
    }

    Ok(StreamGrantValidation {
        database_privilege,
        schema_privilege,
        table_privilege,
        warnings,
    })
}

fn describe_table(
    client: &dyn crate::client::SnowflakeClient,
    binding: &crate::binding::SnowflakeConnectorBinding,
) -> Result<crate::client::RowSet> {
    client.query_rows(&format!(
        "DESCRIBE TABLE {}",
        crate::client::qualified_table_name(&binding.source_schema, &binding.source_table),
    ))
}

fn schema_fingerprint(
    binding: &crate::binding::SnowflakeConnectorBinding,
    rows: &[Vec<String>],
) -> String {
    format!(
        "{}:{}:{}",
        binding.source_schema,
        binding.source_table,
        rows.iter()
            .map(|row| row.join("|"))
            .collect::<Vec<_>>()
            .join(";"),
    )
}

fn load_primary_keys(
    client: &dyn crate::client::SnowflakeClient,
    source_schema: &str,
    source_table: &str,
) -> Result<Vec<String>> {
    let show = client.exec(&format!(
        "SHOW PRIMARY KEYS IN TABLE {}",
        crate::client::qualified_table_name(source_schema, source_table),
    ))?;
    let rows = client.query_rows(&format!(
        "SELECT \"column_name\" FROM TABLE(RESULT_SCAN('{}')) ORDER BY \"key_sequence\"",
        crate::client::quote_literal_value(&show.query_id),
    ))?;

    Ok(rows
        .rows
        .into_iter()
        .filter_map(|row| row.first().cloned())
        .filter(|column| !column.trim().is_empty())
        .collect())
}

fn load_role_grants(
    client: &dyn crate::client::SnowflakeClient,
    role: &str,
) -> Result<Vec<RoleGrant>> {
    let show = client.exec(&format!(
        "SHOW GRANTS TO ROLE {}",
        crate::client::quote_identifier(role),
    ))?;
    let rows = client.query_rows(&format!(
        "SELECT UPPER(\"privilege\") AS \"privilege\", UPPER(\"granted_on\") AS \"granted_on\", UPPER(\"name\") AS \"name\" FROM TABLE(RESULT_SCAN('{}'))",
        crate::client::quote_literal_value(&show.query_id),
    ))?;
    grants_from_row_set(rows)
}

fn load_database_role_grants(
    client: &dyn crate::client::SnowflakeClient,
    database_role: &str,
) -> Result<Vec<RoleGrant>> {
    let show = client.exec(&format!(
        "SHOW GRANTS TO DATABASE ROLE {}",
        quote_qualified_identifier(database_role),
    ))?;
    let rows = client.query_rows(&format!(
        "SELECT UPPER(\"privilege\") AS \"privilege\", UPPER(\"granted_on\") AS \"granted_on\", UPPER(\"name\") AS \"name\" FROM TABLE(RESULT_SCAN('{}'))",
        crate::client::quote_literal_value(&show.query_id),
    ))?;
    grants_from_row_set(rows)
}

fn load_effective_role_grants(
    client: &dyn crate::client::SnowflakeClient,
    root_role: &str,
) -> Result<Vec<RoleGrant>> {
    let mut visited = BTreeSet::new();
    let mut pending = VecDeque::from([GrantPrincipal::Role(normalize_object_name(root_role))]);
    let mut effective_grants = Vec::new();

    while let Some(principal) = pending.pop_front() {
        if !visited.insert(principal.clone()) {
            continue;
        }

        let grants = match &principal {
            GrantPrincipal::Role(role) => load_role_grants(client, role)?,
            GrantPrincipal::DatabaseRole(database_role) => {
                load_database_role_grants(client, database_role)?
            }
        };
        for inherited_principal in grants.iter().filter_map(inherited_grant_principal) {
            if !visited.contains(&inherited_principal) {
                pending.push_back(inherited_principal);
            }
        }
        effective_grants.extend(grants);
    }

    Ok(effective_grants)
}

fn inherited_grant_principal(grant: &RoleGrant) -> Option<GrantPrincipal> {
    match grant.granted_on.as_str() {
        "ROLE" => Some(GrantPrincipal::Role(grant.name.clone())),
        "DATABASE ROLE" => Some(GrantPrincipal::DatabaseRole(grant.name.clone())),
        _ => None,
    }
}

fn matching_privilege(
    grants: &[RoleGrant],
    granted_on: &str,
    object_names: &BTreeSet<String>,
    accepted_privileges: &[&str],
) -> Option<String> {
    grants
        .iter()
        .find(|grant| {
            grant.granted_on == granted_on
                && object_names.contains(&grant.name)
                && accepted_privileges
                    .iter()
                    .any(|privilege| grant.privilege == *privilege)
        })
        .map(|grant| grant.privilege.clone())
}

fn object_name_candidates(database: &str, schema: &str, table: Option<&str>) -> BTreeSet<String> {
    let database = normalize_object_name(database);
    let schema = normalize_object_name(schema);
    let table = table.map(normalize_object_name);
    let mut names = BTreeSet::new();

    match table {
        Some(table) => {
            names.insert(table.clone());
            names.insert(format!("{schema}.{table}"));
            if !database.is_empty() {
                names.insert(format!("{database}.{schema}.{table}"));
            }
        }
        None => {
            names.insert(schema.clone());
            if !database.is_empty() {
                names.insert(format!("{database}.{schema}"));
            }
        }
    }

    names
}

fn database_name_candidates(database: &str) -> BTreeSet<String> {
    BTreeSet::from([normalize_object_name(database)])
}

fn normalized_cell(row: &[String], index: usize, column: &str) -> Result<String> {
    row.get(index)
        .map(|value| value.trim().to_ascii_uppercase())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| Error::msg(format!("Snowflake grant row missing {column}")))
}

fn normalized_object_name_cell(row: &[String], index: usize) -> Result<String> {
    row.get(index)
        .map(|value| normalize_object_name(value))
        .filter(|value| !value.is_empty())
        .ok_or_else(|| Error::msg("Snowflake grant row missing name"))
}

fn normalize_object_name(value: &str) -> String {
    value
        .trim()
        .replace('"', "")
        .split('.')
        .map(|part| part.trim().to_ascii_uppercase())
        .collect::<Vec<_>>()
        .join(".")
}

fn column_index(columns: &[String], name: &str) -> Result<usize> {
    columns
        .iter()
        .position(|column| column.eq_ignore_ascii_case(name))
        .ok_or_else(|| Error::msg(format!("Snowflake grant query missing {name} column")))
}

fn grants_from_row_set(rows: crate::client::RowSet) -> Result<Vec<RoleGrant>> {
    let privilege_index = column_index(&rows.columns, "privilege")?;
    let granted_on_index = column_index(&rows.columns, "granted_on")?;
    let name_index = column_index(&rows.columns, "name")?;

    rows.rows
        .into_iter()
        .map(|row| {
            Ok(RoleGrant {
                privilege: normalized_cell(&row, privilege_index, "privilege")?,
                granted_on: normalized_cell(&row, granted_on_index, "granted_on")?,
                name: normalized_object_name_cell(&row, name_index)?,
            })
        })
        .collect()
}

fn quote_qualified_identifier(value: &str) -> String {
    value
        .split('.')
        .map(crate::client::quote_identifier)
        .collect::<Vec<_>>()
        .join(".")
}

#[cfg(test)]
mod tests {
    use crate::client::{RowSet, SnowflakeClient, StatementOutcome};

    struct FakeMetadataClient;

    impl SnowflakeClient for FakeMetadataClient {
        fn exec(&self, sql: &str) -> anyhow::Result<StatementOutcome> {
            assert!(sql.starts_with("SHOW PRIMARY KEYS"));
            Ok(StatementOutcome {
                query_id: "show-pk-query".to_string(),
            })
        }

        fn query_rows(&self, sql: &str) -> anyhow::Result<RowSet> {
            if sql.contains("RESULT_SCAN('show-pk-query')") {
                return Ok(RowSet {
                    query_id: "pk-query".to_string(),
                    columns: vec!["COLUMN_NAME".to_string()],
                    rows: vec![vec!["CUSTOMER_ID".to_string()]],
                });
            }

            if sql.starts_with("DESCRIBE TABLE") {
                return Ok(RowSet {
                    query_id: "describe-query".to_string(),
                    columns: Vec::new(),
                    rows: vec![vec![
                        "CUSTOMER_ID".to_string(),
                        "VARCHAR".to_string(),
                        "COLUMN".to_string(),
                    ]],
                });
            }

            panic!("unexpected metadata query: {sql}");
        }
    }

    #[test]
    fn load_table_metadata_requires_primary_key() {
        let binding =
            crate::SnowflakeConnectorBinding::from_request(crate::SnowflakeBindingRequest {
                connector_name: "snowflake_customer_state_append".to_string(),
                tables: vec![crate::SnowflakeTableBinding {
                    source_schema: "PUBLIC".to_string(),
                    source_table: "CUSTOMER_STATE".to_string(),
                    destination_namespace: "customer_state".to_string(),
                    destination_table: "customer_state".to_string(),
                    table_mode: "append_only".to_string(),
                }],
                durable_checkpoint: None,
            })
            .expect("binding");

        let metadata = super::load_table_metadata(&FakeMetadataClient, &binding).expect("metadata");

        assert_eq!(metadata.primary_keys, vec!["CUSTOMER_ID".to_string()]);
        assert_eq!(metadata.columns, vec!["CUSTOMER_ID".to_string()]);
        assert!(metadata.schema_fingerprint.contains("CUSTOMER_STATE"));
    }

    #[test]
    fn validate_stream_grants_follows_inherited_roles_and_avoids_cycles() {
        let binding =
            crate::SnowflakeConnectorBinding::from_request(crate::SnowflakeBindingRequest {
                connector_name: "snowflake_customer_state_append".to_string(),
                tables: vec![crate::SnowflakeTableBinding {
                    source_schema: "PUBLIC".to_string(),
                    source_table: "CUSTOMER_STATE".to_string(),
                    destination_namespace: "customer_state".to_string(),
                    destination_table: "customer_state".to_string(),
                    table_mode: "append_only".to_string(),
                }],
                durable_checkpoint: None,
            })
            .expect("binding");

        let validation = super::validate_stream_grants(
            &InheritedGrantClient,
            "ICEFLOW_ROLE",
            "SOURCE_DB",
            &binding,
        )
        .expect("inherited grants should satisfy stream bootstrap grants");

        assert_eq!(validation.database_privilege, "USAGE");
        assert_eq!(validation.schema_privilege, "CREATE STREAM");
        assert_eq!(validation.table_privilege, "SELECT");
    }

    #[test]
    fn validate_stream_grants_rejects_missing_database_usage() {
        let binding = test_binding();

        let err = super::validate_stream_grants(
            &NoDatabaseGrantClient,
            "ICEFLOW_ROLE",
            "SOURCE_DB",
            &binding,
        )
        .expect_err("database grant is required");

        assert!(err.to_string().contains("database SOURCE_DB"));
        assert!(err.to_string().contains("USAGE or OWNERSHIP"));
    }

    #[test]
    fn validate_stream_grants_follows_database_role_grants() {
        let binding = test_binding();

        let validation = super::validate_stream_grants(
            &DatabaseRoleGrantClient,
            "ICEFLOW_ROLE",
            "SOURCE_DB",
            &binding,
        )
        .expect("database role grants should satisfy stream bootstrap grants");

        assert_eq!(validation.database_privilege, "USAGE");
        assert_eq!(validation.schema_privilege, "CREATE STREAM");
        assert_eq!(validation.table_privilege, "SELECT");
    }

    fn test_binding() -> crate::SnowflakeConnectorBinding {
        crate::SnowflakeConnectorBinding::from_request(crate::SnowflakeBindingRequest {
            connector_name: "snowflake_customer_state_append".to_string(),
            tables: vec![crate::SnowflakeTableBinding {
                source_schema: "PUBLIC".to_string(),
                source_table: "CUSTOMER_STATE".to_string(),
                destination_namespace: "customer_state".to_string(),
                destination_table: "customer_state".to_string(),
                table_mode: "append_only".to_string(),
            }],
            durable_checkpoint: None,
        })
        .expect("binding")
    }

    struct NoDatabaseGrantClient;

    impl SnowflakeClient for NoDatabaseGrantClient {
        fn exec(&self, sql: &str) -> anyhow::Result<StatementOutcome> {
            assert_eq!(sql, "SHOW GRANTS TO ROLE \"ICEFLOW_ROLE\"");
            Ok(StatementOutcome {
                query_id: "grants-iceflow-role".to_string(),
            })
        }

        fn query_rows(&self, sql: &str) -> anyhow::Result<RowSet> {
            assert!(sql.contains("RESULT_SCAN('grants-iceflow-role')"));
            Ok(grant_row_set(vec![
                vec!["CREATE STREAM", "SCHEMA", "SOURCE_DB.PUBLIC"],
                vec!["SELECT", "TABLE", "SOURCE_DB.PUBLIC.CUSTOMER_STATE"],
            ]))
        }
    }

    struct DatabaseRoleGrantClient;

    impl SnowflakeClient for DatabaseRoleGrantClient {
        fn exec(&self, sql: &str) -> anyhow::Result<StatementOutcome> {
            let query_id = match sql {
                "SHOW GRANTS TO ROLE \"ICEFLOW_ROLE\"" => "grants-iceflow-role",
                "SHOW GRANTS TO DATABASE ROLE \"SOURCE_DB\".\"ACCESS_DB_ROLE\"" => {
                    "grants-database-role"
                }
                other => panic!("unexpected grant query: {other}"),
            };
            Ok(StatementOutcome {
                query_id: query_id.to_string(),
            })
        }

        fn query_rows(&self, sql: &str) -> anyhow::Result<RowSet> {
            if sql.contains("RESULT_SCAN('grants-iceflow-role')") {
                return Ok(grant_row_set(vec![vec![
                    "USAGE",
                    "DATABASE ROLE",
                    "SOURCE_DB.ACCESS_DB_ROLE",
                ]]));
            }

            if sql.contains("RESULT_SCAN('grants-database-role')") {
                return Ok(grant_row_set(vec![
                    vec!["USAGE", "DATABASE", "SOURCE_DB"],
                    vec!["CREATE STREAM", "SCHEMA", "SOURCE_DB.PUBLIC"],
                    vec!["SELECT", "TABLE", "SOURCE_DB.PUBLIC.CUSTOMER_STATE"],
                ]));
            }

            panic!("unexpected grant rows query: {sql}");
        }
    }

    struct InheritedGrantClient;

    impl SnowflakeClient for InheritedGrantClient {
        fn exec(&self, sql: &str) -> anyhow::Result<StatementOutcome> {
            let query_id = match sql {
                "SHOW GRANTS TO ROLE \"ICEFLOW_ROLE\"" => "grants-iceflow-role",
                "SHOW GRANTS TO ROLE \"ACCESS_ROLE\"" => "grants-access-role",
                other => panic!("unexpected grant query: {other}"),
            };
            Ok(StatementOutcome {
                query_id: query_id.to_string(),
            })
        }

        fn query_rows(&self, sql: &str) -> anyhow::Result<RowSet> {
            let rows = if sql.contains("RESULT_SCAN('grants-iceflow-role')") {
                vec![vec![
                    "USAGE".to_string(),
                    "ROLE".to_string(),
                    "ACCESS_ROLE".to_string(),
                ]]
            } else if sql.contains("RESULT_SCAN('grants-access-role')") {
                vec![
                    vec![
                        "USAGE".to_string(),
                        "DATABASE".to_string(),
                        "SOURCE_DB".to_string(),
                    ],
                    vec![
                        "USAGE".to_string(),
                        "ROLE".to_string(),
                        "ICEFLOW_ROLE".to_string(),
                    ],
                    vec![
                        "CREATE STREAM".to_string(),
                        "SCHEMA".to_string(),
                        "SOURCE_DB.PUBLIC".to_string(),
                    ],
                    vec![
                        "SELECT".to_string(),
                        "TABLE".to_string(),
                        "SOURCE_DB.PUBLIC.CUSTOMER_STATE".to_string(),
                    ],
                ]
            } else {
                panic!("unexpected grant rows query: {sql}");
            };

            Ok(RowSet {
                query_id: "grants-query".to_string(),
                columns: vec![
                    "privilege".to_string(),
                    "granted_on".to_string(),
                    "name".to_string(),
                ],
                rows,
            })
        }
    }

    fn grant_row_set(rows: Vec<Vec<&str>>) -> RowSet {
        RowSet {
            query_id: "grants-query".to_string(),
            columns: vec![
                "privilege".to_string(),
                "granted_on".to_string(),
                "name".to_string(),
            ],
            rows: rows
                .into_iter()
                .map(|row| row.into_iter().map(str::to_string).collect())
                .collect(),
        }
    }
}
