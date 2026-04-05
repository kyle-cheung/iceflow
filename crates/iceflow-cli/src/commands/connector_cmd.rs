use anyhow::{Error, Result};
use iceflow_source::SourceCapability;
use serde::Serialize;
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use crate::config::{
    build_source_from_config, load_catalog_config, load_connector_config, load_destination_config,
    load_optional_catalog_config, load_source_config, resolve_catalog_name, CaptureSettings,
    ConnectorConfig, DestinationConfig,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckArgs {
    pub connector_config: PathBuf,
    pub config_root: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CheckReport {
    pub valid: bool,
    pub errors: Vec<String>,
    pub warnings: Vec<String>,
    pub table_count: usize,
}

impl CheckArgs {
    pub fn parse(args: &[String]) -> Result<Self> {
        let connector_config = args
            .iter()
            .position(|arg| arg == "--connector")
            .and_then(|index| args.get(index + 1))
            .map(PathBuf::from)
            .ok_or_else(|| Error::msg("--connector <path> is required"))?;
        let config_root = infer_config_root(&connector_config)?;

        Ok(Self {
            connector_config,
            config_root,
        })
    }
}

impl CheckReport {
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).expect("connector check report serialization should not fail")
    }
}

pub fn check_blocking(args: CheckArgs) -> Result<CheckReport> {
    crate::block_on(check(args))
}

pub async fn check(args: CheckArgs) -> Result<CheckReport> {
    let connector = load_connector_config(&args.connector_config)?;
    let source_path = args
        .config_root
        .join("sources")
        .join(format!("{}.toml", connector.source));
    let source_config = load_source_config(&source_path)
        .map_err(|err| Error::msg(format!("source '{}': {err}", connector.source)))?;
    let destination_path = args
        .config_root
        .join("destinations")
        .join(format!("{}.toml", connector.destination));
    let destination_config = load_destination_config(&destination_path)
        .map_err(|err| Error::msg(format!("destination '{}': {err}", connector.destination)))?;
    let source = build_source_from_config(
        &source_config,
        source_path.parent().unwrap_or_else(|| Path::new(".")),
    )
    .map_err(|err| Error::msg(format!("build source '{}': {err}", connector.source)))?;
    let source_report = source
        .check()
        .await
        .map_err(|err| Error::msg(format!("source check '{}': {err}", connector.source)))?;

    let mut errors = Vec::new();
    let mut warnings = Vec::new();

    let catalog_state = validate_catalog_configuration(
        &args.config_root,
        &connector,
        &destination_config,
        &mut errors,
    )?;

    match (&destination_config.kind[..], &catalog_state) {
        ("filesystem", CatalogValidation::Resolved(name)) => errors.push(format!(
            "destination '{}' does not support catalog '{}'",
            connector.destination, name
        )),
        ("polaris", CatalogValidation::Unresolved) => errors.push(format!(
            "destination '{}' requires a catalog reference",
            connector.destination
        )),
        _ => {}
    }

    if connector.tables.is_empty() {
        errors.push("connector must declare at least one table".to_string());
    }

    if source_config.kind == "file" && connector.capture != CaptureSettings::default() {
        errors.push(
            "file source connectors must not declare publication, slot, or bootstrap capture settings"
                .to_string(),
        );
    }

    for (index, table) in connector.tables.iter().enumerate() {
        match table.table_mode.as_str() {
            "append_only" | "keyed_upsert" => {}
            other => errors.push(format!("tables[{index}]: unsupported table_mode '{other}'")),
        }

        if table.source_table.is_empty() {
            errors.push(format!("tables[{index}]: source_table is required"));
        }
        if table.destination_table.is_empty() {
            errors.push(format!("tables[{index}]: destination_table is required"));
        }
        if table.destination_namespace.is_empty() {
            warnings.push(format!(
                "tables[{index}]: empty destination_namespace is allowed but should be deliberate"
            ));
        }
        if table.table_mode == "keyed_upsert" && table.key_columns.is_empty() {
            errors.push(format!(
                "tables[{index}]: keyed_upsert requires key_columns"
            ));
        }
        if table.table_mode == "keyed_upsert" && table.ordering_field.is_none() {
            errors.push(format!(
                "tables[{index}]: keyed_upsert requires ordering_field"
            ));
        }

        if table.table_mode == "append_only"
            && !source_report
                .capabilities
                .contains(&SourceCapability::AppendOnly)
        {
            errors.push(format!(
                "tables[{index}]: source does not advertise append_only capability"
            ));
        }
        if table.table_mode == "keyed_upsert"
            && !source_report
                .capabilities
                .contains(&SourceCapability::KeyedUpsert)
        {
            errors.push(format!(
                "tables[{index}]: source does not advertise keyed_upsert capability"
            ));
        }
        if table.table_mode == "keyed_upsert"
            && !source_report
                .capabilities
                .contains(&SourceCapability::StableLatestWinsOrdering)
        {
            errors.push(format!(
                "tables[{index}]: source does not advertise stable_latest_wins_ordering capability"
            ));
        }
    }

    Ok(CheckReport {
        valid: errors.is_empty(),
        errors,
        warnings,
        table_count: connector.tables.len(),
    })
}

fn infer_config_root(connector_config: &Path) -> Result<PathBuf> {
    let Some(connectors_dir) = connector_config.parent() else {
        return Err(Error::msg(
            "--connector must be nested under <config-root>/connectors/",
        ));
    };
    let Some(config_root) = connectors_dir.parent() else {
        return Err(Error::msg(
            "--connector must be nested under <config-root>/connectors/",
        ));
    };

    if connectors_dir.as_os_str().is_empty()
        || config_root.as_os_str().is_empty()
        || connectors_dir.file_name().and_then(|name| name.to_str()) != Some("connectors")
    {
        return Err(Error::msg(
            "--connector must be nested under <config-root>/connectors/",
        ));
    }

    Ok(config_root.to_path_buf())
}

enum CatalogValidation {
    Resolved(String),
    Unresolved,
    Conflict,
}

fn validate_catalog_configuration(
    config_root: &Path,
    connector: &ConnectorConfig,
    destination: &DestinationConfig,
    errors: &mut Vec<String>,
) -> Result<CatalogValidation> {
    match resolve_catalog_name(connector, destination) {
        Ok(Some(name)) => {
            load_optional_catalog_config(config_root, connector, destination)
                .map_err(|err| Error::msg(format!("catalog resolution: {err}")))?;
            Ok(CatalogValidation::Resolved(name))
        }
        Ok(None) => Ok(CatalogValidation::Unresolved),
        Err(err) => {
            errors.push(err.to_string());
            for catalog_name in referenced_catalog_names(connector, destination) {
                validate_catalog_file(config_root, &catalog_name, errors);
            }
            Ok(CatalogValidation::Conflict)
        }
    }
}

fn referenced_catalog_names(
    connector: &ConnectorConfig,
    destination: &DestinationConfig,
) -> BTreeSet<String> {
    connector
        .catalog
        .iter()
        .chain(destination.catalog.iter())
        .cloned()
        .collect()
}

fn validate_catalog_file(config_root: &Path, catalog_name: &str, errors: &mut Vec<String>) {
    let catalog_path = config_root
        .join("catalogs")
        .join(format!("{catalog_name}.toml"));
    if let Err(err) = load_catalog_config(&catalog_path) {
        errors.push(format!("catalog '{catalog_name}': {err}"));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_args_parse_derives_config_root_from_nested_connector_path() -> Result<()> {
        let parsed = CheckArgs::parse(&[
            "--connector".to_string(),
            "fixtures/config_samples/connectors/orders_append.toml".to_string(),
        ])?;

        assert_eq!(parsed.config_root, PathBuf::from("fixtures/config_samples"));
        Ok(())
    }

    #[test]
    fn check_args_parse_rejects_bare_connector_filename() {
        let err = CheckArgs::parse(&["--connector".to_string(), "orders_append.toml".to_string()])
            .expect_err("bare connector path should fail");

        assert_eq!(
            err.to_string(),
            "--connector must be nested under <config-root>/connectors/"
        );
    }

    #[test]
    fn check_args_parse_rejects_single_component_connector_path() {
        let err = CheckArgs::parse(&[
            "--connector".to_string(),
            "connectors/orders_append.toml".to_string(),
        ])
        .expect_err("single-component connector path should fail");

        assert_eq!(
            err.to_string(),
            "--connector must be nested under <config-root>/connectors/"
        );
    }
}
