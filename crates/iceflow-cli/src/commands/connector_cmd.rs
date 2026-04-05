use anyhow::{Error, Result};
use iceflow_source::SourceCapability;
use serde::Serialize;
use std::path::{Path, PathBuf};

use crate::config::{
    build_source_from_config, load_connector_config, load_destination_config,
    load_optional_catalog_config, load_source_config, resolve_catalog_name, CaptureSettings,
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

        let config_root = connector_config
            .parent()
            .and_then(|path| path.parent())
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("."));

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

    let catalog_name = match resolve_catalog_name(&connector, &destination_config) {
        Ok(name) => name,
        Err(err) => {
            errors.push(err.to_string());
            None
        }
    };

    match (destination_config.kind.as_str(), catalog_name.as_ref()) {
        ("filesystem", Some(name)) => errors.push(format!(
            "destination '{}' does not support catalog '{}'",
            connector.destination, name
        )),
        ("polaris", None) => errors.push(format!(
            "destination '{}' requires a catalog reference",
            connector.destination
        )),
        _ => {}
    }

    if catalog_name.is_some() {
        load_optional_catalog_config(&args.config_root, &connector, &destination_config)
            .map_err(|err| Error::msg(format!("catalog resolution: {err}")))?;
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
            && (!source_report
                .capabilities
                .contains(&SourceCapability::KeyedUpsert)
                || !source_report
                    .capabilities
                    .contains(&SourceCapability::StableLatestWinsOrdering))
        {
            errors.push(format!(
                "tables[{index}]: keyed_upsert requires KeyedUpsert and StableLatestWinsOrdering capabilities"
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
