use crate::config::types::{CatalogConfig, ConnectorConfig, DestinationConfig, SourceConfig};
use anyhow::{Error, Result};
use serde::de::DeserializeOwned;
use std::collections::BTreeMap;
use std::path::Path;

pub fn load_source_config(path: &Path) -> Result<SourceConfig> {
    let mut config: SourceConfig = load_toml_config(path, "source")?;
    validate_config_version("source", config.version)?;
    resolve_env_vars(&mut config.properties)?;
    Ok(config)
}

pub fn load_destination_config(path: &Path) -> Result<DestinationConfig> {
    let mut config: DestinationConfig = load_toml_config(path, "destination")?;
    validate_config_version("destination", config.version)?;
    resolve_env_vars(&mut config.properties)?;
    Ok(config)
}

pub fn load_catalog_config(path: &Path) -> Result<CatalogConfig> {
    let mut config: CatalogConfig = load_toml_config(path, "catalog")?;
    validate_config_version("catalog", config.version)?;
    resolve_env_vars(&mut config.properties)?;
    Ok(config)
}

pub fn load_connector_config(path: &Path) -> Result<ConnectorConfig> {
    let config: ConnectorConfig = load_toml_config(path, "connector")?;
    validate_config_version("connector", config.version)?;
    Ok(config)
}

fn load_toml_config<T>(path: &Path, label: &str) -> Result<T>
where
    T: DeserializeOwned,
{
    let content = std::fs::read_to_string(path).map_err(|err| {
        Error::msg(format!(
            "failed to read {label} config {}: {err}",
            path.display()
        ))
    })?;
    toml::from_str(&content).map_err(|err| {
        Error::msg(format!(
            "failed to parse {label} config {}: {err}",
            path.display()
        ))
    })
}

fn validate_config_version(label: &str, version: u32) -> Result<()> {
    if version != 1 {
        return Err(Error::msg(format!(
            "unsupported {label} config version: {version}"
        )));
    }
    Ok(())
}

fn resolve_env_vars(properties: &mut BTreeMap<String, String>) -> Result<()> {
    for (key, value) in properties.iter_mut() {
        if let Some(var_name) = value.strip_prefix('$') {
            let resolved = std::env::var(var_name).map_err(|_| {
                Error::msg(format!(
                    "config property '{key}' references environment variable '{var_name}' which is not set"
                ))
            })?;
            *value = resolved;
        }
    }
    Ok(())
}
