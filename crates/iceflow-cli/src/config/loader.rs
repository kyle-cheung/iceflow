use crate::config::types::{CatalogConfig, ConnectorConfig, DestinationConfig, SourceConfig};
use anyhow::{Error, Result};
use serde::de::DeserializeOwned;
use std::collections::BTreeMap;
use std::path::Path;

pub fn load_source_config(path: &Path) -> Result<SourceConfig> {
    let config: SourceConfig = load_toml_config(path, "source")?;
    validate_config_version("source", config.version)?;
    Ok(SourceConfig {
        properties: resolve_env_vars(config.properties)?,
        ..config
    })
}

pub fn load_destination_config(path: &Path) -> Result<DestinationConfig> {
    let config: DestinationConfig = load_toml_config(path, "destination")?;
    validate_config_version("destination", config.version)?;
    Ok(DestinationConfig {
        properties: resolve_env_vars(config.properties)?,
        ..config
    })
}

pub fn load_catalog_config(path: &Path) -> Result<CatalogConfig> {
    let config: CatalogConfig = load_toml_config(path, "catalog")?;
    validate_config_version("catalog", config.version)?;
    Ok(CatalogConfig {
        properties: resolve_env_vars(config.properties)?,
        ..config
    })
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

fn resolve_env_vars(properties: BTreeMap<String, String>) -> Result<BTreeMap<String, String>> {
    resolve_env_vars_with(properties, |var_name| {
        std::env::var(var_name).map_err(|err| err.to_string())
    })
}

fn resolve_env_vars_with<F>(
    properties: BTreeMap<String, String>,
    mut resolve: F,
) -> Result<BTreeMap<String, String>>
where
    F: FnMut(&str) -> std::result::Result<String, String>,
{
    properties
        .into_iter()
        .map(|(key, value)| {
            let resolved = if let Some(literal) = value.strip_prefix("$$") {
                format!("${literal}")
            } else if let Some(var_name) = value.strip_prefix('$') {
                resolve(var_name).map_err(|err| {
                    Error::msg(format!(
                        "config property '{key}' references environment variable '{var_name}' which could not be loaded: {err}"
                    ))
                })?
            } else {
                value
            };
            Ok((key, resolved))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_env_vars_with_substitutes_referenced_values() -> Result<()> {
        let properties = BTreeMap::from([("host".to_string(), "$TEST_PG_HOST".to_string())]);

        let resolved = resolve_env_vars_with(properties, |var_name| match var_name {
            "TEST_PG_HOST" => Ok("localhost".to_string()),
            other => Err(format!("unexpected variable lookup: {other}")),
        })?;

        assert_eq!(resolved.get("host").map(String::as_str), Some("localhost"));
        Ok(())
    }

    #[test]
    fn resolve_env_vars_with_rejects_missing_variables() {
        let properties = BTreeMap::from([("password".to_string(), "$MISSING_SECRET".to_string())]);

        let err = resolve_env_vars_with(properties, |_| Err("not set".to_string()))
            .expect_err("missing env var should fail");

        assert!(err.to_string().contains("MISSING_SECRET"));
    }

    #[test]
    fn resolve_env_vars_with_allows_escaped_dollar_prefix() -> Result<()> {
        let properties = BTreeMap::from([("password".to_string(), "$$literal".to_string())]);

        let resolved = resolve_env_vars_with(properties, |_| {
            Err("escaped values should not trigger environment lookup".to_string())
        })?;

        assert_eq!(
            resolved.get("password").map(String::as_str),
            Some("$literal")
        );
        Ok(())
    }
}
