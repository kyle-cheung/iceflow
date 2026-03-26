use anyhow::{Error, Result};
use reqwest::Url;
use std::path::{Path, PathBuf};

pub(super) trait CompactionCatalog {
    fn validate_target(&self) -> Result<()>;
    fn snapshot_dir(&self, table_root: &Path) -> PathBuf;
}

#[derive(Debug, Clone)]
pub(super) struct PolarisCompactionCatalog {
    client: reqwest::blocking::Client,
    catalog_uri: String,
    catalog_name: String,
    namespace: String,
}

impl PolarisCompactionCatalog {
    pub(super) fn new(
        catalog_uri: impl Into<String>,
        catalog_name: impl Into<String>,
        namespace: impl Into<String>,
    ) -> Self {
        Self {
            client: reqwest::blocking::Client::new(),
            catalog_uri: catalog_uri.into().trim_end_matches('/').to_string(),
            catalog_name: catalog_name.into(),
            namespace: namespace.into(),
        }
    }

    fn base_url(&self) -> Result<Url> {
        Url::parse(&self.catalog_uri).map_err(|err| Error::msg(err.to_string()))
    }

    fn config_url(&self) -> Result<Url> {
        let mut url = self.base_url()?;
        {
            let mut segments = url.path_segments_mut().map_err(|_| {
                Error::msg(format!(
                    "catalog URI is not a valid base: {}",
                    self.catalog_uri
                ))
            })?;
            segments.push("v1");
            segments.push("config");
        }
        url.query_pairs_mut()
            .append_pair("warehouse", &self.catalog_name);
        Ok(url)
    }

    fn namespace_url(&self, prefix: &str) -> Result<Url> {
        let mut url = self.base_url()?;
        {
            let mut segments = url.path_segments_mut().map_err(|_| {
                Error::msg(format!(
                    "catalog URI is not a valid base: {}",
                    self.catalog_uri
                ))
            })?;
            segments.push("v1");
            segments.push(prefix);
            segments.push("namespaces");
            segments.push(&self.namespace);
        }
        Ok(url)
    }
}

impl CompactionCatalog for PolarisCompactionCatalog {
    fn validate_target(&self) -> Result<()> {
        let config = self
            .client
            .get(self.config_url()?)
            .send()
            .map_err(|err| Error::msg(err.to_string()))?
            .error_for_status()
            .map_err(|err| Error::msg(err.to_string()))?
            .json::<serde_json::Value>()
            .map_err(|err| Error::msg(err.to_string()))?;

        let prefix = match config {
            serde_json::Value::Object(object) => object
                .get("defaults")
                .and_then(|defaults| defaults.get("prefix"))
                .and_then(|value| value.as_str())
                .or_else(|| {
                    object
                        .get("overrides")
                        .and_then(|overrides| overrides.get("prefix"))
                        .and_then(|value| value.as_str())
                })
                .unwrap_or(self.catalog_name.as_str())
                .to_string(),
            _ => self.catalog_name.clone(),
        };

        self.client
            .get(self.namespace_url(&prefix)?)
            .send()
            .map_err(|err| Error::msg(err.to_string()))?
            .error_for_status()
            .map_err(|err| Error::msg(err.to_string()))?;
        Ok(())
    }

    fn snapshot_dir(&self, table_root: &Path) -> PathBuf {
        table_root.join("_greytl_snapshots")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compact_catalog_urls_encode_user_supplied_segments() {
        let catalog = PolarisCompactionCatalog::new(
            "http://127.0.0.1:8181/api/catalog",
            "qa warehouse/blue team",
            "orders/events #1",
        );

        let config_url = catalog.config_url().expect("config url");
        assert_eq!(
            config_url.as_str(),
            "http://127.0.0.1:8181/api/catalog/v1/config?warehouse=qa+warehouse%2Fblue+team"
        );

        let namespace_url = catalog
            .namespace_url("prefix/with spaces?#")
            .expect("namespace url");
        assert_eq!(
            namespace_url.as_str(),
            "http://127.0.0.1:8181/api/catalog/v1/prefix%2Fwith%20spaces%3F%23/namespaces/orders%2Fevents%20%231"
        );
    }
}
