use anyhow::{Error, Result};
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

    fn config_url(&self) -> String {
        format!(
            "{}/v1/config?warehouse={}",
            self.catalog_uri, self.catalog_name
        )
    }

    fn namespace_url(&self, prefix: &str) -> String {
        format!(
            "{}/v1/{prefix}/namespaces/{}",
            self.catalog_uri, self.namespace
        )
    }
}

impl CompactionCatalog for PolarisCompactionCatalog {
    fn validate_target(&self) -> Result<()> {
        let config = self
            .client
            .get(self.config_url())
            .send()
            .map_err(|err| Error::msg(err.to_string()))?
            .error_for_status()
            .map_err(|err| Error::msg(err.to_string()))?
            .json::<serde_json_ext::Value>()
            .map_err(|err| Error::msg(err.to_string()))?;

        let prefix = match config {
            serde_json_ext::Value::Object(object) => object
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
            .get(self.namespace_url(&prefix))
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
