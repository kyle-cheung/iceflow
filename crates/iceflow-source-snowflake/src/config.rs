use anyhow::{Error, Result};
use std::collections::BTreeMap;
use std::fmt::{self, Debug, Formatter};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SnowflakeAuthMethod {
    Password,
}

#[derive(Clone, PartialEq, Eq)]
pub struct SnowflakeSourceConfig {
    pub source_label: String,
    pub account: String,
    pub user: String,
    pub password: String,
    pub warehouse: String,
    pub role: String,
    pub database: String,
    pub auth_method: SnowflakeAuthMethod,
}

impl Debug for SnowflakeSourceConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("SnowflakeSourceConfig")
            .field("source_label", &self.source_label)
            .field("account", &self.account)
            .field("user", &self.user)
            .field("password", &"<redacted>")
            .field("warehouse", &self.warehouse)
            .field("role", &self.role)
            .field("database", &self.database)
            .field("auth_method", &self.auth_method)
            .finish()
    }
}

impl SnowflakeSourceConfig {
    pub fn from_properties(
        source_label: impl Into<String>,
        properties: &BTreeMap<String, String>,
    ) -> Result<Self> {
        let auth_method = match required(properties, "auth_method")? {
            "password" => SnowflakeAuthMethod::Password,
            other => return Err(Error::msg(format!("unsupported auth_method: {other}"))),
        };

        Ok(Self {
            source_label: source_label.into(),
            account: required(properties, "account")?.to_string(),
            user: required(properties, "user")?.to_string(),
            password: properties.get("password").cloned().unwrap_or_default(),
            warehouse: required(properties, "warehouse")?.to_string(),
            role: required(properties, "role")?.to_string(),
            database: required(properties, "database")?.to_string(),
            auth_method,
        })
    }
}

fn required<'a>(properties: &'a BTreeMap<String, String>, key: &str) -> Result<&'a str> {
    properties
        .get(key)
        .map(String::as_str)
        .ok_or_else(|| Error::msg(format!("missing required property '{key}'")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_password_auth_config() {
        let config = SnowflakeSourceConfig::from_properties(
            "local_snowflake",
            &BTreeMap::from([
                ("account".to_string(), "xy12345.us-east-1".to_string()),
                ("user".to_string(), "ICEFLOW_DEMO".to_string()),
                ("password".to_string(), "secret".to_string()),
                ("warehouse".to_string(), "ICEFLOW_WH".to_string()),
                ("role".to_string(), "ICEFLOW_ROLE".to_string()),
                ("database".to_string(), "SOURCE_DB".to_string()),
                ("auth_method".to_string(), "password".to_string()),
            ]),
        )
        .expect("valid config");

        assert_eq!(config.account, "xy12345.us-east-1");
        assert_eq!(config.auth_method, SnowflakeAuthMethod::Password);
    }

    #[test]
    fn debug_redacts_password() {
        let config = SnowflakeSourceConfig {
            source_label: "local_snowflake".to_string(),
            account: "xy12345.us-east-1".to_string(),
            user: "ICEFLOW_DEMO".to_string(),
            password: "super-secret".to_string(),
            warehouse: "ICEFLOW_WH".to_string(),
            role: "ICEFLOW_ROLE".to_string(),
            database: "SOURCE_DB".to_string(),
            auth_method: SnowflakeAuthMethod::Password,
        };

        let debug = format!("{config:?}");

        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("super-secret"));
    }

    #[test]
    fn parses_password_auth_config_without_password_for_external_adbc_auth() {
        let config = SnowflakeSourceConfig::from_properties(
            "local_snowflake",
            &BTreeMap::from([
                ("account".to_string(), "xy12345.us-east-1".to_string()),
                ("user".to_string(), "ICEFLOW_DEMO".to_string()),
                ("warehouse".to_string(), "ICEFLOW_WH".to_string()),
                ("role".to_string(), "ICEFLOW_ROLE".to_string()),
                ("database".to_string(), "SOURCE_DB".to_string()),
                ("auth_method".to_string(), "password".to_string()),
            ]),
        )
        .expect("valid config");

        assert!(config.password.is_empty());
    }

    #[test]
    fn rejects_unsupported_auth_method() {
        let err = SnowflakeSourceConfig::from_properties(
            "local_snowflake",
            &BTreeMap::from([("auth_method".to_string(), "key_pair".to_string())]),
        )
        .expect_err("unsupported auth");

        assert!(err.to_string().contains("auth_method"));
    }
}
