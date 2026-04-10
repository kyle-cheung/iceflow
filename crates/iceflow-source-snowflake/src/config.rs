use anyhow::{Error, Result};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SnowflakeAuthMethod {
    Password,
}

#[derive(Debug, Clone, PartialEq, Eq)]
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
            password: required(properties, "password")?.to_string(),
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
    fn rejects_unsupported_auth_method() {
        let err = SnowflakeSourceConfig::from_properties(
            "local_snowflake",
            &BTreeMap::from([("auth_method".to_string(), "key_pair".to_string())]),
        )
        .expect_err("unsupported auth");

        assert!(err.to_string().contains("auth_method"));
    }
}
