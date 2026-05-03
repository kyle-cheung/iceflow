use std::ffi::OsString;

pub(crate) struct SavedEnv {
    values: Vec<(&'static str, Option<OsString>)>,
}

impl SavedEnv {
    pub(crate) fn capture(names: &[&'static str]) -> Self {
        Self {
            values: names
                .iter()
                .map(|name| (*name, std::env::var_os(name)))
                .collect(),
        }
    }
}

impl Drop for SavedEnv {
    fn drop(&mut self) {
        for (name, value) in &self.values {
            if let Some(value) = value {
                std::env::set_var(name, value);
            } else {
                std::env::remove_var(name);
            }
        }
    }
}
