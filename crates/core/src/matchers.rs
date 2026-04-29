use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy)]
pub struct MatchInput<'a> {
    pub host: &'a str,
    pub path: &'a str,
    pub headers: &'a [(String, String)],
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigConflict {
    pub kind: String,
    pub message: String,
}

impl ConfigConflict {
    pub fn new(kind: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            kind: kind.into(),
            message: message.into(),
        }
    }
}
