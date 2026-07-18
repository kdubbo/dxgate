//! Authentication policy enforcement: API-key matching and HMAC JWT
//! validation. Pure over the policy config and the request header pairs.

use super::header_value;
use dxgate_core::AuthPolicy;
use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};
use serde::Deserialize;
use serde_json::Value;
use std::env;

pub(super) fn validate_auth(auth: &AuthPolicy, headers: &[(String, String)]) -> Result<(), String> {
    match auth {
        AuthPolicy::ApiKey {
            header,
            values,
            value_env,
        } => {
            let actual = header_value(headers, header)
                .ok_or_else(|| format!("missing header {}", header))?;
            let mut accepted = values.clone();
            if let Some(env_name) = value_env {
                if let Ok(value) = env::var(env_name) {
                    accepted.push(value);
                }
            }
            if accepted.iter().any(|value| value == actual) {
                Ok(())
            } else {
                Err("invalid API key".to_string())
            }
        }
        AuthPolicy::Jwt {
            header,
            hmac_secret_env,
            issuer,
            audiences,
        } => {
            let raw = header_value(headers, header)
                .ok_or_else(|| format!("missing header {}", header))?;
            let token = raw.strip_prefix("Bearer ").unwrap_or(raw);
            let Some(secret_env) = hmac_secret_env else {
                return Err("JWT policy requires hmac_secret_env".to_string());
            };
            let secret = env::var(secret_env)
                .map_err(|_| format!("JWT secret env {} is not set", secret_env))?;
            let mut validation = Validation::new(Algorithm::HS256);
            if audiences.is_empty() {
                validation.validate_aud = false;
            } else {
                validation.set_audience(audiences);
            }
            if let Some(issuer) = issuer {
                validation.set_issuer(&[issuer]);
            }
            decode::<JwtClaims>(
                token,
                &DecodingKey::from_secret(secret.as_bytes()),
                &validation,
            )
            .map(|_| ())
            .map_err(|e| format!("invalid JWT: {e}"))
        }
    }
}

#[derive(Debug, Deserialize)]
struct JwtClaims {
    #[allow(dead_code)]
    sub: Option<String>,
    #[allow(dead_code)]
    exp: Option<usize>,
    #[allow(dead_code)]
    iss: Option<String>,
    #[allow(dead_code)]
    aud: Option<Value>,
}
