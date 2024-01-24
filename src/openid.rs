use std::{env, time::{SystemTime, Duration}};

use anyhow::{Result, Context, anyhow};
use jsonwebtoken::{decode_header, DecodingKey, Validation, Algorithm, decode};
use serde::Deserialize;
use tokio::sync::Mutex;
use url::Url;

#[derive(Debug, Deserialize)]
pub struct Claims {
    pub sub: String,
}

#[derive(Clone, Deserialize)]
struct JwksResponse {
    keys: Vec<JsonWebKey>
}

#[derive(Clone, Deserialize)]
struct JsonWebKey {
    kid: String,
    x: String,
    y: String,
}

#[derive(Deserialize, Clone)]
struct OpenIdConfiguration {
    issuer: String,
    jwks_uri: String,
}

pub struct OpenIdClient {
    base_url: Url,
    oidc_config: Mutex<Option<OpenIdConfiguration>>,
    oidc_config_last_updated: Mutex<Option<SystemTime>>,
    oidc_config_max_age: Duration,
    jwks: Mutex<Option<JwksResponse>>,
    jwks_last_updated: Mutex<Option<SystemTime>>,
    jwks_max_age: Duration,
}

impl OpenIdClient {
    pub fn new(base_url: Url) -> Self {
        Self {
            base_url,
            oidc_config: Mutex::new(None),
            oidc_config_last_updated: Mutex::new(None),
            oidc_config_max_age: Duration::from_secs(24 * 60 * 60),
            jwks: Mutex::new(None),
            jwks_last_updated: Mutex::new(None),
            jwks_max_age: Duration::from_secs(24 * 60 * 60),
        }
    }

    pub async fn authorize_current_user(
        &self,
        token: &str,
    ) -> Result<Claims> {

        self.refresh_oidc_config().await?;
        let oidc_lock = self.oidc_config.lock().await;
        let oidc_config = oidc_lock.as_ref()
            .with_context(|| "OIDC config was None after refreshing")?;

        let kid = decode_header(&token)
            .with_context(|| "Failed to decode JWT header")?
            .kid
            .ok_or(anyhow!("Failed to get kid from jwt header."))?;

        let jwk = self.key(&kid, &oidc_config).await?;

        let decoding_key = DecodingKey::from_ec_components(&jwk.x, &jwk.y)
            .with_context(|| "Failed to build decoding key from EC components")?;

        let audience =
            env::var("HEMATITE_JWT_AUD")
            .with_context(|| "Env var HEMATITE_JWT_AUD is missing.")?;

        let mut validation = Validation::new(Algorithm::ES384);
        validation.set_issuer(&[&oidc_config.issuer]);
        validation.set_audience(&[audience]);

        decode::<Claims>(&token, &decoding_key, &validation)
            .map(|token_data| token_data.claims)
            .with_context(|| "Failed to decode token")
    }

    async fn refresh_oidc_config(&self) -> Result<()> {
        let cfg_cache_opt = self.oidc_config.lock().await;

        let should_refresh = {
            let last_updated_opt = self.oidc_config_last_updated.lock().await;

            if let Some(last_updated) = last_updated_opt.as_ref() {
                let age = SystemTime::now().duration_since(*last_updated)
                    .with_context(|| "Failed to calculate age of OIDC config cache")?;

                age > self.oidc_config_max_age
            } else {
                true
            }
        };

        if should_refresh {
            let oidc_config_url = self.base_url.join(".well-known/openid-configuration")
                .with_context(|| "Failed to build openid-configuration URL")?;

            let oidc_cfg: OpenIdConfiguration =
                reqwest::get(oidc_config_url.clone()).await
                .with_context(|| format!("Failed to get OIDC config url at {}", oidc_config_url))?
                .json().await
                .with_context(|| format!("Failed to decode OIDC config as JSON from {}", oidc_config_url))?;

            let cfg_opt = Some(oidc_cfg.clone());

            std::mem::swap(&mut cfg_cache_opt.as_ref(), &mut cfg_opt.as_ref());

            let timestamp = Some(SystemTime::now());

            let timestamp_cache = self.oidc_config_last_updated.lock().await;
            std::mem::swap(&mut timestamp_cache.as_ref(), &mut timestamp.as_ref());
        }

        Ok(())
    }

    async fn key(&self, kid: &str, oidc_config: &OpenIdConfiguration) -> Result<JsonWebKey> {
        self.refresh_keys(kid, oidc_config).await?;

        let jwks_body = self.jwks.lock().await.as_ref().with_context(|| "Expected jwks body to be present after fetching it")?.clone();

        jwks_body.keys.into_iter().find(|key| key.kid == kid)
            .ok_or(anyhow!("Couldn't find key in jwks response"))
    }

    async fn refresh_keys(&self, kid: &str, oidc_config: &OpenIdConfiguration) -> Result<()> {
        let jwks_cache_opt = self.jwks.lock().await;

        let should_refresh = {
            let last_updated_opt = self.jwks_last_updated.lock().await;

            if let Some(last_updated) = last_updated_opt.as_ref() {
                let age = SystemTime::now().duration_since(*last_updated)
                    .with_context(|| "Failed to calculate age of JWKS cache")?;

                age > self.jwks_max_age || jwks_cache_opt.as_ref().unwrap().keys.iter().find(|key| key.kid == kid).is_none()
            } else {
                true
            }
        };

        if should_refresh {
            let jwks_body: JwksResponse =
                reqwest::get(&oidc_config.jwks_uri).await
                .with_context(|| format!("Failed to get JWKS response at URL {}", &oidc_config.jwks_uri))?
                .json().await
                .with_context(|| format!("Failed to decode JWKS response as JSON from {}", &oidc_config.jwks_uri))?;

            let jwks_opt = Some(jwks_body.clone());

            std::mem::swap(&mut jwks_cache_opt.as_ref(), &mut jwks_opt.as_ref());

            let timestamp = Some(SystemTime::now());

            let timestamp_cache = self.jwks_last_updated.lock().await;
            std::mem::swap(&mut timestamp_cache.as_ref(), &mut timestamp.as_ref());
        };

        Ok(())
    }
}
