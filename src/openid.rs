use std::env;

use anyhow::{Result, Context, anyhow};
use jsonwebtoken::{decode_header, DecodingKey, Validation, Algorithm, decode};
use serde::Deserialize;
use tokio::sync::Mutex;
use url::Url;

#[derive(Debug, Deserialize)]
pub struct Claims {
    pub sub: String,
}

#[derive(Clone, Debug, Deserialize)]
struct JwksResponse {
    keys: Vec<JsonWebKey>
}

#[derive(Clone, Debug, Deserialize)]
struct JsonWebKey {
    kid: String,
    x: String,
    y: String,
}

#[derive(Deserialize, Debug, Clone)]
struct OpenIdConfiguration {
    issuer: String,
    jwks_uri: String,
}

#[derive(Debug)]
pub struct OpenIdClient {
    base_url: Url,
    oidc_config: Mutex<Option<OpenIdConfiguration>>,
    jwks: Mutex<Option<JwksResponse>>,
}

impl OpenIdClient {
    pub fn new(base_url: Url) -> Self {
        Self {
            base_url,
            oidc_config: Mutex::new(None),
            jwks: Mutex::new(None),
        }
    }

    #[tracing::instrument]
    pub async fn authorize_current_user(
        &self,
        token: &str,
    ) -> Result<Claims> {
        let oidc_config: OpenIdConfiguration = self.oidc_config().await?;

        let kid = decode_header(&token)
            .with_context(|| "Failed to decode JWT header")?
            .kid
            .with_context(|| "Failed to get kid from jwt header.")?;

        let jwk = self.key(&kid, &oidc_config).await?;

        let decoding_key = DecodingKey::from_ec_components(&jwk.x, &jwk.y)
            .with_context(|| "Failed to build decoding key from EC components")?;

        let audience =
            env::var("HEMATITE_JWT_AUD")
            .with_context(|| "Env var HEMATITE_JWT_AUD is missing.")?;

        let mut validation = Validation::new(Algorithm::ES384);
        validation.set_issuer(&[oidc_config.issuer]);
        validation.set_audience(&[audience]);

        decode::<Claims>(&token, &decoding_key, &validation)
            .map(|token_data| token_data.claims)
            .with_context(|| "Failed to decode token")
    }

    async fn oidc_config(&self) -> Result<OpenIdConfiguration> {
        let mut cfg_cache_opt = self.oidc_config.lock().await;

        if let Some(oidc_cfg) = cfg_cache_opt.as_ref() {
            Ok(oidc_cfg.clone())
        } else {
            let oidc_config_url = self.base_url.join(".well-known/openid-configuration")
                .with_context(|| "Failed to build openid-configuration URL")?;

            let oidc_cfg: OpenIdConfiguration =
                reqwest::get(oidc_config_url.clone()).await
                .with_context(|| format!("Failed to get OIDC config url at {}", oidc_config_url))?
                .json().await
                .with_context(|| format!("Failed to decode OIDC config as JSON from {}", oidc_config_url))?;

            let mut cfg_opt = Some(oidc_cfg.clone());

            std::mem::swap(&mut *cfg_cache_opt, &mut cfg_opt);

            Ok(oidc_cfg.clone())
        }
    }

    async fn key(&self, kid: &str, oidc_config: &OpenIdConfiguration) -> Result<JsonWebKey> {
        let mut jwks_cache_opt = self.jwks.lock().await;

        let jwks_body: JwksResponse =
            if let Some(jwks_opt) = jwks_cache_opt.as_ref() {
                jwks_opt.clone()
            } else {
                let jwks_body: JwksResponse =
                    reqwest::get(&oidc_config.jwks_uri).await
                    .with_context(|| format!("Failed to get JWKS response at URL {}", oidc_config.jwks_uri))?
                    .json().await
                    .with_context(|| format!("Failed to decode JWKS response as JSON from {}", oidc_config.jwks_uri))?;

                let mut jwks_opt = Some(jwks_body.clone());

                std::mem::swap(&mut *jwks_cache_opt, &mut jwks_opt);

                jwks_body
            };

        jwks_body.keys.into_iter().find(|key| key.kid == kid)
            .ok_or(anyhow!("Couldn't find key in jwks response"))
    }
}
