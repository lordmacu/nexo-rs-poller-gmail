//! Gmail (Google API) poller — **scaffold**.
//!
//! Full port from `crates/poller/src/builtins/gmail/` is tracked as
//! a Phase 96 follow-up. Scaffold proves the manifest + reverse-RPC
//! credential resolution path; fetch + label diff + dispatch logic
//! to follow.

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::json;

use nexo_microapp_sdk::poller::{PollerHandler, TickRequest};
use nexo_poller::{PollerError, PollerHost, TickAck, TickMetrics};

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct GmailJobConfig {
    /// Gmail search query (`is:unread`, `from:...`, etc.).
    #[serde(default = "default_query")]
    pub query: String,
    #[serde(default = "default_max_per_tick")]
    pub max_per_tick: usize,
    #[serde(default = "default_template")]
    pub message_template: String,
    pub deliver: DeliverCfg,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct DeliverCfg {
    pub channel: String,
    #[serde(alias = "recipient")]
    pub to: String,
}

fn default_query() -> String {
    "is:unread".into()
}
fn default_max_per_tick() -> usize {
    20
}
fn default_template() -> String {
    "✉ {subject} — {from}\n{snippet}".to_string()
}

pub struct GmailHandler;

impl GmailHandler {
    pub fn new() -> Self {
        Self
    }
}

impl Default for GmailHandler {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl PollerHandler for GmailHandler {
    async fn tick(
        &self,
        req: TickRequest,
        host: std::sync::Arc<dyn PollerHost>,
    ) -> Result<TickAck, PollerError> {
        let _cfg: GmailJobConfig =
            serde_json::from_value(req.config.clone()).map_err(|e| PollerError::Config {
                job: req.job_id.clone(),
                reason: e.to_string(),
            })?;

        let _cred = host
            .credentials_get("google".into())
            .await
            .map_err(|e| PollerError::Permanent(anyhow::anyhow!("credentials_get: {e}")))?;

        // TODO Phase 96 follow-up: port the Gmail API fetch +
        // historyId diff + dispatch logic from the in-tree builtin.
        host.log(
            nexo_poller::LogLevel::Info,
            format!("gmail tick stub — job {}", req.job_id),
            json!({}),
        )
        .await
        .ok();

        Ok(TickAck {
            next_cursor: None,
            next_interval_hint: None,
            metrics: Some(TickMetrics::default()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_minimal_config() {
        let cfg: GmailJobConfig = serde_json::from_value(json!({
            "deliver": { "channel": "telegram", "to": "-100" },
        }))
        .unwrap();
        assert_eq!(cfg.query, "is:unread");
        assert_eq!(cfg.max_per_tick, 20);
    }

    #[test]
    fn config_accepts_recipient_alias() {
        let cfg: GmailJobConfig = serde_json::from_value(json!({
            "deliver": { "channel": "whatsapp", "recipient": "+57300" },
        }))
        .unwrap();
        assert_eq!(cfg.deliver.to, "+57300");
    }
}
