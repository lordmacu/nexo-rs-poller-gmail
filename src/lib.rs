//! Gmail (Google API) poller — search → regex extract → outbound
//! dispatch.
//!
//! One job per query: a single agent can run many Gmail polls
//! (`leads`, `invoices`, `monitor`) sharing the same Google
//! credentials but with independent cursors, schedules, and delivery
//! targets.
//!
//! Cursor: dedup-only — Gmail's `is:unread` + post-dispatch
//! `mark_read` is the primary dedup mechanism. The cursor stores a
//! belt-and-suspenders set of seen message ids (bounded ring of
//! 5000) to suppress duplicates when dispatch succeeds but
//! `mark_read` fails. A future migration to `historyId` is
//! non-breaking — the cursor slot is reserved.
//!
//! Ported from `nexo-poller::builtins::gmail` (V1) during Phase 96.
//! OAuth client + token refresh happen inside this subprocess; the
//! daemon hands credential paths over via reverse-RPC
//! `host.credentials_get("google")`.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use async_trait::async_trait;
use base64::Engine;
use dashmap::DashMap;
use regex::Regex;
use serde::Deserialize;
use serde_json::{json, Value};

use nexo_microapp_sdk::poller::{PollerHandler, TickRequest};
use nexo_plugin_google::{GoogleAuthClient, GoogleAuthConfig, SecretSources};
use nexo_poller::{PollerError, PollerHost, TickAck, TickMetrics};

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct GmailJobConfig {
    /// Gmail search query (`is:unread`, `subject:lead`, …).
    pub query: String,
    /// `newer_than:` suffix appended to the query (`1d`, `2h`).
    /// Avoids back-filling years of historical mail on first deploy.
    #[serde(default)]
    pub newer_than: Option<String>,
    /// Hard cap on dispatches per tick.
    #[serde(default = "default_max_per_tick")]
    pub max_per_tick: usize,
    /// Throttle (ms) between dispatches inside the same tick.
    #[serde(default = "default_dispatch_delay")]
    pub dispatch_delay_ms: u64,
    /// `From:` substring filter — empty = accept any sender.
    #[serde(default)]
    pub sender_allowlist: Vec<String>,
    /// Named regexes against the body. Each capture group becomes a
    /// `{field}` placeholder in the template.
    #[serde(default)]
    pub extract: HashMap<String, String>,
    /// Skip dispatch when any of these extracted fields ended up empty.
    #[serde(default)]
    pub require_fields: Vec<String>,
    /// Mustache-light template with `{field}` substitutions.
    /// `{subject}`, `{from}`, and `{snippet}` are always available.
    pub message_template: String,
    /// Mark each dispatched message as read in Gmail. Default true.
    #[serde(default = "default_mark_read")]
    pub mark_read_on_dispatch: bool,
    pub deliver: DeliverCfg,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct DeliverCfg {
    pub channel: String,
    #[serde(alias = "recipient")]
    pub to: String,
}

fn default_max_per_tick() -> usize {
    20
}
fn default_dispatch_delay() -> u64 {
    1000
}
fn default_mark_read() -> bool {
    true
}

#[derive(Debug, Deserialize)]
struct GoogleAccountCreds {
    account_id: String,
    client_id_path: String,
    client_secret_path: String,
    token_path: String,
    #[serde(default)]
    scopes: Vec<String>,
}

pub struct GmailHandler {
    clients: DashMap<String, Arc<GoogleAuthClient>>,
}

impl GmailHandler {
    pub fn new() -> Self {
        Self {
            clients: DashMap::new(),
        }
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
        host: Arc<dyn PollerHost>,
    ) -> Result<TickAck, PollerError> {
        let cfg: GmailJobConfig =
            serde_json::from_value(req.config.clone()).map_err(|e| PollerError::Config {
                job: req.job_id.clone(),
                reason: e.to_string(),
            })?;

        let cred_value = host
            .credentials_get("google".into())
            .await
            .map_err(|e| PollerError::Permanent(anyhow::anyhow!("credentials_get: {e}")))?;
        let cred: GoogleAccountCreds =
            serde_json::from_value(cred_value).map_err(|e| PollerError::Permanent(anyhow::anyhow!(
                "credentials_get returned unexpected shape: {e}"
            )))?;

        let client = self.build_client(&cred).await?;

        // Compose effective query.
        let mut q = cfg.query.clone();
        if let Some(bound) = cfg.newer_than.as_deref() {
            if !bound.trim().is_empty() {
                q.push_str(&format!(" newer_than:{bound}"));
            }
        }
        let list_url = format!(
            "https://gmail.googleapis.com/gmail/v1/users/me/messages?q={}&maxResults={}",
            urlencode(&q),
            cfg.max_per_tick
        );
        let list: Value = client
            .authorized_call("GET", &list_url, None)
            .await
            .map_err(|e| classify_google_err(e, "list messages"))?;

        let messages = match list.get("messages").and_then(Value::as_array) {
            Some(v) => v.clone(),
            None => {
                return Ok(TickAck {
                    next_cursor: None,
                    next_interval_hint: None,
                    metrics: Some(TickMetrics::default()),
                });
            }
        };

        let mut compiled_extract: HashMap<String, Regex> = HashMap::new();
        for (name, pat) in &cfg.extract {
            let re = Regex::new(pat).map_err(|e| PollerError::Config {
                job: req.job_id.clone(),
                reason: format!("invalid regex for `{name}`: {e}"),
            })?;
            compiled_extract.insert(name.clone(), re);
        }

        // Resolve outbound topic via reverse-RPC.
        let target_cred = host
            .credentials_get(cfg.deliver.channel.clone())
            .await
            .map_err(|e| PollerError::Permanent(anyhow::anyhow!("credentials_get outbound: {e}")))?;
        let target_account = target_cred
            .get("account_id")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                PollerError::Permanent(anyhow::anyhow!(
                    "outbound credentials_get('{}') missing account_id",
                    cfg.deliver.channel
                ))
            })?
            .to_string();
        let topic = format!("plugin.outbound.{}.{}", cfg.deliver.channel, target_account);

        // Belt-and-suspenders dedup cursor — set of seen message ids,
        // capped at 5000, drop oldest 1000 when over.
        let cursor_bytes = req.cursor_bytes()?;
        let mut seen_set: std::collections::HashSet<String> = cursor_bytes
            .as_deref()
            .and_then(|b| serde_json::from_slice::<Vec<String>>(b).ok())
            .map(|v| v.into_iter().collect())
            .unwrap_or_default();

        let mut items_seen = 0u32;
        let mut items_dispatched = 0u32;
        for (idx, m) in messages.iter().take(cfg.max_per_tick).enumerate() {
            items_seen += 1;
            let Some(id) = m.get("id").and_then(Value::as_str) else {
                continue;
            };
            if seen_set.contains(id) {
                continue;
            }
            match self
                .process_one(id, &cfg, &compiled_extract, &client)
                .await
            {
                Ok(Some(text)) => {
                    let payload = json!({ "to": cfg.deliver.to, "text": text });
                    let payload_bytes = serde_json::to_vec(&payload)
                        .map_err(|e| PollerError::Transient(anyhow::Error::from(e)))?;
                    host.broker_publish(topic.clone(), payload_bytes)
                        .await
                        .map_err(|e| {
                            PollerError::Transient(anyhow::anyhow!("broker_publish: {e}"))
                        })?;
                    items_dispatched += 1;
                    seen_set.insert(id.to_string());
                    if idx + 1 < messages.len() && cfg.dispatch_delay_ms > 0 {
                        tokio::time::sleep(std::time::Duration::from_millis(
                            cfg.dispatch_delay_ms,
                        ))
                        .await;
                    }
                }
                Ok(None) => {
                    // Filter miss / required-field empty — remember
                    // so a future tick does not re-attempt.
                    seen_set.insert(id.to_string());
                }
                Err(e) => {
                    tracing::warn!(
                        message_id = %id,
                        error = %e,
                        "gmail process_one failed",
                    );
                }
            }
        }

        // Cap seen set, drop oldest 1000 by sort order (Gmail ids are
        // roughly monotonic).
        if seen_set.len() > 5000 {
            let mut ids: Vec<String> = seen_set.iter().cloned().collect();
            ids.sort();
            for id in ids.into_iter().take(1000) {
                seen_set.remove(&id);
            }
        }
        let next_cursor =
            serde_json::to_vec(&seen_set.into_iter().collect::<Vec<_>>()).ok();

        Ok(TickAck {
            next_cursor,
            next_interval_hint: None,
            metrics: Some(TickMetrics {
                items_seen,
                items_dispatched,
            }),
        })
    }
}

impl GmailHandler {
    async fn build_client(
        &self,
        cred: &GoogleAccountCreds,
    ) -> Result<Arc<GoogleAuthClient>, PollerError> {
        if let Some(c) = self.clients.get(&cred.account_id) {
            return Ok(c.clone());
        }
        let cid = std::fs::read_to_string(&cred.client_id_path)
            .map(|s| s.trim().to_string())
            .map_err(|e| {
                PollerError::Transient(anyhow::Error::from(e).context("read client_id_path"))
            })?;
        let cs = std::fs::read_to_string(&cred.client_secret_path)
            .map(|s| s.trim().to_string())
            .map_err(|e| {
                PollerError::Transient(anyhow::Error::from(e).context("read client_secret_path"))
            })?;
        let auth_cfg = GoogleAuthConfig {
            client_id: cid,
            client_secret: cs,
            scopes: cred.scopes.clone(),
            token_file: cred.token_path.clone(),
            redirect_port: 0,
        };
        let token_path = PathBuf::from(&cred.token_path);
        let workspace = token_path
            .parent()
            .map(|p| p.to_path_buf())
            .unwrap_or_else(|| std::path::PathBuf::from("."));
        let client = GoogleAuthClient::new_with_sources(
            auth_cfg,
            &workspace,
            Some(SecretSources {
                client_id_path: PathBuf::from(&cred.client_id_path),
                client_secret_path: PathBuf::from(&cred.client_secret_path),
            }),
        );
        client
            .load_from_disk()
            .await
            .map_err(|e| PollerError::Permanent(e.context("gmail: load_from_disk")))?;
        self.clients.insert(cred.account_id.clone(), client.clone());
        Ok(client)
    }

    async fn process_one(
        &self,
        id: &str,
        cfg: &GmailJobConfig,
        extract: &HashMap<String, Regex>,
        client: &Arc<GoogleAuthClient>,
    ) -> Result<Option<String>, anyhow::Error> {
        let url =
            format!("https://gmail.googleapis.com/gmail/v1/users/me/messages/{id}?format=full");
        let msg: Value = client
            .authorized_call("GET", &url, None)
            .await
            .context("get message detail")?;

        let subject = header_value(&msg, "Subject").unwrap_or_default();
        let from = header_value(&msg, "From").unwrap_or_default();
        let snippet = msg
            .get("snippet")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let body = extract_body(&msg);

        if !cfg.sender_allowlist.is_empty() {
            let from_l = from.to_lowercase();
            let allowed = cfg
                .sender_allowlist
                .iter()
                .any(|s| from_l.contains(&s.to_lowercase()));
            if !allowed {
                return Ok(None);
            }
        }

        let mut fields: HashMap<String, String> = HashMap::new();
        fields.insert("subject".into(), subject.clone());
        fields.insert("snippet".into(), snippet.clone());
        fields.insert("from".into(), from.clone());
        for (name, re) in extract {
            let captured = re
                .captures(&body)
                .or_else(|| re.captures(&snippet))
                .and_then(|c| c.get(1))
                .map(|m| m.as_str().trim().to_string())
                .unwrap_or_default();
            fields.insert(name.clone(), captured);
        }

        for req in &cfg.require_fields {
            let v = fields.get(req).map(String::as_str).unwrap_or("");
            if v.is_empty() {
                if cfg.mark_read_on_dispatch {
                    mark_read(client, id).await.ok();
                }
                return Ok(None);
            }
        }

        let text = render_template(&cfg.message_template, &fields);
        if cfg.mark_read_on_dispatch {
            mark_read(client, id).await.ok();
        }

        Ok(Some(text))
    }
}

fn classify_google_err(err: anyhow::Error, ctx: &str) -> PollerError {
    let msg = err.to_string();
    if msg.contains("invalid_grant") || msg.contains("revoked") || msg.contains("401") {
        PollerError::Permanent(err.context(format!("google: {ctx}")))
    } else {
        PollerError::Transient(err.context(format!("google: {ctx}")))
    }
}

async fn mark_read(client: &Arc<GoogleAuthClient>, id: &str) -> anyhow::Result<()> {
    let url = format!("https://gmail.googleapis.com/gmail/v1/users/me/messages/{id}/modify");
    let body = json!({ "removeLabelIds": ["UNREAD"] });
    client
        .authorized_call("POST", &url, Some(body))
        .await
        .context("gmail: mark_read")?;
    Ok(())
}

fn urlencode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.' | '~') {
            out.push(ch);
        } else {
            for b in ch.to_string().as_bytes() {
                out.push_str(&format!("%{b:02X}"));
            }
        }
    }
    out
}

fn header_value(msg: &Value, name: &str) -> Option<String> {
    let headers = msg.get("payload")?.get("headers")?.as_array()?;
    for h in headers {
        if h.get("name").and_then(Value::as_str)? == name {
            return h.get("value").and_then(Value::as_str).map(str::to_string);
        }
    }
    None
}

fn extract_body(msg: &Value) -> String {
    let Some(payload) = msg.get("payload") else {
        return String::new();
    };
    if let Some(text) = find_body(payload, "text/plain") {
        return text;
    }
    if let Some(html) = find_body(payload, "text/html") {
        return strip_html(&html);
    }
    String::new()
}

fn find_body(part: &Value, want: &str) -> Option<String> {
    let mime = part.get("mimeType").and_then(Value::as_str).unwrap_or("");
    if mime == want {
        if let Some(data) = part
            .get("body")
            .and_then(|b| b.get("data"))
            .and_then(Value::as_str)
        {
            return decode_b64url(data);
        }
    }
    if let Some(parts) = part.get("parts").and_then(Value::as_array) {
        for p in parts {
            if let Some(t) = find_body(p, want) {
                return Some(t);
            }
        }
    }
    None
}

fn decode_b64url(s: &str) -> Option<String> {
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(s.trim_end_matches('='))
        .ok()?;
    Some(String::from_utf8_lossy(&bytes).into_owned())
}

fn strip_html(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut in_tag = false;
    for c in s.chars() {
        match c {
            '<' => in_tag = true,
            '>' => in_tag = false,
            _ if !in_tag => out.push(c),
            _ => {}
        }
    }
    out
}

fn render_template(template: &str, fields: &HashMap<String, String>) -> String {
    let mut out = String::with_capacity(template.len());
    let bytes = template.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'{' {
            if let Some(end) = template[i + 1..].find('}') {
                let key = &template[i + 1..i + 1 + end];
                if let Some(v) = fields.get(key) {
                    out.push_str(v);
                    i += 1 + end + 1;
                    continue;
                }
            }
        }
        out.push(bytes[i] as char);
        i += 1;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_minimal_config() {
        let cfg: GmailJobConfig = serde_json::from_value(json!({
            "query": "is:unread",
            "message_template": "{snippet}",
            "deliver": { "channel": "whatsapp", "to": "57300@s.whatsapp.net" },
        }))
        .unwrap();
        assert_eq!(cfg.query, "is:unread");
        assert_eq!(cfg.max_per_tick, 20);
        assert!(cfg.mark_read_on_dispatch);
    }

    #[test]
    fn config_accepts_recipient_alias() {
        let cfg: GmailJobConfig = serde_json::from_value(json!({
            "query": "is:unread",
            "message_template": "x",
            "deliver": { "channel": "telegram", "recipient": "-100" },
        }))
        .unwrap();
        assert_eq!(cfg.deliver.to, "-100");
    }

    #[test]
    fn rejects_missing_template() {
        let r: Result<GmailJobConfig, _> = serde_json::from_value(json!({
            "query": "is:unread",
            "deliver": { "channel": "whatsapp", "to": "x" },
        }));
        assert!(r.is_err());
    }

    #[test]
    fn rejects_unknown_field() {
        let r: Result<GmailJobConfig, _> = serde_json::from_value(json!({
            "query": "is:unread",
            "message_template": "x",
            "deliver": { "channel": "whatsapp", "to": "y" },
            "bogus": "no",
        }));
        assert!(r.is_err());
    }

    #[test]
    fn render_substitutes_known_keys() {
        let mut f = HashMap::new();
        f.insert("name".into(), "Ana".into());
        f.insert("phone".into(), "+57300".into());
        assert_eq!(render_template("Hi {name} ({phone})", &f), "Hi Ana (+57300)");
    }

    #[test]
    fn render_keeps_unknown_placeholders() {
        let f = HashMap::new();
        assert_eq!(render_template("{unknown}", &f), "{unknown}");
    }

    #[test]
    fn strip_html_removes_tags() {
        assert_eq!(strip_html("<p>Hi <b>there</b></p>"), "Hi there");
    }

    #[test]
    fn classify_revoked_as_permanent() {
        let e = anyhow::anyhow!("invalid_grant: revoked");
        assert!(matches!(
            classify_google_err(e, "ctx"),
            PollerError::Permanent(_)
        ));
    }

    #[test]
    fn classify_5xx_as_transient() {
        let e = anyhow::anyhow!("503 backend error");
        assert!(matches!(
            classify_google_err(e, "ctx"),
            PollerError::Transient(_)
        ));
    }

    #[test]
    fn header_value_extracts_subject() {
        let msg = json!({
            "payload": {
                "headers": [
                    { "name": "From", "value": "ana@example.com" },
                    { "name": "Subject", "value": "Hello" }
                ]
            }
        });
        assert_eq!(header_value(&msg, "Subject").as_deref(), Some("Hello"));
        assert_eq!(header_value(&msg, "Cc"), None);
    }

    #[test]
    fn extract_body_finds_text_plain_in_multipart() {
        let plain = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode("hello world");
        let msg = json!({
            "payload": {
                "mimeType": "multipart/alternative",
                "parts": [
                    { "mimeType": "text/html", "body": { "data": "" } },
                    { "mimeType": "text/plain", "body": { "data": plain } }
                ]
            }
        });
        assert_eq!(extract_body(&msg), "hello world");
    }

    #[test]
    fn urlencode_pcts_non_safe() {
        assert_eq!(urlencode("is:unread"), "is%3Aunread");
    }
}
