//! The HTTP API: register a settlement, read where settlements stand, and the
//! operational endpoints (health, Prometheus metrics, a status page).
//!
//! Reads are open. Registering a settlement needs the API key as a bearer
//! token; a service started without one is read-only.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::Json;
use axum::Router;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::get;
use canton::daml::{Numeric, Party};
use metrics_exporter_prometheus::PrometheusHandle;
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;

use crate::engine::{Observation, Observations, unix_now};
use crate::ledger::Ledger;
use crate::model::{LegSpec, Settlement, Side, Status};
use crate::store::{HistoryEntry, Store, StoreError};

const MAX_LEGS: usize = 20;
const MIN_TTL: Duration = Duration::from_secs(60);
const MAX_TTL: Duration = Duration::from_secs(7 * 24 * 3600);

pub struct AppState {
    pub store: Store,
    pub observations: Observations,
    pub doorbell: Arc<Notify>,
    pub executor: Party,
    pub registry_admin: Party,
    /// `None` in tests; health then reports the ledger as unknown.
    pub ledger: Option<Ledger>,
    pub api_key: Option<String>,
    pub default_ttl: Duration,
    pub metrics: Option<PrometheusHandle>,
}

pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/healthz", get(healthz))
        .route("/metrics", get(prometheus))
        .route("/v1/settlements", get(list).post(create))
        .route("/v1/settlements/{id}", get(detail))
        .with_state(state)
}

/// An error as the API returns it: a status and `{"error": "..."}`.
#[derive(Debug)]
pub struct ApiError(StatusCode, String);

impl ApiError {
    fn bad_request(message: impl Into<String>) -> Self {
        Self(StatusCode::BAD_REQUEST, message.into())
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.0, Json(serde_json::json!({ "error": self.1 }))).into_response()
    }
}

impl From<StoreError> for ApiError {
    fn from(error: StoreError) -> Self {
        match error {
            StoreError::Exists(id) => Self(
                StatusCode::CONFLICT,
                format!("a settlement with id {id:?} already exists"),
            ),
            other => {
                tracing::error!(error = %other, "store error");
                Self(StatusCode::INTERNAL_SERVER_ERROR, "internal error".into())
            }
        }
    }
}

fn rfc3339(unix_seconds: i64) -> String {
    let at = UNIX_EPOCH + Duration::from_secs(u64::try_from(unix_seconds).unwrap_or(0));
    humantime::format_rfc3339_seconds(at).to_string()
}

#[derive(Serialize)]
struct LegView {
    id: String,
    sender: Party,
    receiver: Party,
    instrument_id: String,
    amount: Numeric,
}

#[derive(Serialize)]
struct SettlementView {
    id: String,
    status: Status,
    created_at: String,
    settle_before: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    settled_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    update_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    settled_offset: Option<i64>,
    failures: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_error: Option<String>,
    legs: Vec<LegView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    observation: Option<Observation>,
    #[serde(skip_serializing_if = "Option::is_none")]
    history: Option<Vec<HistoryEntry>>,
}

fn view(
    settlement: Settlement,
    observation: Option<Observation>,
    history: Option<Vec<HistoryEntry>>,
) -> SettlementView {
    SettlementView {
        id: settlement.id,
        status: settlement.status,
        created_at: rfc3339(settlement.created_at),
        settle_before: rfc3339(settlement.settle_before),
        settled_at: settlement.settled_at.map(rfc3339),
        update_id: settlement.update_id,
        settled_offset: settlement.settled_offset,
        failures: settlement.failures,
        last_error: settlement.last_error,
        legs: settlement
            .legs
            .into_iter()
            .map(|leg| LegView {
                id: leg.id,
                sender: leg.sender,
                receiver: leg.receiver,
                instrument_id: leg.instrument_id,
                amount: leg.amount,
            })
            .collect(),
        observation,
        history,
    }
}

#[derive(Deserialize)]
struct ListQuery {
    status: Option<String>,
    limit: Option<u32>,
}

async fn list(
    State(state): State<Arc<AppState>>,
    Query(query): Query<ListQuery>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let status = match query.status.as_deref() {
        None => None,
        Some(text) => Some(Status::parse(text).ok_or_else(|| {
            ApiError::bad_request("status is one of pending, settled, expired, failed")
        })?),
    };
    let limit = query.limit.unwrap_or(50).clamp(1, 500);
    let mut out = Vec::new();
    for settlement in state.store.list(status, limit).await? {
        let observation = state.observations.get(&settlement.id).await;
        out.push(view(settlement, observation, None));
    }
    Ok(Json(serde_json::json!({ "settlements": out })))
}

async fn detail(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<SettlementView>, ApiError> {
    let settlement = state
        .store
        .get(&id)
        .await?
        .ok_or_else(|| ApiError(StatusCode::NOT_FOUND, format!("no settlement {id:?}")))?;
    let observation = state.observations.get(&id).await;
    let history = state.store.history(&id).await?;
    Ok(Json(view(settlement, observation, Some(history))))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LegRequest {
    id: Option<String>,
    sender: String,
    receiver: String,
    instrument_id: String,
    amount: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateRequest {
    id: Option<String>,
    legs: Vec<LegRequest>,
    ttl_secs: Option<u64>,
}

#[derive(Serialize)]
struct SideToAllocate {
    leg_id: String,
    side: Side,
    otherside: Party,
    instrument_id: String,
    amount: Numeric,
}

#[derive(Serialize)]
struct PartyToAllocate {
    party: Party,
    sides: Vec<SideToAllocate>,
}

#[derive(Serialize)]
struct Created {
    settlement: SettlementView,
    /// What each party must put in its allocation: the settlement id, this
    /// executor, and these sides.
    executor: Party,
    registry_admin: Party,
    allocations_needed: Vec<PartyToAllocate>,
}

fn valid_id(id: &str) -> bool {
    (1..=64).contains(&id.len())
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b':'))
}

fn is_positive(amount: &Numeric) -> bool {
    let text = amount.to_string();
    !text.starts_with('-') && text.bytes().any(|b| (b'1'..=b'9').contains(&b))
}

/// Compare without leaking how much of the key matched through timing.
fn same_secret(given: &str, expected: &str) -> bool {
    let (given, expected) = (given.as_bytes(), expected.as_bytes());
    let mut difference = u8::from(given.len() != expected.len());
    for (index, byte) in expected.iter().enumerate() {
        difference |= byte ^ given.get(index).copied().unwrap_or(0);
    }
    difference == 0
}

fn authorize(state: &AppState, headers: &HeaderMap) -> Result<(), ApiError> {
    let Some(expected) = state.api_key.as_deref() else {
        return Err(ApiError(
            StatusCode::FORBIDDEN,
            "this service is read-only: it was started without SETTLEMENT_API_KEY".into(),
        ));
    };
    let given = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "));
    match given {
        Some(given) if same_secret(given, expected) => Ok(()),
        _ => Err(ApiError(
            StatusCode::UNAUTHORIZED,
            "registering a settlement needs the API key as a bearer token".into(),
        )),
    }
}

fn parse_legs(legs: Vec<LegRequest>) -> Result<Vec<LegSpec>, ApiError> {
    if legs.is_empty() || legs.len() > MAX_LEGS {
        return Err(ApiError::bad_request(format!(
            "a settlement has between 1 and {MAX_LEGS} legs"
        )));
    }
    let mut seen = BTreeSet::new();
    let mut out = Vec::with_capacity(legs.len());
    for (index, leg) in legs.into_iter().enumerate() {
        let id = leg.id.unwrap_or_else(|| format!("leg-{}", index + 1));
        if !valid_id(&id) {
            return Err(ApiError::bad_request(format!(
                "leg id {id:?}: 1-64 characters of letters, digits and -_.:"
            )));
        }
        if !seen.insert(id.clone()) {
            return Err(ApiError::bad_request(format!(
                "leg id {id:?} appears twice"
            )));
        }
        let party = |field: &str, text: &str| {
            crate::model::party_id(text)
                .map_err(|e| ApiError::bad_request(format!("leg {id:?} {field}: {e}")))
        };
        let sender = party("sender", &leg.sender)?;
        let receiver = party("receiver", &leg.receiver)?;
        let amount = Numeric::parse(&leg.amount)
            .map_err(|e| ApiError::bad_request(format!("leg {id:?} amount: {e}")))?;
        if !is_positive(&amount) {
            return Err(ApiError::bad_request(format!(
                "leg {id:?}: the amount must be positive"
            )));
        }
        if leg.instrument_id.is_empty() || leg.instrument_id.len() > 128 {
            return Err(ApiError::bad_request(format!(
                "leg {id:?}: instrument_id is 1-128 characters"
            )));
        }
        out.push(LegSpec {
            id,
            sender,
            receiver,
            instrument_id: leg.instrument_id,
            amount,
        });
    }
    Ok(out)
}

fn allocations_needed(legs: &[LegSpec]) -> Vec<PartyToAllocate> {
    let mut by_party: BTreeMap<Party, Vec<SideToAllocate>> = BTreeMap::new();
    for leg in legs {
        for (side, party, otherside) in [
            (Side::Sender, &leg.sender, &leg.receiver),
            (Side::Receiver, &leg.receiver, &leg.sender),
        ] {
            by_party
                .entry(party.clone())
                .or_default()
                .push(SideToAllocate {
                    leg_id: leg.id.clone(),
                    side,
                    otherside: otherside.clone(),
                    instrument_id: leg.instrument_id.clone(),
                    amount: leg.amount.clone(),
                });
        }
    }
    by_party
        .into_iter()
        .map(|(party, sides)| PartyToAllocate { party, sides })
        .collect()
}

async fn create(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(request): Json<CreateRequest>,
) -> Result<(StatusCode, Json<Created>), ApiError> {
    authorize(&state, &headers)?;
    let now = unix_now();
    let id = match request.id {
        Some(id) if valid_id(&id) => id,
        Some(_) => {
            return Err(ApiError::bad_request(
                "id: 1-64 characters of letters, digits and -_.:",
            ));
        }
        None => {
            let micros = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_micros();
            format!("dvp-{micros}")
        }
    };
    let ttl = request
        .ttl_secs
        .map_or(state.default_ttl, Duration::from_secs);
    if !(MIN_TTL..=MAX_TTL).contains(&ttl) {
        return Err(ApiError::bad_request(format!(
            "ttl_secs is between {} and {}",
            MIN_TTL.as_secs(),
            MAX_TTL.as_secs()
        )));
    }
    let legs = parse_legs(request.legs)?;
    let settlement = Settlement {
        id: id.clone(),
        legs: legs.clone(),
        created_at: now,
        settle_before: now + i64::try_from(ttl.as_secs()).unwrap_or(i64::MAX - now),
        status: Status::Pending,
        update_id: None,
        settled_offset: None,
        settled_at: None,
        failures: 0,
        last_error: None,
    };
    state.store.insert(settlement.clone()).await?;
    metrics::counter!("settlement_created_total", "source" => "api").increment(1);
    tracing::info!(settlement = %id, legs = legs.len(), "registered");
    state.doorbell.notify_one();
    Ok((
        StatusCode::CREATED,
        Json(Created {
            settlement: view(settlement, None, None),
            executor: state.executor.clone(),
            registry_admin: state.registry_admin.clone(),
            allocations_needed: allocations_needed(&legs),
        }),
    ))
}

async fn healthz(State(state): State<Arc<AppState>>) -> Response {
    let Some(ledger) = &state.ledger else {
        return (
            StatusCode::OK,
            Json(serde_json::json!({ "status": "ok", "ledger": "unknown" })),
        )
            .into_response();
    };
    match tokio::time::timeout(Duration::from_secs(5), ledger.ledger_end()).await {
        Ok(Ok(offset)) => (
            StatusCode::OK,
            Json(serde_json::json!({ "status": "ok", "ledger_end": offset })),
        )
            .into_response(),
        Ok(Err(error)) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({ "status": "degraded", "ledger": error.to_string() })),
        )
            .into_response(),
        Err(_) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({ "status": "degraded", "ledger": "timed out" })),
        )
            .into_response(),
    }
}

async fn prometheus(State(state): State<Arc<AppState>>) -> Response {
    match &state.metrics {
        Some(handle) => (
            [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
            handle.render(),
        )
            .into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

fn escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

fn short(party: &Party) -> String {
    party
        .as_str()
        .split("::")
        .next()
        .unwrap_or_default()
        .to_string()
}

async fn index(State(state): State<Arc<AppState>>) -> Result<Html<String>, ApiError> {
    let counts = state.store.counts().await?;
    let recent = state.store.list(None, 50).await?;
    let count = |status: Status| {
        counts
            .iter()
            .find(|(s, _)| *s == status)
            .map_or(0, |(_, n)| *n)
    };
    let mut rows = String::new();
    for settlement in &recent {
        let legs = settlement
            .legs
            .iter()
            .map(|leg| {
                format!(
                    "{} → {}: {} {}",
                    escape(&short(&leg.sender)),
                    escape(&short(&leg.receiver)),
                    escape(&leg.amount.to_string()),
                    escape(&leg.instrument_id)
                )
            })
            .collect::<Vec<_>>()
            .join("<br>");
        let _ = write!(
            rows,
            "<tr><td><a href=\"/v1/settlements/{id}\">{id}</a></td><td class=\"{status}\">{status}</td>\
             <td>{created}</td><td>{legs}</td><td><code>{update}</code></td></tr>",
            id = escape(&settlement.id),
            status = settlement.status.as_str(),
            created = rfc3339(settlement.created_at),
            update = escape(settlement.update_id.as_deref().unwrap_or("")),
        );
    }
    Ok(Html(format!(
        r#"<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Canton settlement service</title>
<style>
body{{font:14px/1.5 system-ui,sans-serif;margin:2rem auto;max-width:72rem;padding:0 1rem;color:#1d2433}}
table{{border-collapse:collapse;width:100%}}td,th{{padding:.4rem .6rem;border-bottom:1px solid #e3e6ec;text-align:left;vertical-align:top}}
code{{font-size:12px;word-break:break-all}}.settled{{color:#0a7a3d}}.pending{{color:#8a6100}}.expired,.failed{{color:#b42318}}
.counts span{{margin-right:1.5rem}}
</style></head><body>
<h1>Canton settlement service</h1>
<p>Delivery-versus-payment on the Canton Network: CIP-0112 allocations settled in one transaction,
built on the <a href="https://github.com/nodejumper-org/canton-rust-sdk">Canton Rust SDK</a>.</p>
<p>Executor: <code>{executor}</code><br>Registry admin: <code>{admin}</code></p>
<p class="counts"><span>pending {pending}</span><span>settled {settled}</span><span>expired {expired}</span><span>failed {failed}</span></p>
<table><thead><tr><th>Settlement</th><th>Status</th><th>Created</th><th>Legs</th><th>Update</th></tr></thead>
<tbody>{rows}</tbody></table>
<p>JSON: <a href="/v1/settlements">/v1/settlements</a> · <a href="/healthz">/healthz</a> · <a href="/metrics">/metrics</a></p>
</body></html>"#,
        executor = escape(state.executor.as_str()),
        admin = escape(state.registry_admin.as_str()),
        pending = count(Status::Pending),
        settled = count(Status::Settled),
        expired = count(Status::Expired),
        failed = count(Status::Failed),
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use http_body_util::BodyExt as _;
    use tower::ServiceExt as _;

    fn party(name: &str) -> String {
        format!("{name}::1220{}", "12".repeat(32))
    }

    fn state(api_key: Option<&str>) -> Arc<AppState> {
        Arc::new(AppState {
            store: Store::in_memory().unwrap(),
            observations: Observations::default(),
            doorbell: Arc::new(Notify::new()),
            executor: Party::parse(party("executor")).unwrap(),
            registry_admin: Party::parse(party("dso")).unwrap(),
            ledger: None,
            api_key: api_key.map(str::to_string),
            default_ttl: Duration::from_secs(600),
            metrics: None,
        })
    }

    fn dvp_body() -> String {
        serde_json::json!({
            "id": "trade-1",
            "legs": [
                { "sender": party("alice"), "receiver": party("bob"), "instrument_id": "Amulet", "amount": "1.0" },
                { "sender": party("bob"), "receiver": party("alice"), "instrument_id": "Amulet", "amount": "0.5" }
            ]
        })
        .to_string()
    }

    async fn call(app: Router, request: Request<Body>) -> (StatusCode, serde_json::Value) {
        let response = app.oneshot(request).await.unwrap();
        let status = response.status();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        (
            status,
            serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null),
        )
    }

    fn post(body: String, key: Option<&str>) -> Request<Body> {
        let mut builder =
            Request::post("/v1/settlements").header("content-type", "application/json");
        if let Some(key) = key {
            builder = builder.header("authorization", format!("Bearer {key}"));
        }
        builder.body(Body::from(body)).unwrap()
    }

    #[tokio::test]
    async fn registering_returns_what_each_party_must_allocate() {
        let app = router(state(Some("k")));
        let (status, body) = call(app.clone(), post(dvp_body(), Some("k"))).await;
        assert_eq!(status, StatusCode::CREATED, "{body}");
        assert_eq!(body["settlement"]["id"], "trade-1");
        assert_eq!(body["settlement"]["status"], "pending");
        assert_eq!(body["settlement"]["legs"][1]["id"], "leg-2");
        let needed = body["allocations_needed"].as_array().unwrap();
        assert_eq!(needed.len(), 2);
        assert_eq!(needed[0]["sides"].as_array().unwrap().len(), 2);

        let (status, detail) = call(
            app,
            Request::get("/v1/settlements/trade-1")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(detail["history"][0]["kind"], "created");
    }

    #[tokio::test]
    async fn the_same_id_twice_is_a_conflict() {
        let app = router(state(Some("k")));
        assert_eq!(
            call(app.clone(), post(dvp_body(), Some("k"))).await.0,
            StatusCode::CREATED
        );
        assert_eq!(
            call(app, post(dvp_body(), Some("k"))).await.0,
            StatusCode::CONFLICT
        );
    }

    #[tokio::test]
    async fn registering_needs_the_api_key() {
        assert_eq!(
            call(router(state(Some("k"))), post(dvp_body(), None))
                .await
                .0,
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            call(router(state(Some("k"))), post(dvp_body(), Some("wrong")))
                .await
                .0,
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            call(router(state(None)), post(dvp_body(), Some("k")))
                .await
                .0,
            StatusCode::FORBIDDEN
        );
    }

    #[tokio::test]
    async fn bad_legs_are_refused_with_the_reason() {
        let app = router(state(Some("k")));
        let cases = [
            serde_json::json!({ "legs": [] }),
            serde_json::json!({ "legs": [{ "sender": "not a party", "receiver": party("bob"), "instrument_id": "Amulet", "amount": "1" }] }),
            serde_json::json!({ "legs": [{ "sender": party("alice"), "receiver": party("bob"), "instrument_id": "Amulet", "amount": "0" }] }),
            serde_json::json!({ "legs": [{ "sender": party("alice"), "receiver": party("bob"), "instrument_id": "Amulet", "amount": "-1" }] }),
            serde_json::json!({ "ttl_secs": 5, "legs": [{ "sender": party("alice"), "receiver": party("bob"), "instrument_id": "Amulet", "amount": "1" }] }),
        ];
        for case in cases {
            let (status, body) = call(app.clone(), post(case.to_string(), Some("k"))).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{case} -> {body}");
            assert!(body["error"].is_string());
        }
    }

    #[tokio::test]
    async fn an_unknown_settlement_is_not_found() {
        let request = Request::get("/v1/settlements/nope")
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            call(router(state(None)), request).await.0,
            StatusCode::NOT_FOUND
        );
    }

    #[test]
    fn secrets_compare_equal_only_when_equal() {
        assert!(same_secret("abc", "abc"));
        assert!(!same_secret("abd", "abc"));
        assert!(!same_secret("ab", "abc"));
        assert!(!same_secret("abcd", "abc"));
    }
}
