//! The settlement loop: on every doorbell from the update stream, and on a
//! timer regardless, re-read the allocations naming this executor and act on
//! each pending settlement — settle it, expire it, or leave it waiting.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{Notify, RwLock};

use crate::ledger::Ledger;
use crate::model::{AllocationRecord, Evaluation, MissingSide, Settlement};
use crate::store::Store;

/// Failed settle attempts after which a settlement is marked failed.
pub const GIVE_UP_AFTER: u32 = 5;

/// What the last pass made of a pending settlement, for the API to show.
#[derive(Clone, Debug, Default, serde::Serialize)]
pub struct Observation {
    /// `waiting`, `ready` or `mismatch`.
    pub state: String,
    pub missing: Vec<MissingSide>,
    pub problems: Vec<String>,
    pub allocations: Vec<String>,
    /// Unix seconds.
    pub observed_at: i64,
}

/// Shared between the loop and the API.
#[derive(Clone, Default)]
pub struct Observations(Arc<RwLock<HashMap<String, Observation>>>);

impl Observations {
    pub async fn get(&self, id: &str) -> Option<Observation> {
        self.0.read().await.get(id).cloned()
    }

    async fn replace(&self, next: HashMap<String, Observation>) {
        *self.0.write().await = next;
    }
}

/// A count as a gauge value. Counts here are far below 2^52, where `f64`
/// starts dropping integers.
#[allow(clippy::cast_precision_loss)]
fn as_gauge(count: usize) -> f64 {
    count as f64
}

pub fn unix_now() -> i64 {
    let since_epoch = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    i64::try_from(since_epoch.as_secs()).unwrap_or(i64::MAX)
}

pub struct Engine {
    pub ledger: Ledger,
    pub store: Store,
    pub doorbell: Arc<Notify>,
    pub observations: Observations,
    pub reconcile_every: Duration,
}

impl Engine {
    pub async fn run(self) {
        let mut timer = tokio::time::interval(self.reconcile_every);
        timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                () = self.doorbell.notified() => {}
                _ = timer.tick() => {}
            }
            if let Err(error) = Box::pin(self.pass()).await {
                metrics::counter!("settlement_pass_errors_total").increment(1);
                tracing::warn!(%error, "settlement pass failed; the next one will retry");
            }
        }
    }

    /// One pass over every pending settlement.
    async fn pass(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let allocations = self.ledger.allocations().await?;
        let mut by_settlement: BTreeMap<String, Vec<AllocationRecord>> = BTreeMap::new();
        for record in allocations {
            by_settlement
                .entry(record.view.settlement.id.clone())
                .or_default()
                .push(record);
        }
        let pending = self.store.pending().await?;
        metrics::gauge!("settlement_pending").set(as_gauge(pending.len()));

        let known: std::collections::HashSet<&str> =
            pending.iter().map(|s| s.id.as_str()).collect();
        let unregistered = by_settlement
            .keys()
            .filter(|id| !known.contains(id.as_str()))
            .count();
        metrics::gauge!("settlement_unregistered_allocation_groups").set(as_gauge(unregistered));

        let mut observations = HashMap::new();
        for settlement in pending {
            let allocations = by_settlement.remove(&settlement.id).unwrap_or_default();
            if let Some(observation) = self.act(&settlement, &allocations).await {
                observations.insert(settlement.id.clone(), observation);
            }
        }
        self.observations.replace(observations).await;
        Ok(())
    }

    /// Settle, expire or wait. Returns what was observed when the settlement
    /// is still pending afterwards.
    async fn act(
        &self,
        settlement: &Settlement,
        allocations: &[AllocationRecord],
    ) -> Option<Observation> {
        let now = unix_now();
        if now >= settlement.settle_before {
            self.expire(settlement, allocations, now).await;
            return None;
        }
        let ids: Vec<String> = allocations.iter().map(|a| a.contract_id.clone()).collect();
        match crate::model::evaluate(settlement, allocations, now.saturating_mul(1_000_000)) {
            Evaluation::Waiting(missing) => Some(Observation {
                state: "waiting".into(),
                missing,
                problems: Vec::new(),
                allocations: ids,
                observed_at: now,
            }),
            Evaluation::Mismatch(problems) => Some(Observation {
                state: "mismatch".into(),
                missing: Vec::new(),
                problems,
                allocations: ids,
                observed_at: now,
            }),
            Evaluation::Ready(ready) => {
                tracing::info!(settlement = %settlement.id, legs = ready.legs.len(),
                    allocations = ready.allocation_ids.len(), "settling");
                match self.ledger.settle(&ready).await {
                    Ok(committed) => {
                        let at = unix_now();
                        if let Err(error) = self
                            .store
                            .mark_settled(
                                &settlement.id,
                                &committed.update_id,
                                committed.offset,
                                at,
                            )
                            .await
                        {
                            tracing::error!(%error, settlement = %settlement.id,
                                update_id = %committed.update_id, "settled, but could not record it");
                        }
                        metrics::counter!("settlement_settled_total").increment(1);
                        metrics::histogram!("settlement_time_to_settle_seconds").record(
                            Duration::from_secs(
                                u64::try_from(at - settlement.created_at).unwrap_or(0),
                            )
                            .as_secs_f64(),
                        );
                        tracing::info!(settlement = %settlement.id, update_id = %committed.update_id,
                            offset = committed.offset, recovered = committed.recovered, "settled");
                        None
                    }
                    Err(error) => {
                        metrics::counter!("settlement_settle_failures_total").increment(1);
                        let failures = self
                            .store
                            .record_failure(&settlement.id, now, &error.to_string(), GIVE_UP_AFTER)
                            .await
                            .unwrap_or(0);
                        tracing::warn!(%error, settlement = %settlement.id, failures, "settling failed");
                        Some(Observation {
                            state: "ready".into(),
                            missing: Vec::new(),
                            problems: vec![format!("the last attempt to settle failed: {error}")],
                            allocations: ids,
                            observed_at: now,
                        })
                    }
                }
            }
        }
    }

    /// Past its deadline: release every allocation still reserved for it, so
    /// no trader's holdings stay locked waiting for a settlement that will not
    /// happen, then mark it expired.
    async fn expire(&self, settlement: &Settlement, allocations: &[AllocationRecord], now: i64) {
        let mut cancelled = 0;
        for record in allocations {
            match self.ledger.cancel(&record.contract_id).await {
                Ok(()) => cancelled += 1,
                Err(error) => {
                    tracing::warn!(%error, settlement = %settlement.id,
                        allocation = %record.contract_id, "could not cancel an allocation");
                    let _ = self
                        .store
                        .note(
                            &settlement.id,
                            now,
                            "cancel_failed",
                            &format!("{}: {error}", record.contract_id),
                        )
                        .await;
                }
            }
        }
        let message = format!(
            "deadline passed with {} of {} allocation(s) cancelled",
            cancelled,
            allocations.len()
        );
        if let Err(error) = self.store.mark_expired(&settlement.id, now, &message).await {
            tracing::error!(%error, settlement = %settlement.id, "could not record the expiry");
        }
        metrics::counter!("settlement_expired_total").increment(1);
        tracing::info!(settlement = %settlement.id, cancelled, "expired");
    }
}
