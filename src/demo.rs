//! Demo traders: two parties that open a small delivery-versus-payment with
//! each other on a timer and allocate both sides of it, so a `DevNet`
//! deployment shows the whole flow — register, allocate, settle — without
//! anyone driving it.
//!
//! In production each counterparty allocates from its own wallet with its
//! own credentials. Here the service's ledger user acts for both traders,
//! which is only acceptable because they are demo parties on a test network.

use std::sync::Arc;
use std::time::Duration;

use canton::daml::{Party, TextMap};
use canton::token::types::metadata as md;
use canton::token::types::v2::{allocation as al, holding as h};
use tokio::sync::Notify;

use crate::config::DemoConfig;
use crate::engine::unix_now;
use crate::ledger::Ledger;
use crate::model::{LegSpec, Settlement, Status};
use crate::store::Store;

pub struct Demo {
    pub ledger: Ledger,
    pub store: Store,
    pub doorbell: Arc<Notify>,
    pub config: DemoConfig,
}

fn side(leg: &LegSpec, kind: al::TransferSide) -> al::TransferLegSide {
    let otherside = match kind {
        al::TransferSide::SenderSide => &leg.receiver,
        al::TransferSide::ReceiverSide => &leg.sender,
    };
    al::TransferLegSide {
        transfer_leg_id: leg.id.clone(),
        side: kind,
        otherside: h::Account {
            owner: Some(otherside.clone()),
            provider: None,
            id: String::new(),
        },
        amount: leg.amount.clone(),
        instrument_id: leg.instrument_id.clone(),
        meta: md::Metadata {
            values: TextMap::new(),
        },
    }
}

/// The sides `trader` authorises: the sender side of every leg it pays and
/// the receiver side of every leg it is paid by — one allocation for both.
fn sides_of(trader: &Party, legs: &[LegSpec]) -> Vec<al::TransferLegSide> {
    let mut sides = Vec::new();
    for leg in legs {
        if &leg.sender == trader {
            sides.push(side(leg, al::TransferSide::SenderSide));
        }
        if &leg.receiver == trader {
            sides.push(side(leg, al::TransferSide::ReceiverSide));
        }
    }
    sides
}

impl Demo {
    pub async fn run(self) {
        // Give the stream and the first pass a moment before the first trade.
        tokio::time::sleep(Duration::from_secs(30)).await;
        let mut timer = tokio::time::interval(self.config.every);
        timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            timer.tick().await;
            if let Err(error) = self.round().await {
                metrics::counter!("settlement_demo_errors_total").increment(1);
                tracing::warn!(%error, "demo round failed");
            }
        }
    }

    async fn round(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let config = &self.config;
        let now = unix_now();
        let ttl = i64::try_from(config.every.as_secs())
            .unwrap_or(900)
            .max(120);
        let legs = vec![
            LegSpec {
                id: "leg-1".into(),
                sender: config.trader_a.clone(),
                receiver: config.trader_b.clone(),
                instrument_id: config.instrument_id.clone(),
                amount: config.amount_a.clone(),
            },
            LegSpec {
                id: "leg-2".into(),
                sender: config.trader_b.clone(),
                receiver: config.trader_a.clone(),
                instrument_id: config.instrument_id.clone(),
                amount: config.amount_b.clone(),
            },
        ];
        let settlement = Settlement {
            id: format!("demo-{now}"),
            legs: legs.clone(),
            created_at: now,
            settle_before: now + ttl,
            status: Status::Pending,
            update_id: None,
            settled_offset: None,
            settled_at: None,
            failures: 0,
            last_error: None,
        };
        let id = settlement.id.clone();
        let deadline = settlement.settle_before.saturating_mul(1_000_000);
        self.store.insert(settlement).await?;
        self.store
            .note(&id, now, "demo", "opened by the demo traders")
            .await?;
        metrics::counter!("settlement_created_total", "source" => "demo").increment(1);
        tracing::info!(settlement = %id, "demo traders opened a settlement");

        for trader in [&config.trader_a, &config.trader_b] {
            let sides = sides_of(trader, &legs);
            match self
                .ledger
                .allocate(trader, &id, sides, &config.instrument_id, deadline)
                .await
            {
                Ok(update_id) => {
                    self.store
                        .note(
                            &id,
                            unix_now(),
                            "allocated",
                            &format!("{trader} in update {update_id}"),
                        )
                        .await?;
                }
                Err(error) => {
                    self.store
                        .note(
                            &id,
                            unix_now(),
                            "allocate_failed",
                            &format!("{trader}: {error}"),
                        )
                        .await?;
                    return Err(error.into());
                }
            }
        }
        self.doorbell.notify_one();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use canton::daml::Numeric;

    fn party(name: &str) -> Party {
        Party::parse(format!("{name}::1220{}", "ef".repeat(32))).unwrap()
    }

    #[test]
    fn each_trader_pays_one_leg_and_is_paid_by_the_other() {
        let (a, b) = (party("a"), party("b"));
        let legs = vec![
            LegSpec {
                id: "leg-1".into(),
                sender: a.clone(),
                receiver: b.clone(),
                instrument_id: "Amulet".into(),
                amount: Numeric::parse("1").unwrap(),
            },
            LegSpec {
                id: "leg-2".into(),
                sender: b.clone(),
                receiver: a.clone(),
                instrument_id: "Amulet".into(),
                amount: Numeric::parse("2").unwrap(),
            },
        ];
        let sides = sides_of(&a, &legs);
        assert_eq!(sides.len(), 2);
        assert_eq!(sides[0].transfer_leg_id, "leg-1");
        assert_eq!(sides[0].side, al::TransferSide::SenderSide);
        assert_eq!(sides[0].otherside.owner, Some(b.clone()));
        assert_eq!(sides[1].transfer_leg_id, "leg-2");
        assert_eq!(sides[1].side, al::TransferSide::ReceiverSide);
        assert_eq!(sides[1].otherside.owner, Some(b));
    }
}
