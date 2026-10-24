//! Everything the service says to the participant and the registry, through
//! the Canton Rust SDK.

use std::sync::Arc;
use std::time::Duration;

use canton::auth::{OidcConfig, TokenProvider};
use canton::daml::{self as rt, Contract as _};
use canton::ledger::CantonClient;
use canton::ledger::request::{ActiveContractsRequest, UpdatesRequest};
use canton::token::RegistryClient;
use canton::token::types::metadata as md;
use canton::token::types::v2::{allocation as al, holding as h};
use futures_util::StreamExt as _;
use tokio::sync::Notify;

use crate::config::{Config, LedgerAuth};
use crate::model::{AllocationRecord, Ready};
use crate::store::Store;

/// How long to look for the completion of a submission whose response was
/// lost before deciding it never landed.
const RECOVERY_WINDOW: Duration = Duration::from_secs(60);

/// What a settlement transaction committed as.
#[derive(Clone, Debug)]
pub struct Committed {
    pub update_id: String,
    pub offset: i64,
    /// The response was lost and the outcome was read back from the
    /// completion stream instead.
    pub recovered: bool,
}

#[derive(Clone)]
pub struct Ledger {
    client: CantonClient,
    registry: RegistryClient,
    executor: rt::Party,
    admin: rt::Party,
}

/// The interface every token-standard V2 allocation implements, as the
/// filter string the Ledger API takes.
fn allocation_interface() -> String {
    let id = al::Allocation::template_id();
    format!("{}:{}:{}", id.package_id, id.module_name, id.entity_name)
}

fn now_micros() -> i64 {
    let since_epoch = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    i64::try_from(since_epoch.as_micros()).unwrap_or(i64::MAX)
}

fn no_meta() -> md::Metadata {
    md::Metadata {
        values: rt::TextMap::new(),
    }
}

impl Ledger {
    /// Connect to the participant and ask the registry who administers its
    /// instruments.
    ///
    /// # Errors
    /// The registry cannot be reached or does not answer as a token-standard
    /// registry.
    pub async fn connect(config: &Config) -> canton::Result<Self> {
        let ledger = canton::Config::new(config.ledger_url.clone());
        let ledger = match &config.auth {
            LedgerAuth::None => ledger,
            LedgerAuth::Token(token) => ledger.with_token(token.clone()),
            LedgerAuth::Oidc {
                token_url,
                client_id,
                client_secret,
                audience,
            } => {
                let mut oidc =
                    OidcConfig::new(token_url.clone(), client_id.clone(), client_secret.clone());
                if let Some(audience) = audience {
                    oidc = oidc.with_audience(audience.clone());
                }
                ledger.with_oidc(TokenProvider::new(oidc))
            }
        };
        let client = CantonClient::connect_lazy(ledger)?;
        let registry = RegistryClient::new(&config.registry_url)?;
        let info = registry.info().await?;
        let admin = rt::Party::parse(&info.admin_id).map_err(|e| {
            canton::Error::UnexpectedResponse(format!("the registry's admin id: {e}"))
        })?;
        Ok(Self {
            client,
            registry,
            executor: config.executor.clone(),
            admin,
        })
    }

    #[must_use]
    pub fn executor(&self) -> &rt::Party {
        &self.executor
    }

    #[must_use]
    pub fn registry_admin(&self) -> &rt::Party {
        &self.admin
    }

    /// # Errors
    /// The participant cannot be reached.
    pub async fn ledger_end(&self) -> canton::Result<i64> {
        self.client.ledger_end().await
    }

    /// Every active V2 allocation that names this service as an executor,
    /// read from the active contract set through the `Allocation` interface.
    ///
    /// # Errors
    /// The read fails, or a view does not decode.
    pub async fn allocations(&self) -> canton::Result<Vec<AllocationRecord>> {
        let end = self.client.ledger_end().await?;
        let request = ActiveContractsRequest::new(vec![self.executor.to_string()], end)
            .for_interfaces([allocation_interface()])?;
        let stream = self.client.active_contracts_with(request).await?;
        futures_util::pin_mut!(stream);
        let mut out = Vec::new();
        while let Some(item) = stream.next().await {
            let Some(event) = item?.created_event else {
                continue;
            };
            let Some(record) = event
                .interface_views
                .iter()
                .find_map(|view| view.view_value.as_ref())
            else {
                continue;
            };
            let view: al::AllocationView = rt::from_record(record).map_err(|e| {
                canton::Error::UnexpectedResponse(format!(
                    "allocation {} has a view that does not decode: {e}",
                    event.contract_id
                ))
            })?;
            if view.settlement.executors.contains(&self.executor) {
                out.push(AllocationRecord {
                    contract_id: event.contract_id,
                    view,
                });
            }
        }
        Ok(out)
    }

    /// Settle `ready` as one batch: every leg moves in one transaction, or
    /// none does.
    ///
    /// A submission whose response is lost may still have committed, so a
    /// retriable failure is not retried blind: the command's outcome is read
    /// back from the completion stream by its change id first.
    ///
    /// # Errors
    /// The registry refuses the batch, the ledger rejects it, or its outcome
    /// cannot be established.
    pub async fn settle(&self, ready: &Ready) -> canton::Result<Committed> {
        let finalized = ready
            .allocation_ids
            .iter()
            .map(|cid| al::FinalizedAllocation {
                allocation_cid: rt::ContractId::new(cid.clone()),
                extra_transfer_leg_sides: Vec::new(),
                next_iteration_funding: None,
            })
            .collect();
        let command = canton::token::v2::settle_batch(
            &self.registry,
            ready.settlement.clone(),
            ready.legs.clone(),
            finalized,
            vec![self.executor.clone()],
        )
        .await?;
        let before = self.client.ledger_end().await?;
        let submission = self
            .client
            .submission(command.into_submit(self.executor.as_str()));
        match submission.submit_and_wait_for_transaction().await {
            Ok(transaction) => Ok(Committed {
                update_id: transaction.update_id,
                offset: transaction.offset,
                recovered: false,
            }),
            Err(error) if error.is_retriable() => {
                tracing::warn!(%error, settlement = %ready.settlement.id,
                    "the settlement's outcome is unknown; reading it back from the completion stream");
                let completion = submission.recover(before, RECOVERY_WINDOW).await?;
                Ok(Committed {
                    update_id: completion.update_id,
                    offset: completion.offset,
                    recovered: true,
                })
            }
            Err(error) => Err(error),
        }
    }

    /// Cancel an allocation naming this executor, releasing the sender's
    /// holdings — what happens to every allocation of a settlement whose
    /// deadline passed.
    ///
    /// # Errors
    /// The registry or the ledger refuses.
    pub async fn cancel(&self, allocation_id: &str) -> canton::Result<()> {
        let command = canton::token::v2::cancel(
            &self.registry,
            &rt::ContractId::new(allocation_id.to_string()),
            vec![self.executor.clone()],
        )
        .await?;
        self.client
            .submit_and_wait(command.into_submit(self.executor.as_str()))
            .await?;
        Ok(())
    }

    /// Allocate on behalf of `trader`: reserve its holdings for the sender
    /// sides it names and consent to the receiver sides. Used by the demo
    /// traders; a real counterparty allocates from its own wallet.
    ///
    /// # Errors
    /// The trader has no spendable holdings of the instrument, or the
    /// registry or ledger refuses.
    pub async fn allocate(
        &self,
        trader: &rt::Party,
        settlement_id: &str,
        sides: Vec<al::TransferLegSide>,
        instrument_id: &str,
        deadline_micros: i64,
    ) -> canton::Result<String> {
        let pays = sides
            .iter()
            .any(|side| side.side == al::TransferSide::SenderSide);
        let holdings: Vec<rt::ContractId<h::Holding>> = if pays {
            let spendable =
                canton::token::holdings::spendable(&self.client, trader.as_str(), instrument_id)
                    .await?;
            if spendable.is_empty() {
                return Err(canton::Error::InvalidRequest(format!(
                    "{trader} has no unlocked holdings of {instrument_id}"
                )));
            }
            spendable
                .iter()
                .map(canton::token::holdings::HoldingSummary::typed)
                .collect()
        } else {
            Vec::new()
        };
        let account = |party: &rt::Party| h::Account {
            owner: Some(party.clone()),
            provider: None,
            id: String::new(),
        };
        let command = canton::token::v2::allocate(
            &self.registry,
            al::SettlementInfo {
                executors: vec![self.executor.clone()],
                id: settlement_id.to_string(),
                cid: None,
                meta: no_meta(),
            },
            al::AllocationSpecification {
                admin: self.admin.clone(),
                authorizer: account(trader),
                transfer_leg_sides: sides,
                settlement_deadline: Some(rt::Timestamp(deadline_micros)),
                next_iteration_funding: None,
                committed: false,
                meta: no_meta(),
            },
            rt::Timestamp(now_micros()),
            holdings,
            vec![trader.clone()],
        )
        .await?;
        let response = self
            .client
            .submit_and_wait(command.into_submit(trader.as_str()))
            .await?;
        Ok(response.update_id)
    }

    /// Follow the update stream for allocations naming this executor and ring
    /// `doorbell` on each one. The stream says *that* something changed; what
    /// changed is always re-read from the active contract set, so a missed or
    /// duplicated update cannot leave the service believing something the
    /// ledger does not hold.
    ///
    /// The position is saved after every update and resumed from on restart.
    /// A resume point the participant has pruned is skipped forward to the
    /// ledger end: nothing is lost by it, because the next pass re-reads the
    /// active contract set anyway.
    pub async fn watch(self, store: Store, doorbell: Arc<Notify>) {
        let interface = allocation_interface();
        loop {
            let begin = match store.checkpoint().await {
                Ok(Some(offset)) => offset,
                Ok(None) => match self.client.ledger_end().await {
                    Ok(end) => end,
                    Err(error) => {
                        tracing::warn!(%error, "cannot read the ledger end; retrying");
                        tokio::time::sleep(Duration::from_secs(5)).await;
                        continue;
                    }
                },
                Err(error) => {
                    tracing::error!(%error, "cannot read the stream checkpoint; retrying");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    continue;
                }
            };
            let request = match UpdatesRequest::new(vec![self.executor.to_string()], begin)
                .for_interfaces([interface.clone()])
            {
                Ok(request) => request,
                Err(error) => {
                    tracing::error!(%error, "the update filter is invalid; the stream is off");
                    return;
                }
            };
            tracing::info!(offset = begin, "following allocation updates");
            let stream = self.client.updates_resumable_typed_with(request);
            futures_util::pin_mut!(stream);
            while let Some(item) = stream.next().await {
                match item {
                    Ok(update) => {
                        metrics::counter!("settlement_updates_observed_total").increment(1);
                        if let Err(error) = store.save_checkpoint(update.offset()).await {
                            tracing::error!(%error, "cannot save the stream checkpoint");
                        }
                        doorbell.notify_one();
                    }
                    Err(error) => {
                        if let Some(earliest) = error.pruned_earliest_offset() {
                            tracing::warn!(
                                earliest,
                                "the resume point was pruned; moving to the ledger end"
                            );
                            if let Ok(end) = self.client.ledger_end().await {
                                let _ = store.save_checkpoint(end).await;
                            }
                        } else {
                            tracing::warn!(%error, "the update stream failed; reconnecting");
                        }
                        doorbell.notify_one();
                        break;
                    }
                }
            }
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    }
}
