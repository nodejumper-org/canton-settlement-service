//! What a settlement is, and the one decision the service exists to make:
//! given the allocations on the ledger, can this settlement settle now?
//!
//! The decision is a pure function of a settlement and a snapshot of the
//! allocations naming this executor, so it is tested without a ledger, and
//! the service re-makes it from scratch on every pass instead of keeping a
//! running tally that could drift from what the ledger holds.

use std::collections::{BTreeMap, BTreeSet};

use canton::daml::{Numeric, Party, TextMap};
use canton::token::types::metadata as md;
use canton::token::types::v2::allocation as al;
use serde::{Deserialize, Serialize};

/// One transfer in a settlement: `amount` of `instrument_id` from `sender` to
/// `receiver`. A delivery-versus-payment is two legs in opposite directions.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LegSpec {
    pub id: String,
    pub sender: Party,
    pub receiver: Party,
    pub instrument_id: String,
    pub amount: Numeric,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    /// Waiting for its allocations.
    Pending,
    /// Settled on the ledger, in one transaction.
    Settled,
    /// Its deadline passed first; any allocations were cancelled.
    Expired,
    /// Settling was refused too many times to keep trying.
    Failed,
}

impl Status {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Settled => "settled",
            Self::Expired => "expired",
            Self::Failed => "failed",
        }
    }

    #[must_use]
    pub fn parse(text: &str) -> Option<Self> {
        match text {
            "pending" => Some(Self::Pending),
            "settled" => Some(Self::Settled),
            "expired" => Some(Self::Expired),
            "failed" => Some(Self::Failed),
            _ => None,
        }
    }
}

/// A settlement the service was asked to execute.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Settlement {
    pub id: String,
    pub legs: Vec<LegSpec>,
    /// Unix seconds.
    pub created_at: i64,
    /// Unix seconds. Past this the service cancels instead of settling.
    pub settle_before: i64,
    pub status: Status,
    pub update_id: Option<String>,
    pub settled_offset: Option<i64>,
    /// Unix seconds.
    pub settled_at: Option<i64>,
    pub failures: u32,
    pub last_error: Option<String>,
}

/// Which half of a leg an allocation authorises.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Side {
    /// The sender reserving the holdings it will pay with.
    Sender,
    /// The receiver consenting to be paid.
    Receiver,
}

impl From<&al::TransferSide> for Side {
    fn from(side: &al::TransferSide) -> Self {
        match side {
            al::TransferSide::SenderSide => Self::Sender,
            al::TransferSide::ReceiverSide => Self::Receiver,
        }
    }
}

/// An allocation as read from the active contract set.
#[derive(Clone, Debug)]
pub struct AllocationRecord {
    pub contract_id: String,
    pub view: al::AllocationView,
}

/// What one party still has to allocate.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct MissingSide {
    pub leg_id: String,
    pub side: Side,
    pub party: Party,
}

/// Everything needed to submit the settlement.
#[derive(Clone, Debug)]
pub struct Ready {
    /// Taken from an allocation, so it is exactly what the allocations named.
    pub settlement: al::SettlementInfo,
    pub legs: Vec<al::TransferLeg>,
    pub allocation_ids: Vec<String>,
}

#[derive(Clone, Debug)]
pub enum Evaluation {
    Ready(Box<Ready>),
    Waiting(Vec<MissingSide>),
    /// An allocation names this settlement but not what was agreed. Settling
    /// would either fail at the registry or move something nobody asked for,
    /// so the service waits and reports it instead.
    Mismatch(Vec<String>),
}

/// A party id as Canton issues it, `<hint>::<fingerprint>`.
///
/// `Party::parse` checks only the character set and length, which a
/// misplaced hint alone passes. A settlement naming a party that cannot exist
/// would wait out its whole deadline before anyone noticed, so the shape is
/// checked here, where the id first arrives.
///
/// # Errors
/// The text is not a well-formed party id.
pub fn party_id(text: &str) -> Result<Party, String> {
    let party = Party::parse(text).map_err(|e| e.to_string())?;
    match text.split_once("::") {
        Some((hint, fingerprint))
            if !hint.is_empty()
                && !hint.contains(' ')
                && fingerprint.len() >= 8
                && fingerprint.bytes().all(|b| b.is_ascii_hexdigit()) =>
        {
            Ok(party)
        }
        _ => Err(format!(
            "{text:?} is not a party id of the form <hint>::<fingerprint>"
        )),
    }
}

fn owner(account: &canton::token::types::v2::holding::Account) -> Option<&Party> {
    account.owner.as_ref()
}

/// An allocation that can no longer be settled authorises nothing, however
/// well it matches: past its own settlement deadline, or past its expiry.
fn is_live(record: &AllocationRecord, now: i64) -> bool {
    let past = |at: Option<canton::daml::Timestamp>| at.is_some_and(|at| at.0 <= now);
    !past(record.view.allocation.settlement_deadline) && !past(record.view.expires_at)
}

/// Check one side of an allocation against the agreed leg. `Ok` is the
/// `(leg, side)` it authorises; `Err` says what disagrees.
fn check_side(
    record: &AllocationRecord,
    side: &al::TransferLegSide,
    specs: &BTreeMap<&str, &LegSpec>,
) -> Result<(String, Side), String> {
    let Some(spec) = specs.get(side.transfer_leg_id.as_str()) else {
        return Err(format!(
            "allocation {} names leg {:?}, which this settlement does not have",
            record.contract_id, side.transfer_leg_id
        ));
    };
    let kind = Side::from(&side.side);
    let (expected_authorizer, expected_otherside, name) = match kind {
        Side::Sender => (&spec.sender, &spec.receiver, "sender"),
        Side::Receiver => (&spec.receiver, &spec.sender, "receiver"),
    };
    let mut wrong = Vec::new();
    if owner(&record.view.allocation.authorizer) != Some(expected_authorizer) {
        wrong.push("authorizer");
    }
    if owner(&side.otherside) != Some(expected_otherside) {
        wrong.push("counterparty");
    }
    if side.amount != spec.amount {
        wrong.push("amount");
    }
    if side.instrument_id != spec.instrument_id {
        wrong.push("instrument");
    }
    if wrong.is_empty() {
        Ok((spec.id.clone(), kind))
    } else {
        Err(format!(
            "allocation {} disagrees with leg {:?} on its {name} side: {}",
            record.contract_id,
            spec.id,
            wrong.join(", ")
        ))
    }
}

/// The transfer legs of the batch. The sender's allocation states each leg's
/// accounts exactly as the registry will compare them, provider and account
/// id included, so the legs are taken from it rather than rebuilt.
fn batch_legs(
    settlement: &Settlement,
    covered: &BTreeMap<(String, Side), &AllocationRecord>,
) -> Vec<al::TransferLeg> {
    settlement
        .legs
        .iter()
        .filter_map(|spec| {
            let record = covered.get(&(spec.id.clone(), Side::Sender))?;
            let allocation = &record.view.allocation;
            let side = allocation
                .transfer_leg_sides
                .iter()
                .find(|s| s.transfer_leg_id == spec.id && Side::from(&s.side) == Side::Sender)?;
            Some(al::TransferLeg {
                transfer_leg_id: spec.id.clone(),
                sender: allocation.authorizer.clone(),
                receiver: side.otherside.clone(),
                amount: side.amount.clone(),
                instrument_id: side.instrument_id.clone(),
                meta: md::Metadata {
                    values: TextMap::new(),
                },
            })
        })
        .collect()
}

/// Decide whether `settlement` can settle on the strength of `allocations`,
/// which are the allocations naming this service as executor for this
/// settlement's id. `now` is Unix microseconds, the ledger's unit.
#[must_use]
pub fn evaluate(settlement: &Settlement, allocations: &[AllocationRecord], now: i64) -> Evaluation {
    let specs: BTreeMap<&str, &LegSpec> = settlement
        .legs
        .iter()
        .map(|leg| (leg.id.as_str(), leg))
        .collect();
    let mut problems = Vec::new();
    // (leg id, side) -> the allocation authorising it.
    let mut covered: BTreeMap<(String, Side), &AllocationRecord> = BTreeMap::new();
    let mut used: BTreeSet<&str> = BTreeSet::new();

    let mut ordered: Vec<&AllocationRecord> =
        allocations.iter().filter(|r| is_live(r, now)).collect();
    ordered.sort_by(|a, b| a.contract_id.cmp(&b.contract_id));

    for record in ordered {
        let checked: Vec<_> = record
            .view
            .allocation
            .transfer_leg_sides
            .iter()
            .map(|side| check_side(record, side, &specs))
            .collect();
        let (keys, errors): (Vec<_>, Vec<_>) = checked.into_iter().partition(Result::is_ok);
        if !errors.is_empty() {
            problems.extend(errors.into_iter().filter_map(Result::err));
            continue;
        }
        // A duplicate of sides already covered (a retried allocation, say) is
        // left out of the batch: settling it too would be settling twice.
        let fresh: Vec<_> = keys
            .into_iter()
            .filter_map(Result::ok)
            .filter(|key| !covered.contains_key(key))
            .collect();
        if !fresh.is_empty() {
            for key in fresh {
                covered.insert(key, record);
            }
            used.insert(record.contract_id.as_str());
        }
    }

    if !problems.is_empty() {
        return Evaluation::Mismatch(problems);
    }

    let missing: Vec<MissingSide> = settlement
        .legs
        .iter()
        .flat_map(|leg| {
            [(Side::Sender, &leg.sender), (Side::Receiver, &leg.receiver)]
                .into_iter()
                .filter(|(side, _)| !covered.contains_key(&(leg.id.clone(), *side)))
                .map(|(side, party)| MissingSide {
                    leg_id: leg.id.clone(),
                    side,
                    party: party.clone(),
                })
        })
        .collect();
    if !missing.is_empty() {
        return Evaluation::Waiting(missing);
    }

    let Some(first) = covered.values().next() else {
        return Evaluation::Waiting(Vec::new());
    };
    Evaluation::Ready(Box::new(Ready {
        settlement: first.view.settlement.clone(),
        legs: batch_legs(settlement, &covered),
        allocation_ids: used.into_iter().map(str::to_string).collect(),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use canton::daml::Timestamp;
    use canton::token::types::v2::holding::Account;

    fn party(name: &str) -> Party {
        Party::parse(format!("{name}::1220{}", "ab".repeat(32))).unwrap()
    }

    fn account(p: &Party) -> Account {
        Account {
            owner: Some(p.clone()),
            provider: None,
            id: String::new(),
        }
    }

    fn no_meta() -> md::Metadata {
        md::Metadata {
            values: TextMap::new(),
        }
    }

    fn dvp() -> Settlement {
        Settlement {
            id: "dvp-1".into(),
            legs: vec![
                LegSpec {
                    id: "leg-1".into(),
                    sender: party("alice"),
                    receiver: party("bob"),
                    instrument_id: "Amulet".into(),
                    amount: Numeric::parse("1.0").unwrap(),
                },
                LegSpec {
                    id: "leg-2".into(),
                    sender: party("bob"),
                    receiver: party("alice"),
                    instrument_id: "Amulet".into(),
                    amount: Numeric::parse("0.5").unwrap(),
                },
            ],
            created_at: 0,
            settle_before: 1_000,
            status: Status::Pending,
            update_id: None,
            settled_offset: None,
            settled_at: None,
            failures: 0,
            last_error: None,
        }
    }

    /// One trader's allocation: the sender side of the leg it pays and the
    /// receiver side of the leg it is paid by.
    fn allocation(
        cid: &str,
        trader: &Party,
        sides: &[(&str, al::TransferSide, &Party, &str)],
        deadline: Option<i64>,
    ) -> AllocationRecord {
        AllocationRecord {
            contract_id: cid.into(),
            view: al::AllocationView {
                original_allocation_cid: None,
                holding_cids: Vec::new(),
                created_at: Timestamp(0),
                num_iterations: canton::daml::Int64::from(0),
                expires_at: None,
                available_actions: canton::daml::GenMap::default(),
                meta: no_meta(),
                settlement: al::SettlementInfo {
                    executors: vec![party("executor")],
                    id: "dvp-1".into(),
                    cid: None,
                    meta: no_meta(),
                },
                allocation: al::AllocationSpecification {
                    admin: party("dso"),
                    authorizer: account(trader),
                    transfer_leg_sides: sides
                        .iter()
                        .map(|(leg, side, other, amount)| al::TransferLegSide {
                            transfer_leg_id: (*leg).into(),
                            side: *side,
                            otherside: account(other),
                            amount: Numeric::parse(*amount).unwrap(),
                            instrument_id: "Amulet".into(),
                            meta: no_meta(),
                        })
                        .collect(),
                    settlement_deadline: deadline.map(Timestamp),
                    next_iteration_funding: None,
                    committed: false,
                    meta: no_meta(),
                },
            },
        }
    }

    fn alice_allocates(cid: &str) -> AllocationRecord {
        let (alice, bob) = (party("alice"), party("bob"));
        allocation(
            cid,
            &alice,
            &[
                ("leg-1", al::TransferSide::SenderSide, &bob, "1.0"),
                ("leg-2", al::TransferSide::ReceiverSide, &bob, "0.5"),
            ],
            None,
        )
    }

    fn bob_allocates(cid: &str) -> AllocationRecord {
        let (alice, bob) = (party("alice"), party("bob"));
        allocation(
            cid,
            &bob,
            &[
                ("leg-2", al::TransferSide::SenderSide, &alice, "0.5"),
                ("leg-1", al::TransferSide::ReceiverSide, &alice, "1.0"),
            ],
            None,
        )
    }

    #[test]
    fn a_party_id_needs_a_hint_and_a_fingerprint() {
        assert!(party_id(&format!("alice::1220{}", "ab".repeat(32))).is_ok());
        assert!(party_id("not a party").is_err());
        assert!(party_id("alice").is_err());
        assert!(party_id("::1220abcd").is_err());
        assert!(party_id("alice::not-hex!").is_err());
    }

    #[test]
    fn nothing_allocated_waits_for_all_four_sides() {
        let Evaluation::Waiting(missing) = evaluate(&dvp(), &[], 0) else {
            panic!("expected waiting");
        };
        assert_eq!(missing.len(), 4);
    }

    #[test]
    fn one_trader_allocated_waits_for_the_other() {
        let Evaluation::Waiting(missing) = evaluate(&dvp(), &[alice_allocates("00a")], 0) else {
            panic!("expected waiting");
        };
        assert_eq!(
            missing,
            vec![
                MissingSide {
                    leg_id: "leg-1".into(),
                    side: Side::Receiver,
                    party: party("bob")
                },
                MissingSide {
                    leg_id: "leg-2".into(),
                    side: Side::Sender,
                    party: party("bob")
                },
            ]
        );
    }

    #[test]
    fn both_traders_allocated_is_ready_with_both_legs_and_both_allocations() {
        let allocations = [alice_allocates("00a"), bob_allocates("00b")];
        let Evaluation::Ready(ready) = evaluate(&dvp(), &allocations, 0) else {
            panic!("expected ready");
        };
        assert_eq!(
            ready.allocation_ids,
            vec!["00a".to_string(), "00b".to_string()]
        );
        assert_eq!(ready.legs.len(), 2);
        assert_eq!(ready.legs[0].transfer_leg_id, "leg-1");
        assert_eq!(ready.legs[0].sender.owner, Some(party("alice")));
        assert_eq!(ready.legs[0].receiver.owner, Some(party("bob")));
        assert_eq!(ready.settlement.id, "dvp-1");
    }

    #[test]
    fn a_wrong_amount_is_a_mismatch_not_a_settlement() {
        let (alice, bob) = (party("alice"), party("bob"));
        let short = allocation(
            "00a",
            &alice,
            &[
                ("leg-1", al::TransferSide::SenderSide, &bob, "0.9"),
                ("leg-2", al::TransferSide::ReceiverSide, &bob, "0.5"),
            ],
            None,
        );
        let Evaluation::Mismatch(problems) = evaluate(&dvp(), &[short, bob_allocates("00b")], 0)
        else {
            panic!("expected mismatch");
        };
        assert!(problems[0].contains("amount"), "{problems:?}");
    }

    #[test]
    fn a_stranger_cannot_authorise_someone_elses_side() {
        let (mallory, bob) = (party("mallory"), party("bob"));
        let forged = allocation(
            "00a",
            &mallory,
            &[("leg-1", al::TransferSide::SenderSide, &bob, "1.0")],
            None,
        );
        let Evaluation::Mismatch(problems) = evaluate(&dvp(), &[forged], 0) else {
            panic!("expected mismatch");
        };
        assert!(problems[0].contains("authorizer"), "{problems:?}");
    }

    #[test]
    fn an_allocation_past_its_own_deadline_authorises_nothing() {
        let (alice, bob) = (party("alice"), party("bob"));
        let stale = allocation(
            "00a",
            &alice,
            &[
                ("leg-1", al::TransferSide::SenderSide, &bob, "1.0"),
                ("leg-2", al::TransferSide::ReceiverSide, &bob, "0.5"),
            ],
            Some(10),
        );
        let Evaluation::Waiting(missing) = evaluate(&dvp(), &[stale, bob_allocates("00b")], 10)
        else {
            panic!("expected waiting");
        };
        assert_eq!(missing.len(), 2);
    }

    #[test]
    fn a_duplicate_allocation_is_left_out_of_the_batch() {
        let allocations = [
            alice_allocates("00a"),
            alice_allocates("00c"),
            bob_allocates("00b"),
        ];
        let Evaluation::Ready(ready) = evaluate(&dvp(), &allocations, 0) else {
            panic!("expected ready");
        };
        assert_eq!(
            ready.allocation_ids,
            vec!["00a".to_string(), "00b".to_string()]
        );
    }

    #[test]
    fn a_leg_the_settlement_does_not_have_is_a_mismatch() {
        let (alice, bob) = (party("alice"), party("bob"));
        let extra = allocation(
            "00a",
            &alice,
            &[("leg-9", al::TransferSide::SenderSide, &bob, "1.0")],
            None,
        );
        assert!(matches!(
            evaluate(&dvp(), &[extra], 0),
            Evaluation::Mismatch(_)
        ));
    }
}
