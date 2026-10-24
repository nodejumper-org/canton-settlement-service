//! The service's own state, in `SQLite`: the settlements it was asked to
//! execute, a history of what happened to each, and the update-stream offset
//! to resume from.
//!
//! The ledger stays the source of truth for allocations; nothing here is a
//! copy of ledger state that could go stale. What is stored is what the ledger
//! does not know — what was agreed off-ledger, and what this service did.

use std::path::Path;
use std::sync::{Arc, Mutex};

use rusqlite::{Connection, OptionalExtension as _, params};

use crate::model::{LegSpec, Settlement, Status};

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("database: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("stored legs do not decode: {0}")]
    Legs(#[from] serde_json::Error),
    #[error("a settlement with id {0:?} already exists")]
    Exists(String),
    #[error("the database task did not finish: {0}")]
    Task(String),
    #[error("the database lock was poisoned")]
    Poisoned,
}

/// One line of a settlement's history.
#[derive(Clone, Debug, serde::Serialize)]
pub struct HistoryEntry {
    /// Unix seconds.
    pub at: i64,
    pub kind: String,
    pub message: String,
}

#[derive(Clone)]
pub struct Store {
    connection: Arc<Mutex<Connection>>,
}

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS settlements (
    id             TEXT PRIMARY KEY,
    legs           TEXT NOT NULL,
    created_at     INTEGER NOT NULL,
    settle_before  INTEGER NOT NULL,
    status         TEXT NOT NULL,
    update_id      TEXT,
    settled_offset INTEGER,
    settled_at     INTEGER,
    failures       INTEGER NOT NULL DEFAULT 0,
    last_error     TEXT
);
CREATE INDEX IF NOT EXISTS settlements_by_status ON settlements (status, created_at);
CREATE TABLE IF NOT EXISTS history (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    settlement_id TEXT NOT NULL,
    at            INTEGER NOT NULL,
    kind          TEXT NOT NULL,
    message       TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS history_by_settlement ON history (settlement_id, id);
CREATE TABLE IF NOT EXISTS checkpoint (
    name   TEXT PRIMARY KEY,
    offset INTEGER NOT NULL
);
";

const COLUMNS: &str = "id, legs, created_at, settle_before, status, update_id, settled_offset, \
                       settled_at, failures, last_error";

fn row(row: &rusqlite::Row<'_>) -> rusqlite::Result<(Settlement, String)> {
    let status: String = row.get(4)?;
    Ok((
        Settlement {
            id: row.get(0)?,
            legs: Vec::new(),
            created_at: row.get(2)?,
            settle_before: row.get(3)?,
            status: Status::parse(&status).unwrap_or(Status::Failed),
            update_id: row.get(5)?,
            settled_offset: row.get(6)?,
            settled_at: row.get(7)?,
            failures: row.get(8)?,
            last_error: row.get(9)?,
        },
        row.get(1)?,
    ))
}

fn decode(rows: Vec<(Settlement, String)>) -> Result<Vec<Settlement>, StoreError> {
    rows.into_iter()
        .map(|(mut settlement, legs)| {
            settlement.legs = serde_json::from_str::<Vec<LegSpec>>(&legs)?;
            Ok(settlement)
        })
        .collect()
}

impl Store {
    /// Open (or create) the database at `path`.
    ///
    /// # Errors
    /// The file cannot be opened or the schema cannot be created.
    pub fn open(path: &Path) -> Result<Self, StoreError> {
        Self::from_connection(Connection::open(path)?)
    }

    /// An in-memory database, for tests.
    ///
    /// # Errors
    /// The schema cannot be created.
    #[cfg(test)]
    pub fn in_memory() -> Result<Self, StoreError> {
        Self::from_connection(Connection::open_in_memory()?)
    }

    fn from_connection(connection: Connection) -> Result<Self, StoreError> {
        connection.pragma_update(None, "journal_mode", "WAL")?;
        connection.pragma_update(None, "synchronous", "NORMAL")?;
        connection.execute_batch(SCHEMA)?;
        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
        })
    }

    /// Run `work` against the connection off the async runtime: `SQLite` calls
    /// block, and a blocked runtime thread stalls the update stream with it.
    async fn with<T, F>(&self, work: F) -> Result<T, StoreError>
    where
        T: Send + 'static,
        F: FnOnce(&Connection) -> Result<T, StoreError> + Send + 'static,
    {
        let connection = Arc::clone(&self.connection);
        tokio::task::spawn_blocking(move || {
            let guard = connection.lock().map_err(|_| StoreError::Poisoned)?;
            work(&guard)
        })
        .await
        .map_err(|e| StoreError::Task(e.to_string()))?
    }

    /// Record a new settlement.
    ///
    /// # Errors
    /// [`StoreError::Exists`] if the id is taken.
    pub async fn insert(&self, settlement: Settlement) -> Result<(), StoreError> {
        let legs = serde_json::to_string(&settlement.legs)?;
        self.with(move |db| {
            let inserted = db.execute(
                "INSERT OR IGNORE INTO settlements (id, legs, created_at, settle_before, status) \
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    settlement.id,
                    legs,
                    settlement.created_at,
                    settlement.settle_before,
                    settlement.status.as_str()
                ],
            )?;
            if inserted == 0 {
                return Err(StoreError::Exists(settlement.id));
            }
            db.execute(
                "INSERT INTO history (settlement_id, at, kind, message) VALUES (?1, ?2, 'created', ?3)",
                params![
                    settlement.id,
                    settlement.created_at,
                    format!("{} leg(s)", settlement.legs.len())
                ],
            )?;
            Ok(())
        })
        .await
    }

    /// # Errors
    /// The query fails.
    pub async fn get(&self, id: &str) -> Result<Option<Settlement>, StoreError> {
        let id = id.to_string();
        let found = self
            .with(move |db| {
                Ok(db
                    .query_row(
                        &format!("SELECT {COLUMNS} FROM settlements WHERE id = ?1"),
                        params![id],
                        row,
                    )
                    .optional()?)
            })
            .await?;
        Ok(decode(found.into_iter().collect())?.pop())
    }

    /// The most recent settlements, newest first, optionally of one status.
    ///
    /// # Errors
    /// The query fails.
    pub async fn list(
        &self,
        status: Option<Status>,
        limit: u32,
    ) -> Result<Vec<Settlement>, StoreError> {
        let rows = self
            .with(move |db| {
                let mut statement = db.prepare(&format!(
                    "SELECT {COLUMNS} FROM settlements \
                     WHERE (?1 IS NULL OR status = ?1) ORDER BY created_at DESC, id DESC LIMIT ?2"
                ))?;
                let rows = statement
                    .query_map(params![status.map(Status::as_str), limit], row)?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                Ok(rows)
            })
            .await?;
        decode(rows)
    }

    /// Every settlement still waiting for its allocations.
    ///
    /// # Errors
    /// The query fails.
    pub async fn pending(&self) -> Result<Vec<Settlement>, StoreError> {
        self.list(Some(Status::Pending), u32::MAX).await
    }

    /// How many settlements are in each status.
    ///
    /// # Errors
    /// The query fails.
    pub async fn counts(&self) -> Result<Vec<(Status, u64)>, StoreError> {
        self.with(|db| {
            let mut statement =
                db.prepare("SELECT status, COUNT(*) FROM settlements GROUP BY status")?;
            let rows = statement
                .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)))?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(rows
                .into_iter()
                .filter_map(|(status, n)| Some((Status::parse(&status)?, u64::try_from(n).ok()?)))
                .collect())
        })
        .await
    }

    /// Mark a settlement settled by `update_id` at `offset`.
    ///
    /// # Errors
    /// The update fails.
    pub async fn mark_settled(
        &self,
        id: &str,
        update_id: &str,
        offset: i64,
        at: i64,
    ) -> Result<(), StoreError> {
        let (id, update_id) = (id.to_string(), update_id.to_string());
        self.with(move |db| {
            db.execute(
                "UPDATE settlements SET status = 'settled', update_id = ?2, settled_offset = ?3, \
                 settled_at = ?4, last_error = NULL WHERE id = ?1",
                params![id, update_id, offset, at],
            )?;
            history(
                db,
                &id,
                at,
                "settled",
                &format!("update {update_id} at offset {offset}"),
            )
        })
        .await
    }

    /// Mark a settlement expired.
    ///
    /// # Errors
    /// The update fails.
    pub async fn mark_expired(&self, id: &str, at: i64, message: &str) -> Result<(), StoreError> {
        let (id, message) = (id.to_string(), message.to_string());
        self.with(move |db| {
            db.execute(
                "UPDATE settlements SET status = 'expired' WHERE id = ?1",
                params![id],
            )?;
            history(db, &id, at, "expired", &message)
        })
        .await
    }

    /// Record a failed attempt to settle; past `give_up_after` failures the
    /// settlement is marked failed. Returns the new failure count.
    ///
    /// # Errors
    /// The update fails.
    pub async fn record_failure(
        &self,
        id: &str,
        at: i64,
        error: &str,
        give_up_after: u32,
    ) -> Result<u32, StoreError> {
        let (id, error) = (id.to_string(), error.to_string());
        self.with(move |db| {
            db.execute(
                "UPDATE settlements SET failures = failures + 1, last_error = ?2, \
                 status = CASE WHEN failures + 1 >= ?3 THEN 'failed' ELSE status END WHERE id = ?1",
                params![id, error, give_up_after],
            )?;
            history(db, &id, at, "failure", &error)?;
            Ok(db.query_row(
                "SELECT failures FROM settlements WHERE id = ?1",
                params![id],
                |r| r.get(0),
            )?)
        })
        .await
    }

    /// Append a line to a settlement's history.
    ///
    /// # Errors
    /// The insert fails.
    pub async fn note(
        &self,
        id: &str,
        at: i64,
        kind: &str,
        message: &str,
    ) -> Result<(), StoreError> {
        let (id, kind, message) = (id.to_string(), kind.to_string(), message.to_string());
        self.with(move |db| history(db, &id, at, &kind, &message))
            .await
    }

    /// # Errors
    /// The query fails.
    pub async fn history(&self, id: &str) -> Result<Vec<HistoryEntry>, StoreError> {
        let id = id.to_string();
        self.with(move |db| {
            let mut statement = db.prepare(
                "SELECT at, kind, message FROM history WHERE settlement_id = ?1 ORDER BY id",
            )?;
            let rows = statement
                .query_map(params![id], |r| {
                    Ok(HistoryEntry {
                        at: r.get(0)?,
                        kind: r.get(1)?,
                        message: r.get(2)?,
                    })
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(rows)
        })
        .await
    }

    /// The offset the update stream last reached.
    ///
    /// # Errors
    /// The query fails.
    pub async fn checkpoint(&self) -> Result<Option<i64>, StoreError> {
        self.with(|db| {
            Ok(db
                .query_row(
                    "SELECT offset FROM checkpoint WHERE name = 'updates'",
                    [],
                    |r| r.get(0),
                )
                .optional()?)
        })
        .await
    }

    /// # Errors
    /// The update fails.
    pub async fn save_checkpoint(&self, offset: i64) -> Result<(), StoreError> {
        self.with(move |db| {
            db.execute(
                "INSERT INTO checkpoint (name, offset) VALUES ('updates', ?1) \
                 ON CONFLICT(name) DO UPDATE SET offset = excluded.offset",
                params![offset],
            )?;
            Ok(())
        })
        .await
    }
}

fn history(
    db: &Connection,
    id: &str,
    at: i64,
    kind: &str,
    message: &str,
) -> Result<(), StoreError> {
    db.execute(
        "INSERT INTO history (settlement_id, at, kind, message) VALUES (?1, ?2, ?3, ?4)",
        params![id, at, kind, message],
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use canton::daml::{Numeric, Party};

    fn settlement(id: &str, created_at: i64) -> Settlement {
        let party = |n: &str| Party::parse(format!("{n}::1220{}", "cd".repeat(32))).unwrap();
        Settlement {
            id: id.into(),
            legs: vec![LegSpec {
                id: "leg-1".into(),
                sender: party("alice"),
                receiver: party("bob"),
                instrument_id: "Amulet".into(),
                amount: Numeric::parse("1.5").unwrap(),
            }],
            created_at,
            settle_before: created_at + 60,
            status: Status::Pending,
            update_id: None,
            settled_offset: None,
            settled_at: None,
            failures: 0,
            last_error: None,
        }
    }

    #[tokio::test]
    async fn a_settlement_round_trips_and_its_id_is_unique() {
        let store = Store::in_memory().unwrap();
        store.insert(settlement("dvp-1", 10)).await.unwrap();
        assert!(matches!(
            store.insert(settlement("dvp-1", 11)).await,
            Err(StoreError::Exists(_))
        ));
        assert_eq!(
            store.get("dvp-1").await.unwrap(),
            Some(settlement("dvp-1", 10))
        );
        assert_eq!(store.get("nope").await.unwrap(), None);
    }

    #[tokio::test]
    async fn settling_moves_it_out_of_pending_and_into_the_history() {
        let store = Store::in_memory().unwrap();
        store.insert(settlement("dvp-1", 10)).await.unwrap();
        store.insert(settlement("dvp-2", 20)).await.unwrap();
        store.mark_settled("dvp-1", "upd-1", 42, 30).await.unwrap();

        let pending: Vec<_> = store
            .pending()
            .await
            .unwrap()
            .into_iter()
            .map(|s| s.id)
            .collect();
        assert_eq!(pending, vec!["dvp-2"]);
        let settled = store.get("dvp-1").await.unwrap().unwrap();
        assert_eq!(settled.status, Status::Settled);
        assert_eq!(settled.settled_offset, Some(42));
        let kinds: Vec<_> = store
            .history("dvp-1")
            .await
            .unwrap()
            .into_iter()
            .map(|h| h.kind)
            .collect();
        assert_eq!(kinds, vec!["created", "settled"]);
    }

    #[tokio::test]
    async fn repeated_failures_give_up_at_the_limit() {
        let store = Store::in_memory().unwrap();
        store.insert(settlement("dvp-1", 10)).await.unwrap();
        assert_eq!(
            store
                .record_failure("dvp-1", 11, "refused", 2)
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            store.get("dvp-1").await.unwrap().unwrap().status,
            Status::Pending
        );
        assert_eq!(
            store
                .record_failure("dvp-1", 12, "refused", 2)
                .await
                .unwrap(),
            2
        );
        assert_eq!(
            store.get("dvp-1").await.unwrap().unwrap().status,
            Status::Failed
        );
    }

    #[tokio::test]
    async fn the_checkpoint_is_overwritten_not_appended() {
        let store = Store::in_memory().unwrap();
        assert_eq!(store.checkpoint().await.unwrap(), None);
        store.save_checkpoint(5).await.unwrap();
        store.save_checkpoint(9).await.unwrap();
        assert_eq!(store.checkpoint().await.unwrap(), Some(9));
    }
}
