use observability_core::{
    Observation, ObservationError, ObservationQueueItem, ObservationQueueRepository,
    ObservationQueueStats, ObservationRepository, QueueDisposition, TenantId, UsageEntry,
    UsageLedger, UsageRepository,
};
use rusqlite::{params, Connection, OptionalExtension};
use std::path::Path;
use std::sync::Mutex;

pub struct SqliteStore {
    connection: Mutex<Connection>,
}

impl SqliteStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, ObservationError> {
        let connection =
            Connection::open(path).map_err(|e| ObservationError::Storage(e.to_string()))?;
        connection.execute_batch(
            "CREATE TABLE IF NOT EXISTS observations (id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, started_at_ms INTEGER NOT NULL, payload TEXT NOT NULL);
             CREATE TABLE IF NOT EXISTS usage_entries (id INTEGER PRIMARY KEY AUTOINCREMENT, tenant_id TEXT NOT NULL, period TEXT NOT NULL, kind TEXT NOT NULL, quantity INTEGER NOT NULL);
             CREATE TABLE IF NOT EXISTS observation_queue (
                 id TEXT PRIMARY KEY,
                 tenant_id TEXT NOT NULL,
                 state TEXT NOT NULL CHECK (state IN ('pending', 'processing', 'dead_letter')),
                 attempts INTEGER NOT NULL DEFAULT 0,
                 enqueued_at_ms INTEGER NOT NULL,
                 available_at_ms INTEGER NOT NULL,
                 lease_until_ms INTEGER,
                 last_error TEXT,
                 payload TEXT NOT NULL
             );
             CREATE INDEX IF NOT EXISTS observation_queue_claim_idx ON observation_queue (state, available_at_ms, enqueued_at_ms);
             CREATE INDEX IF NOT EXISTS observation_queue_tenant_idx ON observation_queue (tenant_id, state, enqueued_at_ms);",
        ).map_err(|e| ObservationError::Storage(e.to_string()))?;
        Ok(Self {
            connection: Mutex::new(connection),
        })
    }
}

impl ObservationQueueRepository for SqliteStore {
    fn enqueue_batch(
        &self,
        observations: &[Observation],
        now_ms: i64,
    ) -> Result<usize, ObservationError> {
        let payloads = observations
            .iter()
            .map(|observation| {
                observation.validate()?;
                serde_json::to_string(observation)
                    .map(|payload| (observation, payload))
                    .map_err(|error| ObservationError::Storage(error.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| ObservationError::Storage("sqlite lock poisoned".into()))?;
        let transaction = connection
            .transaction()
            .map_err(|error| ObservationError::Storage(error.to_string()))?;
        let mut inserted = 0;
        for (observation, payload) in payloads {
            inserted += transaction
                .execute(
                    "INSERT INTO observation_queue (id, tenant_id, state, attempts, enqueued_at_ms, available_at_ms, payload)
                     VALUES (?1, ?2, 'pending', 0, ?3, ?3, ?4)
                     ON CONFLICT(id) DO NOTHING",
                    params![
                        observation.id.to_string(),
                        observation.tenant_id.0.to_string(),
                        now_ms,
                        payload
                    ],
                )
                .map_err(|error| ObservationError::Storage(error.to_string()))?;
        }
        transaction
            .commit()
            .map_err(|error| ObservationError::Storage(error.to_string()))?;
        Ok(inserted)
    }

    fn claim_next(
        &self,
        now_ms: i64,
        lease_until_ms: i64,
    ) -> Result<Option<ObservationQueueItem>, ObservationError> {
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| ObservationError::Storage("sqlite lock poisoned".into()))?;
        let transaction = connection
            .transaction()
            .map_err(|error| ObservationError::Storage(error.to_string()))?;
        transaction
            .execute(
                "UPDATE observation_queue
                 SET state = 'pending', lease_until_ms = NULL
                 WHERE state = 'processing' AND lease_until_ms <= ?1",
                params![now_ms],
            )
            .map_err(|error| ObservationError::Storage(error.to_string()))?;
        let claimed = transaction
            .query_row(
                "SELECT id, payload, attempts, available_at_ms, last_error
                 FROM observation_queue
                 WHERE state = 'pending' AND available_at_ms <= ?1
                 ORDER BY available_at_ms ASC, enqueued_at_ms ASC
                 LIMIT 1",
                params![now_ms],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, u32>(2)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, Option<String>>(4)?,
                    ))
                },
            )
            .optional()
            .map_err(|error| ObservationError::Storage(error.to_string()))?;
        let Some((id, payload, attempts, available_at_ms, last_error)) = claimed else {
            transaction
                .commit()
                .map_err(|error| ObservationError::Storage(error.to_string()))?;
            return Ok(None);
        };
        let changed = transaction
            .execute(
                "UPDATE observation_queue
                 SET state = 'processing', attempts = attempts + 1, lease_until_ms = ?2
                 WHERE id = ?1 AND state = 'pending'",
                params![id, lease_until_ms],
            )
            .map_err(|error| ObservationError::Storage(error.to_string()))?;
        if changed != 1 {
            return Err(ObservationError::Storage(
                "queue item could not be claimed".into(),
            ));
        }
        transaction
            .commit()
            .map_err(|error| ObservationError::Storage(error.to_string()))?;
        let observation: Observation = serde_json::from_str(&payload)
            .map_err(|error| ObservationError::Storage(error.to_string()))?;
        let id = id
            .parse()
            .map_err(|error: uuid::Error| ObservationError::Storage(error.to_string()))?;
        Ok(Some(ObservationQueueItem {
            id,
            observation,
            attempts: attempts + 1,
            available_at_ms,
            last_error,
        }))
    }

    fn complete(&self, id: &uuid::Uuid) -> Result<(), ObservationError> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| ObservationError::Storage("sqlite lock poisoned".into()))?;
        connection
            .execute(
                "DELETE FROM observation_queue WHERE id = ?1 AND state = 'processing'",
                params![id.to_string()],
            )
            .map_err(|error| ObservationError::Storage(error.to_string()))?;
        Ok(())
    }

    fn fail(
        &self,
        id: &uuid::Uuid,
        error: &str,
        retry_at_ms: i64,
        max_attempts: u32,
    ) -> Result<QueueDisposition, ObservationError> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| ObservationError::Storage("sqlite lock poisoned".into()))?;
        let attempts = connection
            .query_row(
                "SELECT attempts FROM observation_queue WHERE id = ?1 AND state = 'processing'",
                params![id.to_string()],
                |row| row.get::<_, u32>(0),
            )
            .optional()
            .map_err(|error| ObservationError::Storage(error.to_string()))?
            .ok_or_else(|| ObservationError::Storage("queue item is not processing".into()))?;
        let disposition = if attempts >= max_attempts.max(1) {
            QueueDisposition::DeadLetter
        } else {
            QueueDisposition::Pending
        };
        let state = match disposition {
            QueueDisposition::Pending => "pending",
            QueueDisposition::DeadLetter => "dead_letter",
        };
        connection
            .execute(
                "UPDATE observation_queue
                 SET state = ?2, available_at_ms = ?3, lease_until_ms = NULL, last_error = ?4
                 WHERE id = ?1 AND state = 'processing'",
                params![id.to_string(), state, retry_at_ms, error],
            )
            .map_err(|error| ObservationError::Storage(error.to_string()))?;
        Ok(disposition)
    }

    fn stats(&self, tenant_id: &TenantId) -> Result<ObservationQueueStats, ObservationError> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| ObservationError::Storage("sqlite lock poisoned".into()))?;
        let mut statement = connection
            .prepare(
                "SELECT state, COUNT(*) FROM observation_queue
                 WHERE tenant_id = ?1 GROUP BY state",
            )
            .map_err(|error| ObservationError::Storage(error.to_string()))?;
        let rows = statement
            .query_map(params![tenant_id.0.to_string()], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, u64>(1)?))
            })
            .map_err(|error| ObservationError::Storage(error.to_string()))?;
        let mut stats = ObservationQueueStats::default();
        for row in rows {
            let (state, count) =
                row.map_err(|error| ObservationError::Storage(error.to_string()))?;
            match state.as_str() {
                "pending" => stats.pending = count,
                "processing" => stats.processing = count,
                "dead_letter" => stats.dead_letter = count,
                _ => {}
            }
        }
        Ok(stats)
    }

    fn dead_letters(
        &self,
        tenant_id: &TenantId,
        limit: usize,
    ) -> Result<Vec<ObservationQueueItem>, ObservationError> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| ObservationError::Storage("sqlite lock poisoned".into()))?;
        let mut statement = connection
            .prepare(
                "SELECT id, payload, attempts, available_at_ms, last_error
                 FROM observation_queue
                 WHERE tenant_id = ?1 AND state = 'dead_letter'
                 ORDER BY enqueued_at_ms ASC LIMIT ?2",
            )
            .map_err(|error| ObservationError::Storage(error.to_string()))?;
        let rows = statement
            .query_map(
                params![tenant_id.0.to_string(), limit.clamp(1, 1_000) as i64],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, u32>(2)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, Option<String>>(4)?,
                    ))
                },
            )
            .map_err(|error| ObservationError::Storage(error.to_string()))?;
        rows.map(|row| {
            let (id, payload, attempts, available_at_ms, last_error) =
                row.map_err(|error| ObservationError::Storage(error.to_string()))?;
            Ok(ObservationQueueItem {
                id: id
                    .parse()
                    .map_err(|error: uuid::Error| ObservationError::Storage(error.to_string()))?,
                observation: serde_json::from_str(&payload)
                    .map_err(|error| ObservationError::Storage(error.to_string()))?,
                attempts,
                available_at_ms,
                last_error,
            })
        })
        .collect()
    }

    fn requeue_dead_letter(
        &self,
        tenant_id: &TenantId,
        id: &uuid::Uuid,
        now_ms: i64,
    ) -> Result<bool, ObservationError> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| ObservationError::Storage("sqlite lock poisoned".into()))?;
        let changed = connection
            .execute(
                "UPDATE observation_queue
                 SET state = 'pending', attempts = 0, enqueued_at_ms = ?3,
                     available_at_ms = ?3, lease_until_ms = NULL, last_error = NULL
                 WHERE id = ?1 AND tenant_id = ?2 AND state = 'dead_letter'",
                params![id.to_string(), tenant_id.0.to_string(), now_ms],
            )
            .map_err(|error| ObservationError::Storage(error.to_string()))?;
        Ok(changed == 1)
    }
}

impl ObservationRepository for SqliteStore {
    fn append(&self, observation: &Observation) -> Result<(), ObservationError> {
        observation.validate()?;
        let payload = serde_json::to_string(observation)
            .map_err(|e| ObservationError::Storage(e.to_string()))?;
        let connection = self
            .connection
            .lock()
            .map_err(|_| ObservationError::Storage("sqlite lock poisoned".into()))?;
        connection.execute(
            "INSERT OR REPLACE INTO observations (id, tenant_id, started_at_ms, payload) VALUES (?1, ?2, ?3, ?4)",
            params![observation.id.to_string(), observation.tenant_id.0.to_string(), observation.started_at_ms, payload],
        ).map_err(|e| ObservationError::Storage(e.to_string()))?;
        Ok(())
    }

    fn list(&self) -> Result<Vec<Observation>, ObservationError> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| ObservationError::Storage("sqlite lock poisoned".into()))?;
        let mut statement = connection
            .prepare("SELECT payload FROM observations ORDER BY started_at_ms ASC")
            .map_err(|e| ObservationError::Storage(e.to_string()))?;
        let rows = statement
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(|e| ObservationError::Storage(e.to_string()))?;
        rows.map(|row| {
            let payload = row.map_err(|e| ObservationError::Storage(e.to_string()))?;
            serde_json::from_str(&payload).map_err(|e| ObservationError::Storage(e.to_string()))
        })
        .collect()
    }
}

impl UsageRepository for SqliteStore {
    fn append(&self, entry: &UsageEntry) -> Result<(), ObservationError> {
        let payload =
            serde_json::to_value(entry).map_err(|e| ObservationError::Storage(e.to_string()))?;
        let tenant = payload["tenant_id"]
            .as_str()
            .ok_or_else(|| ObservationError::Storage("invalid tenant id".into()))?;
        let kind = payload["kind"]
            .as_str()
            .ok_or_else(|| ObservationError::Storage("invalid usage kind".into()))?;
        let connection = self
            .connection
            .lock()
            .map_err(|_| ObservationError::Storage("sqlite lock poisoned".into()))?;
        connection.execute("INSERT INTO usage_entries (tenant_id, period, kind, quantity) VALUES (?1, ?2, ?3, ?4)", params![tenant, entry.period, kind, entry.quantity]).map_err(|e| ObservationError::Storage(e.to_string()))?;
        Ok(())
    }

    fn load(&self) -> Result<UsageLedger, ObservationError> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| ObservationError::Storage("sqlite lock poisoned".into()))?;
        let mut statement = connection
            .prepare("SELECT tenant_id, period, kind, quantity FROM usage_entries")
            .map_err(|e| ObservationError::Storage(e.to_string()))?;
        let rows = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, u64>(3)?,
                ))
            })
            .map_err(|e| ObservationError::Storage(e.to_string()))?;
        let mut ledger = UsageLedger::default();
        for row in rows {
            let (tenant, period, kind, quantity) =
                row.map_err(|e| ObservationError::Storage(e.to_string()))?;
            let tenant_id = observability_core::TenantId(
                tenant
                    .parse()
                    .map_err(|e: uuid::Error| ObservationError::Storage(e.to_string()))?,
            );
            let kind = serde_json::from_value(serde_json::Value::String(kind))
                .map_err(|e| ObservationError::Storage(e.to_string()))?;
            ledger.record(UsageEntry {
                tenant_id,
                period,
                kind,
                quantity,
            });
        }
        Ok(ledger)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use observability_core::{
        ObservationKind, ObservationQueueRepository, ObservationStatus, QueueDisposition, TenantId,
    };
    use std::collections::BTreeMap;
    use uuid::Uuid;

    #[test]
    fn sqlite_round_trips_observation_and_usage() {
        let store = SqliteStore::open(":memory:").unwrap();
        let tenant = TenantId(Uuid::new_v4());
        let observation = Observation {
            id: Uuid::new_v4(),
            tenant_id: tenant.clone(),
            trace_id: "trace".into(),
            span_id: "span".into(),
            kind: ObservationKind::Agent,
            name: "agent.run".into(),
            status: ObservationStatus::Ok,
            started_at_ms: 1,
            duration_ms: 2,
            attributes: BTreeMap::new(),
        };
        ObservationRepository::append(&store, &observation).unwrap();
        assert_eq!(
            ObservationRepository::list(&store).unwrap(),
            vec![observation]
        );
        UsageRepository::append(
            &store,
            &UsageEntry {
                tenant_id: tenant.clone(),
                period: "2026-08".into(),
                kind: observability_core::UsageKind::Observation,
                quantity: 4,
            },
        )
        .unwrap();
        assert_eq!(
            UsageRepository::load(&store).unwrap().total(
                &tenant,
                "2026-08",
                &observability_core::UsageKind::Observation
            ),
            4
        );
    }

    #[test]
    fn durable_queue_retries_dead_letters_and_replays() {
        let store = SqliteStore::open(":memory:").unwrap();
        let tenant = TenantId(Uuid::new_v4());
        let observation = Observation {
            id: Uuid::new_v4(),
            tenant_id: tenant.clone(),
            trace_id: "trace".into(),
            span_id: "span".into(),
            kind: ObservationKind::Agent,
            name: "agent.run".into(),
            status: ObservationStatus::Ok,
            started_at_ms: 1,
            duration_ms: 2,
            attributes: BTreeMap::new(),
        };

        assert_eq!(store.enqueue_batch(&[observation.clone()], 100).unwrap(), 1);
        assert_eq!(store.enqueue_batch(&[observation.clone()], 100).unwrap(), 0);
        assert_eq!(store.stats(&tenant).unwrap().pending, 1);

        let first = store.claim_next(100, 200).unwrap().unwrap();
        assert_eq!(first.attempts, 1);
        assert_eq!(
            store.fail(&first.id, "temporary", 150, 2).unwrap(),
            QueueDisposition::Pending
        );
        assert!(store.claim_next(149, 249).unwrap().is_none());

        let second = store.claim_next(150, 250).unwrap().unwrap();
        assert_eq!(second.attempts, 2);
        assert_eq!(
            store.fail(&second.id, "permanent", 300, 2).unwrap(),
            QueueDisposition::DeadLetter
        );
        assert_eq!(store.stats(&tenant).unwrap().dead_letter, 1);
        let dead_letters = store.dead_letters(&tenant, 10).unwrap();
        assert_eq!(dead_letters[0].last_error.as_deref(), Some("permanent"));

        assert!(store
            .requeue_dead_letter(&tenant, &observation.id, 400)
            .unwrap());
        let replay = store.claim_next(400, 500).unwrap().unwrap();
        assert_eq!(replay.attempts, 1);
        store.complete(&replay.id).unwrap();
        assert_eq!(
            store.stats(&tenant).unwrap(),
            ObservationQueueStats::default()
        );
    }

    #[test]
    fn expired_queue_lease_is_recovered() {
        let store = SqliteStore::open(":memory:").unwrap();
        let observation = Observation {
            id: Uuid::new_v4(),
            tenant_id: TenantId(Uuid::new_v4()),
            trace_id: "trace".into(),
            span_id: "span".into(),
            kind: ObservationKind::Http,
            name: "http.request".into(),
            status: ObservationStatus::Ok,
            started_at_ms: 1,
            duration_ms: 2,
            attributes: BTreeMap::new(),
        };
        store.enqueue_batch(&[observation], 100).unwrap();
        assert_eq!(store.claim_next(100, 200).unwrap().unwrap().attempts, 1);
        assert!(store.claim_next(199, 300).unwrap().is_none());
        assert_eq!(store.claim_next(200, 300).unwrap().unwrap().attempts, 2);
    }
}
