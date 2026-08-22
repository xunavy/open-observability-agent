use observability_core::{
    InvestigationCompletion, InvestigationCreateResult, InvestigationRepository, InvestigationRun,
    InvestigationStatus, InvestigationStepStatus, Observation, ObservationError,
    ObservationQueueItem, ObservationQueueRepository, ObservationQueueStats, ObservationRepository,
    QueueDisposition, TenantId, UsageEntry, UsageEvent, UsageLedger, UsageRepository,
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
             CREATE TABLE IF NOT EXISTS investigation_runs (
                 id TEXT PRIMARY KEY,
                 tenant_id TEXT NOT NULL,
                 idempotency_key TEXT NOT NULL,
                 status TEXT NOT NULL,
                 version INTEGER NOT NULL,
                 payload TEXT NOT NULL,
                 UNIQUE (tenant_id, idempotency_key)
             );
             CREATE TABLE IF NOT EXISTS investigation_steps (
                 id TEXT PRIMARY KEY,
                 run_id TEXT NOT NULL,
                 tenant_id TEXT NOT NULL,
                 status TEXT NOT NULL,
                 payload TEXT NOT NULL,
                 UNIQUE (run_id, id)
             );
             CREATE TABLE IF NOT EXISTS usage_events (
                 event_id TEXT PRIMARY KEY,
                 tenant_id TEXT NOT NULL,
                 occurred_at_ms INTEGER NOT NULL,
                 period TEXT NOT NULL,
                 kind TEXT NOT NULL,
                 quantity INTEGER NOT NULL,
                 source_type TEXT NOT NULL,
                 source_id TEXT NOT NULL,
                 UNIQUE (tenant_id, source_type, source_id, kind)
             );
             CREATE INDEX IF NOT EXISTS observation_queue_claim_idx ON observation_queue (state, available_at_ms, enqueued_at_ms);
             CREATE INDEX IF NOT EXISTS observation_queue_tenant_idx ON observation_queue (tenant_id, state, enqueued_at_ms);
             CREATE INDEX IF NOT EXISTS investigation_runs_tenant_idx ON investigation_runs (tenant_id, id);
             CREATE INDEX IF NOT EXISTS investigation_steps_run_idx ON investigation_steps (tenant_id, run_id);",
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

    fn get_many(
        &self,
        tenant_id: &TenantId,
        ids: &[uuid::Uuid],
    ) -> Result<Vec<Observation>, ObservationError> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| ObservationError::Storage("sqlite lock poisoned".into()))?;
        let mut statement = connection
            .prepare("SELECT payload FROM observations WHERE tenant_id = ?1")
            .map_err(|error| ObservationError::Storage(error.to_string()))?;
        let rows = statement
            .query_map(params![tenant_id.0.to_string()], |row| {
                row.get::<_, String>(0)
            })
            .map_err(|error| ObservationError::Storage(error.to_string()))?;
        let mut found = std::collections::BTreeMap::new();
        for row in rows {
            let payload = row.map_err(|error| ObservationError::Storage(error.to_string()))?;
            let observation: Observation = serde_json::from_str(&payload)
                .map_err(|error| ObservationError::Storage(error.to_string()))?;
            if ids.contains(&observation.id) {
                found.insert(observation.id, observation);
            }
        }
        Ok(ids
            .iter()
            .filter_map(|id| found.remove(id))
            .collect::<Vec<_>>())
    }
}

impl InvestigationRepository for SqliteStore {
    fn create_or_get(
        &self,
        idempotency_key: &str,
        run: &InvestigationRun,
    ) -> Result<InvestigationCreateResult, ObservationError> {
        let run_payload = serde_json::to_string(run)
            .map_err(|error| ObservationError::Storage(error.to_string()))?;
        let step = run
            .steps
            .first()
            .ok_or_else(|| ObservationError::Storage("investigation requires one step".into()))?;
        let step_payload = serde_json::to_string(step)
            .map_err(|error| ObservationError::Storage(error.to_string()))?;
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| ObservationError::Storage("sqlite lock poisoned".into()))?;
        let transaction = connection
            .transaction()
            .map_err(|error| ObservationError::Storage(error.to_string()))?;
        let inserted = transaction
            .execute(
                "INSERT INTO investigation_runs (id, tenant_id, idempotency_key, status, version, payload)
                 VALUES (?1, ?2, ?3, 'planned', ?4, ?5)
                 ON CONFLICT(tenant_id, idempotency_key) DO NOTHING",
                params![
                    run.id.to_string(),
                    run.tenant_id.0.to_string(),
                    idempotency_key,
                    run.version,
                    run_payload
                ],
            )
            .map_err(|error| ObservationError::Storage(error.to_string()))?;
        let result = if inserted == 1 {
            transaction
                .execute(
                    "INSERT INTO investigation_steps (id, run_id, tenant_id, status, payload)
                     VALUES (?1, ?2, ?3, 'planned', ?4)",
                    params![
                        step.id.to_string(),
                        run.id.to_string(),
                        run.tenant_id.0.to_string(),
                        step_payload
                    ],
                )
                .map_err(|error| ObservationError::Storage(error.to_string()))?;
            InvestigationCreateResult {
                run: run.clone(),
                created: true,
            }
        } else {
            let payload = transaction
                .query_row(
                    "SELECT payload FROM investigation_runs
                     WHERE tenant_id = ?1 AND idempotency_key = ?2",
                    params![run.tenant_id.0.to_string(), idempotency_key],
                    |row| row.get::<_, String>(0),
                )
                .map_err(|error| ObservationError::Storage(error.to_string()))?;
            InvestigationCreateResult {
                run: serde_json::from_str(&payload)
                    .map_err(|error| ObservationError::Storage(error.to_string()))?,
                created: false,
            }
        };
        transaction
            .commit()
            .map_err(|error| ObservationError::Storage(error.to_string()))?;
        Ok(result)
    }

    fn get(
        &self,
        tenant_id: &TenantId,
        id: &uuid::Uuid,
    ) -> Result<Option<InvestigationRun>, ObservationError> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| ObservationError::Storage("sqlite lock poisoned".into()))?;
        let payload = connection
            .query_row(
                "SELECT payload FROM investigation_runs WHERE id = ?1 AND tenant_id = ?2",
                params![id.to_string(), tenant_id.0.to_string()],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|error| ObservationError::Storage(error.to_string()))?;
        payload
            .map(|payload| {
                serde_json::from_str(&payload)
                    .map_err(|error| ObservationError::Storage(error.to_string()))
            })
            .transpose()
    }

    fn complete(
        &self,
        tenant_id: &TenantId,
        id: &uuid::Uuid,
        result_observation: &Observation,
        usage_event: &UsageEvent,
        now_ms: i64,
    ) -> Result<InvestigationCompletion, ObservationError> {
        if result_observation.tenant_id != *tenant_id || usage_event.tenant_id != *tenant_id {
            return Err(ObservationError::Storage(
                "investigation result tenant mismatch".into(),
            ));
        }
        if usage_event.source_id != *id || usage_event.source_type != "Investigation" {
            return Err(ObservationError::Storage(
                "usage event source must be the investigation".into(),
            ));
        }
        result_observation.validate()?;
        let result_payload = serde_json::to_string(result_observation)
            .map_err(|error| ObservationError::Storage(error.to_string()))?;
        let usage_kind = serde_json::to_value(&usage_event.kind)
            .map_err(|error| ObservationError::Storage(error.to_string()))?
            .as_str()
            .ok_or_else(|| ObservationError::Storage("invalid usage event kind".into()))?
            .to_owned();

        let mut connection = self
            .connection
            .lock()
            .map_err(|_| ObservationError::Storage("sqlite lock poisoned".into()))?;
        let transaction = connection
            .transaction()
            .map_err(|error| ObservationError::Storage(error.to_string()))?;
        let payload = transaction
            .query_row(
                "SELECT payload FROM investigation_runs WHERE id = ?1 AND tenant_id = ?2",
                params![id.to_string(), tenant_id.0.to_string()],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|error| ObservationError::Storage(error.to_string()))?
            .ok_or_else(|| ObservationError::Storage("investigation was not found".into()))?;
        let mut run: InvestigationRun = serde_json::from_str(&payload)
            .map_err(|error| ObservationError::Storage(error.to_string()))?;
        if run.status == InvestigationStatus::Completed {
            transaction
                .commit()
                .map_err(|error| ObservationError::Storage(error.to_string()))?;
            return Ok(InvestigationCompletion {
                run,
                completed_now: false,
            });
        }
        if run.status != InvestigationStatus::Planned {
            return Err(ObservationError::Storage(
                "investigation is not executable".into(),
            ));
        }

        run.status = InvestigationStatus::Running;
        run.updated_at_ms = now_ms;
        run.version = run.version.saturating_add(1);
        run.steps
            .first_mut()
            .ok_or_else(|| ObservationError::Storage("investigation step is missing".into()))?
            .status = InvestigationStepStatus::Running;

        transaction
            .execute(
                "INSERT OR REPLACE INTO observations (id, tenant_id, started_at_ms, payload)
                 VALUES (?1, ?2, ?3, ?4)",
                params![
                    result_observation.id.to_string(),
                    tenant_id.0.to_string(),
                    result_observation.started_at_ms,
                    result_payload
                ],
            )
            .map_err(|error| ObservationError::Storage(error.to_string()))?;
        let usage_inserted = transaction
            .execute(
                "INSERT INTO usage_events (event_id, tenant_id, occurred_at_ms, period, kind, quantity, source_type, source_id)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                 ON CONFLICT(tenant_id, source_type, source_id, kind) DO NOTHING",
                params![
                    usage_event.event_id.to_string(),
                    tenant_id.0.to_string(),
                    usage_event.occurred_at_ms,
                    usage_event.period,
                    usage_kind,
                    usage_event.quantity,
                    usage_event.source_type,
                    usage_event.source_id.to_string()
                ],
            )
            .map_err(|error| ObservationError::Storage(error.to_string()))?;
        if usage_inserted == 1 {
            transaction
                .execute(
                    "INSERT INTO usage_entries (tenant_id, period, kind, quantity)
                     VALUES (?1, ?2, ?3, ?4)",
                    params![
                        tenant_id.0.to_string(),
                        usage_event.period,
                        usage_kind,
                        usage_event.quantity
                    ],
                )
                .map_err(|error| ObservationError::Storage(error.to_string()))?;
        }

        run.status = InvestigationStatus::Completed;
        run.result_observation_id = Some(result_observation.id);
        run.updated_at_ms = now_ms;
        run.version = run.version.saturating_add(1);
        let step = run
            .steps
            .first_mut()
            .ok_or_else(|| ObservationError::Storage("investigation step is missing".into()))?;
        step.status = InvestigationStepStatus::Completed;
        step.output_observation_id = Some(result_observation.id);
        let step_payload = serde_json::to_string(step)
            .map_err(|error| ObservationError::Storage(error.to_string()))?;
        let run_payload = serde_json::to_string(&run)
            .map_err(|error| ObservationError::Storage(error.to_string()))?;
        transaction
            .execute(
                "UPDATE investigation_runs SET status = 'completed', version = ?3, payload = ?4
                 WHERE id = ?1 AND tenant_id = ?2",
                params![
                    id.to_string(),
                    tenant_id.0.to_string(),
                    run.version,
                    run_payload
                ],
            )
            .map_err(|error| ObservationError::Storage(error.to_string()))?;
        transaction
            .execute(
                "UPDATE investigation_steps SET status = 'completed', payload = ?3
                 WHERE run_id = ?1 AND tenant_id = ?2",
                params![id.to_string(), tenant_id.0.to_string(), step_payload],
            )
            .map_err(|error| ObservationError::Storage(error.to_string()))?;
        transaction
            .commit()
            .map_err(|error| ObservationError::Storage(error.to_string()))?;
        Ok(InvestigationCompletion {
            run,
            completed_now: true,
        })
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
        ApprovalPolicy, InvestigationRepository, InvestigationStep, InvestigationStepStatus,
        ObservationKind, ObservationQueueRepository, ObservationStatus, QueueDisposition, SafeTool,
        TenantId, UsageEvent,
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

        assert_eq!(
            store
                .enqueue_batch(std::slice::from_ref(&observation), 100)
                .unwrap(),
            1
        );
        assert_eq!(
            store
                .enqueue_batch(std::slice::from_ref(&observation), 100)
                .unwrap(),
            0
        );
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
        ObservationQueueRepository::complete(&store, &replay.id).unwrap();
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

    #[test]
    fn investigation_completion_is_idempotent_and_transactionally_metered() {
        let store = SqliteStore::open(":memory:").unwrap();
        let tenant = TenantId(Uuid::new_v4());
        let evidence = Observation {
            id: Uuid::new_v4(),
            tenant_id: tenant.clone(),
            trace_id: "trace".into(),
            span_id: "span".into(),
            kind: ObservationKind::Agent,
            name: "agent.run".into(),
            status: ObservationStatus::Error,
            started_at_ms: 1,
            duration_ms: 2,
            attributes: BTreeMap::new(),
        };
        ObservationRepository::append(&store, &evidence).unwrap();
        assert_eq!(
            ObservationRepository::get_many(&store, &tenant, &[evidence.id, Uuid::new_v4()])
                .unwrap(),
            vec![evidence.clone()]
        );

        let run = InvestigationRun {
            id: Uuid::new_v4(),
            tenant_id: tenant.clone(),
            objective: "解释失败".into(),
            status: InvestigationStatus::Planned,
            evidence_ids: vec![evidence.id],
            steps: vec![InvestigationStep {
                id: Uuid::new_v4(),
                tool: SafeTool::InspectFailureContext,
                approval_policy: ApprovalPolicy::NotRequired,
                status: InvestigationStepStatus::Planned,
                input_hash: "input".into(),
                output_observation_id: None,
                error: None,
            }],
            result_observation_id: None,
            error: None,
            created_at_ms: 10,
            updated_at_ms: 10,
            version: 1,
        };
        let created = InvestigationRepository::create_or_get(&store, "same-key", &run).unwrap();
        assert!(created.created);
        let duplicate = InvestigationRepository::create_or_get(&store, "same-key", &run).unwrap();
        assert!(!duplicate.created);
        assert_eq!(duplicate.run.id, run.id);

        let result = Observation {
            id: Uuid::new_v4(),
            tenant_id: tenant.clone(),
            trace_id: run.id.to_string(),
            span_id: run.steps[0].id.to_string(),
            kind: ObservationKind::Tool,
            name: "investigation.inspect_failure_context".into(),
            status: ObservationStatus::Ok,
            started_at_ms: 20,
            duration_ms: 1,
            attributes: BTreeMap::new(),
        };
        let usage_event = UsageEvent {
            event_id: Uuid::new_v4(),
            tenant_id: tenant.clone(),
            occurred_at_ms: 20,
            period: "2026-08".into(),
            kind: observability_core::UsageKind::AgentRun,
            quantity: 1,
            source_type: "Investigation".into(),
            source_id: run.id,
        };
        let completed =
            InvestigationRepository::complete(&store, &tenant, &run.id, &result, &usage_event, 21)
                .unwrap();
        assert!(completed.completed_now);
        assert_eq!(completed.run.status, InvestigationStatus::Completed);
        let repeated =
            InvestigationRepository::complete(&store, &tenant, &run.id, &result, &usage_event, 22)
                .unwrap();
        assert!(!repeated.completed_now);
        assert_eq!(
            UsageRepository::load(&store).unwrap().total(
                &tenant,
                "2026-08",
                &observability_core::UsageKind::AgentRun
            ),
            1
        );
        assert_eq!(
            InvestigationRepository::get(&store, &tenant, &run.id)
                .unwrap()
                .unwrap()
                .result_observation_id,
            Some(result.id)
        );
        assert!(
            InvestigationRepository::get(&store, &TenantId(Uuid::new_v4()), &run.id)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn invalid_investigation_result_does_not_complete_or_meter() {
        let store = SqliteStore::open(":memory:").unwrap();
        let tenant = TenantId(Uuid::new_v4());
        let run = InvestigationRun {
            id: Uuid::new_v4(),
            tenant_id: tenant.clone(),
            objective: "解释失败".into(),
            status: InvestigationStatus::Planned,
            evidence_ids: Vec::new(),
            steps: vec![InvestigationStep {
                id: Uuid::new_v4(),
                tool: SafeTool::InspectFailureContext,
                approval_policy: ApprovalPolicy::NotRequired,
                status: InvestigationStepStatus::Planned,
                input_hash: "input".into(),
                output_observation_id: None,
                error: None,
            }],
            result_observation_id: None,
            error: None,
            created_at_ms: 10,
            updated_at_ms: 10,
            version: 1,
        };
        InvestigationRepository::create_or_get(&store, "rollback", &run).unwrap();
        let invalid_result = Observation {
            id: Uuid::new_v4(),
            tenant_id: tenant.clone(),
            trace_id: run.id.to_string(),
            span_id: run.steps[0].id.to_string(),
            kind: ObservationKind::Tool,
            name: String::new(),
            status: ObservationStatus::Ok,
            started_at_ms: 20,
            duration_ms: 1,
            attributes: BTreeMap::new(),
        };
        let usage_event = UsageEvent {
            event_id: Uuid::new_v4(),
            tenant_id: tenant.clone(),
            occurred_at_ms: 20,
            period: "2026-08".into(),
            kind: observability_core::UsageKind::AgentRun,
            quantity: 1,
            source_type: "Investigation".into(),
            source_id: run.id,
        };
        assert!(InvestigationRepository::complete(
            &store,
            &tenant,
            &run.id,
            &invalid_result,
            &usage_event,
            21,
        )
        .is_err());
        assert_eq!(
            InvestigationRepository::get(&store, &tenant, &run.id)
                .unwrap()
                .unwrap()
                .status,
            InvestigationStatus::Planned
        );
        assert_eq!(
            UsageRepository::load(&store).unwrap().total(
                &tenant,
                "2026-08",
                &observability_core::UsageKind::AgentRun
            ),
            0
        );
    }
}
