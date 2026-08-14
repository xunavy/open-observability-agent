use observability_core::{
    Observation, ObservationError, ObservationRepository, UsageEntry, UsageLedger, UsageRepository,
};
use rusqlite::{params, Connection};
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
             CREATE TABLE IF NOT EXISTS usage_entries (id INTEGER PRIMARY KEY AUTOINCREMENT, tenant_id TEXT NOT NULL, period TEXT NOT NULL, kind TEXT NOT NULL, quantity INTEGER NOT NULL);",
        ).map_err(|e| ObservationError::Storage(e.to_string()))?;
        Ok(Self {
            connection: Mutex::new(connection),
        })
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
    use observability_core::{ObservationKind, ObservationStatus, TenantId};
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
}
