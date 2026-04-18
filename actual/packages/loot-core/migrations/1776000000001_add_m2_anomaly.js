export default async function runMigration(db) {
  db.transaction(() => {
    // Add M2 anomaly columns to transactions table.
    // All nullable; anomaly_dismissed defaults to 0 (not dismissed).
    const existing = db.runQuery(`PRAGMA table_info(transactions)`, [], true);
    const cols = existing.map(r => r.name);

    if (!cols.includes('anomaly_score')) {
      db.runQuery(
        `ALTER TABLE transactions ADD COLUMN anomaly_score REAL`,
        [],
      );
    }
    if (!cols.includes('anomaly_flags')) {
      db.runQuery(
        `ALTER TABLE transactions ADD COLUMN anomaly_flags TEXT`,
        [],
      );
    }
    if (!cols.includes('anomaly_dismissed')) {
      db.runQuery(
        `ALTER TABLE transactions ADD COLUMN anomaly_dismissed INTEGER DEFAULT 0`,
        [],
      );
    }
  });
}
