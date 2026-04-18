/**
 * Fix missing category_mapping rows for M1 categories.
 *
 * The AQL view resolves transaction.category through category_mapping:
 *   LEFT JOIN category_mapping cm ON cm.id = _.category
 *   → category = cm.transferId
 *
 * Categories inserted by migration 1776000000000 were missing their
 * category_mapping rows (id → id), causing category to always read as NULL
 * in v_transactions even though the raw column had the correct UUID.
 */
const M1_CATEGORY_NAMES = [
  'Restaurants',
  'Groceries',
  'Gas',
  'Utilities',
  'Housing',
  'Transport',
  'Shopping',
  'Healthcare',
  'Entertainment',
  'Education',
  'Charity',
  'Personal Care',
  'Misc',
];

export default async function runMigration(db) {
  console.log('[M1-migration] 1776000000001 fixing category_mapping...');
  db.transaction(() => {
    let fixed = 0;
    for (const name of M1_CATEGORY_NAMES) {
      const cats = db.runQuery(
        `SELECT id FROM categories WHERE UPPER(name) = UPPER(?) AND tombstone = 0 LIMIT 1`,
        [name],
        true,
      );
      if (!cats.length) continue;

      const catId = cats[0].id;

      // Check if mapping already exists
      const existing = db.runQuery(
        `SELECT id FROM category_mapping WHERE id = ? LIMIT 1`,
        [catId],
        true,
      );
      if (existing.length > 0) continue;

      db.runQuery(
        `INSERT OR IGNORE INTO category_mapping (id, transferId) VALUES (?, ?)`,
        [catId, catId],
      );
      console.log('[M1-migration] fixed mapping for:', name, catId);
      fixed++;
    }
    console.log('[M1-migration] 1776000000001 done, fixed', fixed, 'mappings');
  });
}
