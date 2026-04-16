import { v4 as uuidv4 } from 'uuid';

const M1_CATEGORIES = [
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
  db.transaction(() => {
    const groups = db.runQuery(
      `SELECT id FROM category_groups
         WHERE is_income = 0 AND tombstone = 0
         ORDER BY sort_order ASC
         LIMIT 1`,
      [],
      true,
    );
    if (!groups.length) return;
    const groupId = groups[0].id;

    const maxRow = db.runQuery(
      `SELECT COALESCE(MAX(sort_order), 0) AS m FROM categories`,
      [],
      true,
    );
    let sortOrder = (maxRow[0]?.m || 0) + 16384;

    for (const name of M1_CATEGORIES) {
      const existing = db.runQuery(
        `SELECT id FROM categories
           WHERE UPPER(name) = UPPER(?) AND tombstone = 0
           LIMIT 1`,
        [name],
        true,
      );
      if (existing.length > 0) continue;

      db.runQuery(
        `INSERT INTO categories
           (id, name, is_income, cat_group, sort_order, tombstone, hidden)
         VALUES (?, ?, 0, ?, ?, 0, 0)`,
        [uuidv4(), name, groupId, sortOrder],
      );
      sortOrder += 16384;
    }
  });
}
