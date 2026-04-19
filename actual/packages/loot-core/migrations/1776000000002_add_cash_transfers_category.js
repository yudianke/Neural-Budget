import { v4 as uuidv4 } from 'uuid';

/**
 * Add the Cash Transfers category that was missing from migration 1776000000000.
 *
 * Existing budgets that already ran 1776000000000 and 1776000000001 won't get
 * Cash Transfers from those migrations because the category wasn't in the list.
 * This migration adds it for all budgets that don't already have it, and ensures
 * the category_mapping row exists so the AQL view resolves it correctly.
 */
export default async function runMigration(db) {
  db.transaction(() => {
    // Skip if Cash Transfers already exists (e.g. user created it manually)
    const existing = db.runQuery(
      `SELECT id FROM categories
         WHERE UPPER(name) = UPPER('Cash Transfers') AND tombstone = 0
         LIMIT 1`,
      [],
      true,
    );
    if (existing.length > 0) return;

    // Find the first non-income expense group to attach the category to
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

    // Place after all existing categories
    const maxRow = db.runQuery(
      `SELECT COALESCE(MAX(sort_order), 0) AS m FROM categories`,
      [],
      true,
    );
    const sortOrder = (maxRow[0]?.m || 0) + 16384;

    const newId = uuidv4();
    db.runQuery(
      `INSERT INTO categories
         (id, name, is_income, cat_group, sort_order, tombstone, hidden)
       VALUES (?, 'Cash Transfers', 0, ?, ?, 0, 0)`,
      [newId, groupId, sortOrder],
    );

    // category_mapping row so the AQL v_transactions view resolves correctly
    db.runQuery(
      `INSERT OR IGNORE INTO category_mapping (id, transferId) VALUES (?, ?)`,
      [newId, newId],
    );
  });
}
