import * as db from '../db';

/**
 * Row format that matches your Python ingestion schema
 */
export interface MonthlyCategoryHistoryRow {
  budget_id: string;
  category_id: string;
  category_name: string;
  year_month: string; // YYYY-MM
  monthly_spend: number;
  budgeted: number;
}

/**
 * Extract monthly spend per category from Actual DB
 */
export async function exportMonthlyCategoryHistory(
  budgetId: string
): Promise<MonthlyCategoryHistoryRow[]> {
  // Get transactions joined with categories
  const transactions = await db.all(`
    SELECT
      t.date,
      t.amount,
      t.category_id,
      c.name as category_name
    FROM transactions t
    LEFT JOIN categories c ON t.category_id = c.id
    WHERE t.category_id IS NOT NULL
  `);

  const resultMap = new Map<string, MonthlyCategoryHistoryRow>();

  for (const t of transactions) {
    const date = new Date(t.date);

    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const yearMonth = `${year}-${month}`;

    const key = `${t.category_id}__${yearMonth}`;

    if (!resultMap.has(key)) {
      resultMap.set(key, {
        budget_id: budgetId,
        category_id: t.category_id,
        category_name: t.category_name || 'Unknown',
        year_month: yearMonth,
        monthly_spend: 0,
        budgeted: 0, // we’ll fill later
      });
    }

    const row = resultMap.get(key)!;

    // IMPORTANT: Actual stores expenses as negative cents.
    // Skip income/refunds (amount >= 0) — only count actual outgoing spend.
    // Consistent with getCategoryPredictions in app.ts which filters amount > 0.
    if (t.amount >= 0) continue;
    const amount = Math.abs(t.amount) / 100;

    row.monthly_spend += amount;
  }

  return Array.from(resultMap.values());
}
