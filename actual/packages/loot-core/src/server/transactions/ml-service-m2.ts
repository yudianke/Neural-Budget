// @ts-strict-ignore
import * as connection from '#platform/server/connection';
import { logger } from '#platform/server/log';
import { all, first, updateTransaction } from '#server/db';

type TransForAnomalyScoring = {
  id?: string;
  // Public field names (from AQL layer)
  payee_name?: string;
  imported_payee?: string;
  // Internal field names (raw SQLite columns)
  imported_description?: string;
  description?: string;
  amount?: number;
  date?: string;
  category?: string;
};

export type M2AnomalyResult = {
  transaction_id: string;
  synthetic_user_id: string;
  anomaly_score: number;
  is_anomaly: boolean;
  threshold: number;
  rule_flags: {
    duplicate_within_24h: boolean;
    subscription_jump: boolean;
    amount_spike: boolean;
  };
  badge_type: string | null;
  model_version: string;
};

const DEFAULT_M2_URL = 'http://localhost:8003';
// Minimum transactions before M2 is active (mirrors train_m2.py min_transactions).
// Cold-start guard: IsolationForest needs enough history to establish a normal baseline.
const MIN_TRANSACTIONS = 50;

function getM2ServiceUrl(): string {
  return (
    (typeof process !== 'undefined' && process.env?.M2_SERVICE_URL) ||
    DEFAULT_M2_URL
  );
}

/**
 * Compute the 6 model features and deterministic rule booleans from SQLite.
 * One compound query for user-level stats + two targeted queries for rules.
 */
async function computeFeatures(trans: TransForAnomalyScoring): Promise<{
  absAmount: number;
  repeatCount: number;
  isRecurringCandidate: number;
  userTxnIndex: number;
  userMeanAbsAmountPrior: number;
  userStdAbsAmountPrior: number;
  duplicateWithin24h: boolean;
  subscriptionJump: boolean;
}> {
  const amount = typeof trans.amount === 'number' ? trans.amount / 100 : 0;
  const absAmount = Math.abs(amount);
  // Try both public and internal field names for the merchant string
  const merchant =
    trans.payee_name ||
    trans.imported_payee ||
    trans.imported_description ||
    '';

  // --- User-level stats from all prior transactions ---
  const statsRow = await first<{
    txn_count: number;
    mean_abs: number;
  }>(
    `SELECT COUNT(*) AS txn_count,
            AVG(ABS(amount) / 100.0) AS mean_abs
     FROM transactions
     WHERE tombstone = 0`,
    [],
  );

  const userTxnIndex = statsRow?.txn_count ?? 0;
  const userMeanAbsAmountPrior = statsRow?.mean_abs ?? 0;

  // Compute stddev in JS (SQLite has no STDDEV). For cold-start (<2 txns) default to 1.
  let userStdAbsAmountPrior = 1.0;
  if (userTxnIndex >= 2) {
    const allAmounts = await all<{ a: number }>(
      `SELECT ABS(amount) / 100.0 AS a FROM transactions WHERE tombstone = 0`,
      [],
    );
    const vals = allAmounts.map(r => r.a);
    const mean = userMeanAbsAmountPrior;
    const variance = vals.reduce((s, v) => s + (v - mean) ** 2, 0) / vals.length;
    userStdAbsAmountPrior = Math.sqrt(variance) || 1.0;
  }

  // --- repeat_count: how many times this merchant appears ---
  // Note: ActualBudget internal column names differ from public names:
  //   public "imported_payee" → internal "imported_description"
  //   public "payee" → internal "description" (stores payee ID, not name)
  const repeatRow = await first<{ cnt: number }>(
    `SELECT COUNT(*) AS cnt FROM transactions
     WHERE imported_description = ? AND tombstone = 0`,
    [merchant],
  );
  const repeatCount = repeatRow?.cnt ?? 0;

  // --- is_recurring_candidate: merchant appears in ≥ 2 distinct calendar months ---
  const recurRow = await first<{ months: number }>(
    `SELECT COUNT(DISTINCT strftime('%Y-%m', date)) AS months
     FROM transactions
     WHERE imported_description = ? AND tombstone = 0`,
    [merchant],
  );
  const isRecurringCandidate = (recurRow?.months ?? 0) >= 2 ? 1 : 0;

  // --- duplicate_within_24h: same merchant + same abs amount within last 24 h ---
  const dateStr = trans.date || new Date().toISOString().slice(0, 10);
  const dupRow = await first<{ found: number }>(
    `SELECT 1 AS found FROM transactions
     WHERE imported_description = ?
       AND ABS(amount) = ?
       AND date >= date(?, '-1 day')
       AND tombstone = 0
     LIMIT 1`,
    [merchant, Math.abs(trans.amount ?? 0), dateStr],
  );
  const duplicateWithin24h = dupRow != null;

  // --- subscription_jump: new abs amount ≥ 2× median of last 5 charges ---
  let subscriptionJump = false;
  if (isRecurringCandidate && absAmount > 0) {
    const recentRows = await all<{ a: number }>(
      `SELECT ABS(amount) / 100.0 AS a FROM transactions
       WHERE imported_description = ? AND tombstone = 0
       ORDER BY date DESC LIMIT 5`,
      [merchant],
    );
    if (recentRows.length >= 2) {
      const sorted = recentRows.map(r => r.a).sort((a, b) => a - b);
      const median = sorted[Math.floor(sorted.length / 2)];
      if (median > 0 && absAmount >= 2 * median) {
        subscriptionJump = true;
      }
    }
  }

  return {
    absAmount,
    repeatCount,
    isRecurringCandidate,
    userTxnIndex,
    userMeanAbsAmountPrior,
    userStdAbsAmountPrior,
    duplicateWithin24h,
    subscriptionJump,
  };
}

/**
 * Fire an async anomaly score request to M2 service.
 * Returns null on any failure (service down, timeout, cold-start guard).
 * Must be called fire-and-forget from transaction-rules.ts — never awaited on the save path.
 */
export async function scoreAnomaly(
  trans: TransForAnomalyScoring,
): Promise<M2AnomalyResult | null> {
  logger.info('[M2] scoreAnomaly called', { id: trans.id, amount: trans.amount });

  if (!trans.id) {
    logger.info('[M2] no trans.id, skipping');
    return null;
  }

  // Cold-start guard: do not score until user has enough history
  const countRow = await first<{ n: number }>(
    `SELECT COUNT(*) AS n FROM transactions WHERE tombstone = 0`,
    [],
  );
  const txnCount = countRow?.n ?? 0;
  logger.info(`[M2] transaction count: ${txnCount}, threshold: ${MIN_TRANSACTIONS}`);
  if (txnCount < MIN_TRANSACTIONS) return null;

  const features = await computeFeatures(trans);
  logger.info('[M2] features computed', features);

  const url = `${getM2ServiceUrl()}/predict/anomaly`;
  try {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 4000);

    const res = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        transaction_id: trans.id,
        synthetic_user_id: 'actual_user',
        abs_amount: features.absAmount,
        repeat_count: features.repeatCount,
        is_recurring_candidate: features.isRecurringCandidate,
        user_txn_index: features.userTxnIndex,
        user_mean_abs_amount_prior: features.userMeanAbsAmountPrior,
        user_std_abs_amount_prior: features.userStdAbsAmountPrior,
        duplicate_within_24h: features.duplicateWithin24h,
        subscription_jump: features.subscriptionJump,
        merchant: trans.payee_name || trans.imported_payee || '',
        date: trans.date || '',
      }),
      signal: controller.signal,
    });
    clearTimeout(timeoutId);

    if (!res.ok) {
      logger.warn(`M2 service returned ${res.status}`);
      return null;
    }

    const body = (await res.json()) as M2AnomalyResult;
    logger.info('[M2] response:', body);
    return body;
  } catch (err) {
    logger.warn('[M2] service call failed', err);
    return null;
  }
}

/**
 * Persist anomaly result to the transactions table (fire-and-forget callback).
 * Only writes when the transaction is flagged.
 */
export async function persistAnomalyResult(
  transId: string,
  result: M2AnomalyResult,
): Promise<void> {
  if (
    !result.is_anomaly &&
    !result.rule_flags.duplicate_within_24h &&
    !result.rule_flags.subscription_jump
  ) {
    return;
  }
  await updateTransaction({
    id: transId,
    anomaly_score: result.anomaly_score,
    anomaly_flags: JSON.stringify(result.rule_flags),
    anomaly_dismissed: 0,
  });

  // Notify frontend so the live query re-fetches and shows the badge immediately
  connection.send('sync-event', {
    type: 'applied',
    tables: ['transactions'],
  });
}

/**
 * Send dismiss feedback to M2 serving for close-the-loop monitoring.
 * Called fire-and-forget when user clicks × on anomaly badge.
 */
export async function sendDismissFeedback(
  transId: string,
  badgeType: string | null,
  anomalyScore: number | null,
  ruleFlags: string | null,
  merchant: string | null,
  amount: number | null,
  date: string | null,
): Promise<void> {
  const url = `${getM2ServiceUrl()}/feedback`;
  try {
    let parsedFlags = null;
    if (ruleFlags) {
      try {
        parsedFlags = JSON.parse(ruleFlags);
      } catch {
        // ignore
      }
    }

    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 3000);

    await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        entries: [
          {
            transaction_id: transId,
            feedback_type: 'dismiss_false_positive',
            badge_type: badgeType,
            anomaly_score: anomalyScore,
            rule_flags: parsedFlags,
            merchant: merchant || '',
            amount: amount != null ? amount / 100 : null,
            date: date || '',
          },
        ],
      }),
      signal: controller.signal,
    });
    clearTimeout(timeoutId);

    logger.info('[M2] dismiss feedback sent', { transId, badgeType });
  } catch (err) {
    logger.warn('[M2] dismiss feedback failed (non-fatal)', err);
  }
}
