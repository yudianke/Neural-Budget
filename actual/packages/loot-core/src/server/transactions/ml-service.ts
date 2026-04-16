// @ts-strict-ignore
import { logger } from '#platform/server/log';
import { first } from '#server/db';

type TransForPrediction = {
  id?: string;
  payee_name?: string;
  imported_payee?: string;
  amount?: number;
  date?: string;
};

const DEFAULT_M1_URL = 'http://localhost:8001';
const CONFIDENCE_THRESHOLD = 0.6;

function getM1ServiceUrl(): string {
  return (
    (typeof process !== 'undefined' && process.env?.M1_SERVICE_URL) ||
    DEFAULT_M1_URL
  );
}

function buildRequestBody(trans: TransForPrediction) {
  const merchant = trans.payee_name || trans.imported_payee || '';
  const amount = typeof trans.amount === 'number' ? trans.amount / 100 : 0;
  const absAmount = Math.abs(amount);
  const logAbs = Math.log1p(absAmount);

  const dateStr = trans.date || new Date().toISOString().slice(0, 10);
  const d = new Date(dateStr);
  const dayOfWeek = isNaN(d.getTime()) ? 0 : d.getDay();
  const dayOfMonth = isNaN(d.getTime()) ? 1 : d.getDate();
  const month = isNaN(d.getTime()) ? 1 : d.getMonth() + 1;

  return {
    transaction_id: trans.id || 'unknown',
    synthetic_user_id: 'actual_user',
    date: dateStr,
    merchant,
    amount,
    transaction_type: amount < 0 ? 'DEB' : 'CRE',
    account_type: 'checking',
    day_of_week: dayOfWeek,
    day_of_month: dayOfMonth,
    month,
    log_abs_amount: logAbs,
    historical_majority_category_for_payee: '',
  };
}

function normalizeCategoryName(name: string): string {
  return name.replace(/_/g, ' ').trim().toLowerCase();
}

async function lookupCategoryIdByName(name: string): Promise<string | null> {
  const normalized = normalizeCategoryName(name);
  const row = await first<{ id: string }>(
    `SELECT id FROM categories
       WHERE LOWER(REPLACE(name, '_', ' ')) = ? AND tombstone = 0
       LIMIT 1`,
    [normalized],
  );
  if (!row) {
    logger.warn(`M1 category lookup miss for "${name}" (normalized "${normalized}")`);
  }
  return row?.id ?? null;
}

export async function predictCategory(
  trans: TransForPrediction,
): Promise<string | null> {
  const merchant = trans.payee_name || trans.imported_payee;
  if (!merchant) return null;

  if (typeof trans.amount !== 'number' || trans.amount === 0) return null;

  const url = `${getM1ServiceUrl()}/predict/category`;
  try {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 1000);

    const res = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(buildRequestBody(trans)),
      signal: controller.signal,
    });
    clearTimeout(timeoutId);

    if (!res.ok) {
      logger.warn(`M1 service returned ${res.status}`);
      return null;
    }

    const body = (await res.json()) as {
      predicted_category: string;
      confidence: number;
    };

    if (body.confidence < CONFIDENCE_THRESHOLD) return null;

    return await lookupCategoryIdByName(body.predicted_category);
  } catch (err) {
    logger.warn('M1 service call failed', err);
    return null;
  }
}
