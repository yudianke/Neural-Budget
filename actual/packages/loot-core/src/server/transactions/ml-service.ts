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

export type MlSuggestion = {
  category: string;
  categoryId: string | null;
  confidence: number;
};

export type MlPredictionResult = {
  categoryId: string | null;
  confidence: number;
  top3: MlSuggestion[];
};

export type M1FeedbackEntry = {
  transaction_id: string;
  date: string;
  amount: number;
  merchant: string;
  imported_payee?: string;
  predicted_category: string;
  predicted_category_id?: string | null;
  chosen_category: string;
  chosen_category_id?: string | null;
  confidence: number;
  feedback_type: 'accepted' | 'overridden';
  top_3_suggestions: Array<{
    category: string;
    confidence: number;
  }>;
  source?: string;
};

// In the browser Web Worker, cross-origin fetches are blocked by COEP.
// Route through the Vite dev-server proxy (/ml-api) which is same-origin.
// In Node.js (electron/API), fall back to the direct URL.
const DEFAULT_M1_URL = 'http://129.114.26.3:8001';
const CONFIDENCE_THRESHOLD = 0.6;
const CATEGORY_ALIASES: Record<string, string[]> = {
  // M1 project categories → ActualBudget default category names (fallback order)
  restaurants: ['food', 'general'],
  groceries: ['food', 'general'],
  gas: ['transport', 'bills', 'general'],
  transport: ['transport', 'bills', 'general'],
  shopping: ['general', 'food'],
  healthcare: ['bills', 'bills (flexible)', 'general'],
  entertainment: ['general', 'food'],
  utilities: ['bills', 'bills (flexible)', 'general'],
  housing: ['bills', 'general'],
  education: ['bills (flexible)', 'general'],
  charity: ['general'],
  'personal care': ['general', 'food'],
  'cash transfers': ['general'],
  cash_transfers: ['general'],
  misc: ['general'],
  // Ray model label aliases
  amazon: ['shopping', 'general'],
  bills: ['bills', 'bills (flexible)', 'utilities'],
  cash: ['general', 'misc'],
  clothes: ['shopping', 'general'],
  'dine out': ['restaurants', 'food'],
  fitness: ['personal care', 'healthcare'],
  health: ['healthcare', 'bills'],
  'home improvement': ['housing', 'bills (flexible)', 'general'],
  hotels: ['transport', 'general'],
  insurance: ['bills', 'bills (flexible)'],
  interest: ['income', 'general'],
  investment: ['savings', 'income'],
  mortgage: ['housing', 'bills'],
  'other shopping': ['shopping', 'general'],
  others: ['misc', 'general'],
  paycheck: ['income'],
  'purchase of uk.eg.org': ['shopping', 'general'],
  rent: ['housing', 'bills'],
  services: ['bills (flexible)', 'bills', 'general'],
  'services/home improvement': ['housing', 'bills (flexible)', 'general'],
  'supplementary income': ['income'],
  travel: ['transport', 'general'],
};

function getM1ServiceUrl(): string {
  const workerScope =
    typeof self !== 'undefined'
      ? (self as typeof globalThis & {
          importScripts?: unknown;
          location?: Location;
        })
      : null;
  // If running in a browser Web Worker, use the same-origin proxy
  // to avoid COEP/CORS blocking. Check location only (not importScripts)
  // since module workers don't expose importScripts.
  if (workerScope && workerScope.location) {
    return `${workerScope.location.origin}/ml-api`;
  }
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

async function lookupCategoryIdByNormalizedName(
  normalized: string,
): Promise<string | null> {
  if (!normalized) {
    return null;
  }

  const row = await first<{ id: string }>(
    `SELECT id FROM categories
       WHERE LOWER(REPLACE(name, '_', ' ')) = ? AND tombstone = 0
       LIMIT 1`,
    [normalized],
  );
  return row?.id ?? null;
}

async function lookupCategoryIdByName(name: string): Promise<string | null> {
  const normalized = normalizeCategoryName(name);
  const exact = await lookupCategoryIdByNormalizedName(normalized);
  if (exact) {
    return exact;
  }

  const aliases = CATEGORY_ALIASES[normalized] ?? [];
  for (const alias of aliases) {
    const aliasId = await lookupCategoryIdByNormalizedName(alias);
    if (aliasId) {
      return aliasId;
    }
  }

  logger.warn(
    `M1 category lookup miss for "${name}" (normalized "${normalized}")`,
  );
  return null;
}

/**
 * Full prediction result including top-3 suggestions and raw confidence.
 * Used by transaction-rules.ts to attach ML metadata to the transaction so
 * the UI can display AI suggestions and confidence badges.
 */
export async function predictCategoryWithSuggestions(
  trans: TransForPrediction,
): Promise<MlPredictionResult | null> {
  const merchant = trans.payee_name || trans.imported_payee;
  if (!merchant) return null;
  if (typeof trans.amount !== 'number' || trans.amount === 0) return null;

  const url = `${getM1ServiceUrl()}/predict/category`;
  try {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 5000);

    const res = await fetch(url, {
      method: 'POST',
      mode: 'cors',
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
      top_3_suggestions?: Array<{ category: string; confidence: number }>;
    };

    // Resolve category IDs for all top-3 suggestions in parallel
    const rawTop3 = body.top_3_suggestions ?? [
      { category: body.predicted_category, confidence: body.confidence },
    ];
    const top3: MlSuggestion[] = await Promise.all(
      rawTop3.map(async s => {
        const catId = await lookupCategoryIdByName(s.category);
        return {
          category: s.category,
          categoryId: catId,
          confidence: s.confidence,
        };
      }),
    );

    // Only resolve the winning category ID if confidence meets threshold
    const categoryId =
      body.confidence >= CONFIDENCE_THRESHOLD
        ? await lookupCategoryIdByName(body.predicted_category)
        : null;
    return { categoryId, confidence: body.confidence, top3 };
  } catch (err) {
    logger.warn('M1 service call failed', err);
    return null;
  }
}

/**
 * Legacy single-value entry point — kept for backward compatibility.
 * Returns only the winning category UUID (or null if below threshold).
 */
export async function predictCategory(
  trans: TransForPrediction,
): Promise<string | null> {
  const result = await predictCategoryWithSuggestions(trans);
  return result?.categoryId ?? null;
}

export async function logM1FeedbackBatch(
  entries: M1FeedbackEntry[],
): Promise<number> {
  if (!entries || entries.length === 0) {
    return 0;
  }

  const url = `${getM1ServiceUrl()}/feedback`;
  try {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 5000);
    const res = await fetch(url, {
      method: 'POST',
      mode: 'cors',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ entries }),
      signal: controller.signal,
    });
    clearTimeout(timeoutId);

    if (!res.ok) {
      logger.warn(`M1 feedback logging returned ${res.status}`);
      return 0;
    }

    const body = (await res.json()) as { logged?: number };
    return body.logged ?? 0;
  } catch (err) {
    logger.warn('M1 feedback logging failed', err);
    return 0;
  }
}
