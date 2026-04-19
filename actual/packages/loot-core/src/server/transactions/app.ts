import {createApp} from '#server/app';
import {aqlQuery} from '#server/aql';
import { exportMonthlyCategoryHistory } from '../forecast/export-monthly-history';

import * as sheet from '../sheet';
import * as monthUtils from '#shared/months';
import * as db from '#server/db';
import {mutator} from '#server/mutators';
import {undoable} from '#server/undo';
import {q, Query} from '#shared/query';
import type {QueryState} from '#shared/query';
import type {
  AccountEntity,
  CategoryGroupEntity,
  PayeeEntity,
  TransactionEntity,
} from '#types/models';

import { exportQueryToCSV, exportToCSV } from './export/export-to-csv';
import { parseFile } from './import/parse-file';
import type { ParseFileOptions } from './import/parse-file';
import { logM1FeedbackBatch } from './ml-service';
import { mergeTransactions } from './merge';

import {batchUpdateTransactions} from '.';

export type TransactionHandlers = {
  'transactions-batch-update': typeof handleBatchUpdateTransactions;
  'transaction-add': typeof addTransaction;
  'transaction-update': typeof updateTransaction;
  'transaction-delete': typeof deleteTransaction;
  'm1-feedback-log-batch': typeof handleM1FeedbackLogBatch;
  'transaction-move': typeof moveTransaction;
  'transactions-parse-file': typeof parseTransactionsFile;
  'transactions-export': typeof exportTransactions;
  'transactions-export-query': typeof exportTransactionsQuery;
  'transactions-merge': typeof mergeTransactions;
  'get-earliest-transaction': typeof getEarliestTransaction;
  'get-latest-transaction': typeof getLatestTransaction;
  'forecast-get-category-predictions': typeof getCategoryPredictions;
  'forecast-export-monthly-history': typeof exportForecastMonthlyHistory;
  'forecast-apply-as-budgets': typeof applyForecastsAsBudgets;
};

async function exportForecastMonthlyHistory() {
  const prefs = await import('#server/prefs');
  const currentPrefs = prefs.getPrefs() || {};
  const budgetId = currentPrefs.id || 'unknown-budget';

  return exportMonthlyCategoryHistory(budgetId);
}

async function handleBatchUpdateTransactions({
                                               added,
                                               deleted,
                                               updated,
                                               learnCategories,
                                               runTransfers = true,
                                             }: Parameters<typeof batchUpdateTransactions>[0]) {
  const result = await batchUpdateTransactions({
    added,
    updated,
    deleted,
    learnCategories,
    runTransfers,
  });

  return result;
}

async function addTransaction(transaction: TransactionEntity) {
  await handleBatchUpdateTransactions({added: [transaction]});
  return {};
}

async function updateTransaction(transaction: TransactionEntity) {
  await handleBatchUpdateTransactions({updated: [transaction]});
  return {};
}

async function deleteTransaction(transaction: Pick<TransactionEntity, 'id'>) {
  await handleBatchUpdateTransactions({deleted: [transaction]});
  return {};
}

async function handleM1FeedbackLogBatch({
  entries,
}: {
  entries: Parameters<typeof logM1FeedbackBatch>[0];
}) {
  return logM1FeedbackBatch(entries);
}

async function moveTransaction({
  id,
  accountId,
  targetId,
}: {
  id: string;
  accountId: string;
  targetId: string | null;
}) {
  const transaction = await db.getTransaction(id);
  if (!transaction) {
    throw new Error(`Transaction not found: ${id}`);
  }

  if (transaction.account !== accountId) {
    throw new Error(
      `Account mismatch: transaction belongs to account ${transaction.account}, not ${accountId}`,
    );
  }

  await db.moveTransaction(id, accountId, targetId);
  return {};
}

async function parseTransactionsFile({
                                       filepath,
                                       options,
                                     }: {
  filepath: string;
  options: ParseFileOptions;
}) {
  return parseFile(filepath, options);
}

async function exportTransactions({
                                    transactions,
                                    accounts,
                                    categoryGroups,
                                    payees,
                                  }: {
  transactions: TransactionEntity[];
  accounts: AccountEntity[];
  categoryGroups: CategoryGroupEntity[];
  payees: PayeeEntity[];
}) {
  return exportToCSV(transactions, accounts, categoryGroups, payees);
}

async function exportTransactionsQuery({
                                         query: queryState,
                                       }: {
  query: QueryState;
}) {
  return exportQueryToCSV(new Query(queryState));
}

async function getEarliestTransaction() {
  const {data} = await aqlQuery(
    q('transactions')
      .options({splits: 'none'})
      .orderBy({date: 'asc'})
      .select('*')
      .limit(1),
  );
  return data[0] || null;
}

async function getLatestTransaction() {
  const {data} = await aqlQuery(
    q('transactions')
      .options({splits: 'none'})
      .orderBy({date: 'desc'})
      .select('*')
      .limit(1),
  );
  return data[0] || null;
}

const DEFAULT_M3_URL = 'http://129.114.26.3:8002';

function getM3ServiceUrl(): string {
  // Web Worker context — use same-origin Vite proxy to avoid COEP blocking
  if (typeof self !== 'undefined' && typeof (self as any).importScripts === 'function') {
    return `${(self as any).location.origin}/m3-api`;
  }
  return (
    (typeof process !== 'undefined' && process.env?.M3_SERVICE_URL) ||
    DEFAULT_M3_URL
  );
}

function monthToNumber(yearMonth: string) {
  return Number(yearMonth.slice(5, 7));
}

function quarterFromMonth(monthNum: number) {
  return Math.floor((monthNum - 1) / 3) + 1;
}

function monthTrig(monthNum: number) {
  const angle = (2 * Math.PI * monthNum) / 12;
  return {
    month_sin: Math.sin(angle),
    month_cos: Math.cos(angle),
  };
}

type ForecastFeatureRow = {
  project_category: string;
  monthly_spend: number;
  lag_1: number;
  lag_2: number;
  lag_3: number;
  lag_4: number;
  lag_5: number;
  lag_6: number;
  rolling_mean_3: number;
  rolling_std_3: number;
  rolling_mean_6: number;
  rolling_max_3: number;
  history_month_count: number;
  month_num: number;
  quarter: number;
  year: number;
  is_q4: number;
  month_sin: number;
  month_cos: number;
};

async function getCategoryPredictions() {

  const {data} = await aqlQuery(
    q('transactions')
      .options({splits: 'none'})
      .select([
        'id',
        'date',
        'amount',
        'category',
        'payee',
        'account',
        'transfer_id',
      ]),
  );


  const now = new Date();
  const currentMonth = `${now.getFullYear()}-${String(
    now.getMonth() + 1,
  ).padStart(2, '0')}`;

  // M3 forecasts NEXT month's spend, so budget targets should be read from
  // next month's sheet — not the current month's sheet.
  const nextMonth = monthUtils.addMonths(currentMonth, 1);

  const {data: categoryRows} = await aqlQuery(
    q('categories').select(['id', 'name']),
  );

  const nextMonthSheetName = monthUtils.sheetForMonth(nextMonth);

  // Build budgetMap keyed by category id → next-month budgeted amount (dollars).
  // Falls back to current month if next month has no budget entry, so the gap
  // is still meaningful for users who copy budgets forward.
  const currentSheetName = monthUtils.sheetForMonth(currentMonth);

  const budgetMap = new Map<string, number>();

  for (const cat of categoryRows) {
    const nextVal = sheet.get().getCellValue(nextMonthSheetName, `budget-${cat.id}`);
    const currVal = sheet.get().getCellValue(currentSheetName, `budget-${cat.id}`);
    // Prefer next month; fall back to current month; 0 if neither is set.
    const raw = nextVal !== '' && Number(nextVal || 0) !== 0
      ? nextVal
      : currVal;
    budgetMap.set(cat.id, raw === '' ? 0 : Number(raw || 0));
  }
  if (!data || data.length === 0) {
    return {forecasts: [], model_name: 'm3-forecast'};
  }

  const categoryIds = [
    ...new Set(
      data
        .map((t: { category?: string | null }) => t.category)
        .filter((c): c is string => !!c),
    ),
  ];

  const categories = await Promise.all(
    categoryIds.map(id => db.getCategory(id as string)),
  );

  const categoryMap = new Map(
    categories.filter(Boolean).map(cat => [cat!.id, cat!.name]),
  );

  const filtered = data.filter(t => {
    if (!t.category) return false;
    if (t.transfer_id) return false;
    if (!t.date) return false;

    const amount = Number(t.amount || 0);

    // remove income
    if (amount > 0) return false;

    return true;
  });

  if (filtered.length === 0) {
    return {forecasts: [], model_name: 'm3-forecast'};
  }

  const monthlyMap = new Map<string, number>();

  for (const txn of filtered) {
    const categoryName = categoryMap.get(txn.category);
    if (!categoryName) continue;

    const yearMonth = String(txn.date).slice(0, 7);

    // Exclude the current in-progress month — it is a partial month and its
    // spend is systematically lower than a full month, which biases lag_1 low
    // (e.g. on April 18, only ~60% of April's spend is recorded).
    // The model was trained on complete months only.
    if (yearMonth === currentMonth) continue;

    const amount = -Number(txn.amount || 0) / 100;

    const key = `${categoryName}__${yearMonth}`;
    monthlyMap.set(key, (monthlyMap.get(key) || 0) + amount);
  }

  const monthlyRows = [...monthlyMap.entries()].map(([key, monthly_spend]) => {
    const [project_category, year_month] = key.split('__');
    return {project_category, year_month, monthly_spend};
  });

  const byCategory = new Map<
    string,
    Array<{ year_month: string; monthly_spend: number }>
  >();

  for (const row of monthlyRows) {
    if (!byCategory.has(row.project_category)) {
      byCategory.set(row.project_category, []);
    }
    byCategory.get(row.project_category)!.push({
      year_month: row.year_month,
      monthly_spend: row.monthly_spend,
    });
  }

  const featureRows: ForecastFeatureRow[] = [];

  for (const [project_category, rows] of byCategory.entries()) {
    rows.sort((a, b) => a.year_month.localeCompare(b.year_month));

    if (rows.length < 3) {
      continue;
    }

    const history = rows.map(r => r.monthly_spend);
    const latest = rows[rows.length - 1];
    const latestMonth = latest.year_month;

    const lag = (k: number) =>
      history.length - k - 1 >= 0
        ? history[history.length - k - 1]
        : 0;

    const prior3 = history.slice(Math.max(0, history.length - 3));
    const prior6 = history.slice(Math.max(0, history.length - 6));

    const mean = (arr: number[]) =>
      arr.length ? arr.reduce((a, b) => a + b, 0) / arr.length : 0;

    const std = (arr: number[]) => {
      if (arr.length <= 1) return 0;
      const m = mean(arr);
      const variance =
        arr.reduce((acc, x) => acc + (x - m) ** 2, 0) / (arr.length - 1);
      return Math.sqrt(variance);
    };

    const month_num = monthToNumber(latestMonth);
    const quarter = quarterFromMonth(month_num);
    const year = Number(latestMonth.slice(0, 4));
    const is_q4 = [10, 11, 12].includes(month_num) ? 1 : 0;
    const {month_sin, month_cos} = monthTrig(month_num);

    // lag offsets match training convention in _build_supervised_rows:
    //   monthly_spend = prior[-1]  → lag(0) = history[-1]
    //   lag_1         = prior[-1]  → lag(0) = history[-1]  (same as monthly_spend)
    //   lag_2         = prior[-2]  → lag(1) = history[-2]
    //   lag_3         = prior[-3]  → lag(2) = history[-3]
    //   lag_4         = prior[-4]  → lag(3) = history[-4]
    //   lag_5         = prior[-5]  → lag(4) = history[-5]
    //   lag_6         = prior[-6]  → lag(5) = history[-6]
    featureRows.push({
      project_category,
      monthly_spend: latest.monthly_spend,
      lag_1: lag(0),
      lag_2: lag(1),
      lag_3: lag(2),
      lag_4: lag(3),
      lag_5: lag(4),
      lag_6: lag(5),
      rolling_mean_3: mean(prior3),
      rolling_std_3: std(prior3),
      rolling_mean_6: mean(prior6),
      rolling_max_3: prior3.length ? Math.max(...prior3) : 0,
      history_month_count: rows.length,
      month_num,
      quarter,
      year,
      is_q4,
      month_sin,
      month_cos,
    });
  }

  if (featureRows.length === 0) {
    return {forecasts: [], model_name: 'm3-forecast'};
  }

  let result: { forecasts: Array<{ category: string; forecast: number | null }>; model_name: string };
  try {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 5000);
    const response = await fetch(`${getM3ServiceUrl()}/forecast/features`, {
      method: 'POST',
      mode: 'cors',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({rows: featureRows}),
      signal: controller.signal,
    });
    clearTimeout(timeoutId);
    if (!response.ok) {
      throw new Error(`M3 service returned ${response.status}`);
    }
    result = await response.json();
  } catch (err) {
    // M3 service down or timeout — return empty forecasts, don't crash UI
    return {forecasts: [], model_name: 'm3-forecast'};
  }

  // Normalize category names for case-insensitive, underscore-tolerant matching.
  // M3 training data uses lowercase underscore names ("personal_care"),
  // ActualBudget DB uses TitleCase space names ("Personal Care").
  // M1 has a full CATEGORY_ALIASES map; M3 at minimum needs this normalization.
  const normalizeCatName = (s: string) =>
    s.replace(/_/g, ' ').trim().toLowerCase();

  // Pre-build a normalized lookup: normalizedName → categoryId
  const normalizedCategoryMap = new Map<string, string>();
  for (const [id, name] of categoryMap.entries()) {
    normalizedCategoryMap.set(normalizeCatName(name), id);
  }

  const enrichedForecasts = result.forecasts.map(
    (forecast: { category: string; forecast: number | null }) => {
      const normalizedForecastCat = normalizeCatName(forecast.category);

      const match = featureRows.find(
        row => normalizeCatName(row.project_category) === normalizedForecastCat,
      );

      const categoryId = normalizedCategoryMap.get(normalizedForecastCat);

      const budgeted =
        categoryId && budgetMap.has(categoryId)
          ? Number(budgetMap.get(categoryId)) / 100
          : 0;

      const gap_to_budget =
        forecast.forecast != null && budgeted > 0
          ? forecast.forecast - budgeted
          : null;

      return {
        ...forecast,
        // lag_1 = current month (same as monthly_spend after fix)
        // lag_2 = actual previous month spend
        last_month: match ? match.lag_2 : null,
        budgeted,
        gap_to_budget,
      };
    },
  );

  const sortedForecasts = [...enrichedForecasts].sort((a, b) => {
    const aGap = a.gap_to_budget ?? Number.NEGATIVE_INFINITY;
    const bGap = b.gap_to_budget ?? Number.NEGATIVE_INFINITY;
    return bGap - aGap;
  });

  return {
    forecasts: sortedForecasts,
    model_name: result.model_name,
  };
}

/**
 * Apply M3 forecast amounts as next-month budget targets.
 *
 * Only writes to categories where the existing next-month budget is 0 (or unset)
 * — never overwrites a budget the user has already set intentionally.
 *
 * @param entries  Array of { categoryName, amount } from the ForecastCard.
 *                 categoryName is the raw string from the M3 response (may be
 *                 TitleCase or lowercase_underscore — we normalize it).
 *                 amount is in dollars (float); we convert to integer cents.
 * @returns        { applied: number, skipped: number, month: string }
 */
async function applyForecastsAsBudgets({
  entries,
}: {
  entries: Array<{ categoryName: string; amount: number }>;
}): Promise<{ applied: number; skipped: number; month: string }> {
  const { setBudget } = await import('../budget/actions');

  const now = new Date();
  const currentMonth = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}`;
  const targetMonth = monthUtils.addMonths(currentMonth, 1);
  const targetSheetName = monthUtils.sheetForMonth(targetMonth);

  // Load all categories so we can match by name (normalized)
  const { data: categoryRows } = await aqlQuery(
    q('categories').select(['id', 'name']),
  );

  const normalizeCatName = (s: string) =>
    s.replace(/_/g, ' ').trim().toLowerCase();

  // Build normalized name → id map
  const nameToId = new Map<string, string>();
  for (const cat of categoryRows) {
    nameToId.set(normalizeCatName(cat.name), cat.id);
  }

  let applied = 0;
  let skipped = 0;

  for (const entry of entries) {
    const categoryId = nameToId.get(normalizeCatName(entry.categoryName));
    if (!categoryId) {
      skipped++;
      continue;
    }

    // Read the current next-month budget for this category
    const existing = sheet.getCellValue(targetSheetName, `budget-${categoryId}`);
    const existingAmount = existing === '' ? 0 : Number(existing || 0);

    // Skip if the user already has a non-zero budget set
    if (existingAmount !== 0) {
      skipped++;
      continue;
    }

    // Convert dollars to integer cents (ActualBudget stores amounts in cents)
    const amountCents = Math.round(entry.amount * 100);
    if (amountCents <= 0) {
      skipped++;
      continue;
    }

    await setBudget({
      category: categoryId,
      month: targetMonth,
      amount: amountCents,
    });
    applied++;
  }

  return { applied, skipped, month: targetMonth };
}

export const app = createApp<TransactionHandlers>();

app.method(
  'transactions-batch-update',
  mutator(undoable(handleBatchUpdateTransactions)),
);
app.method('transactions-merge', mutator(undoable(mergeTransactions)));

app.method('transaction-add', mutator(addTransaction));
app.method('transaction-update', mutator(updateTransaction));
app.method('transaction-delete', mutator(deleteTransaction));
app.method('m1-feedback-log-batch', mutator(handleM1FeedbackLogBatch));
app.method('transaction-move', mutator(undoable(moveTransaction)));
app.method('transactions-parse-file', mutator(parseTransactionsFile));
app.method('transactions-export', mutator(exportTransactions));
app.method('transactions-export-query', mutator(exportTransactionsQuery));
app.method('get-earliest-transaction', getEarliestTransaction);
app.method('get-latest-transaction', getLatestTransaction);
app.method('forecast-get-category-predictions', getCategoryPredictions);
app.method('forecast-export-monthly-history', exportForecastMonthlyHistory);
app.method('forecast-apply-as-budgets', mutator(applyForecastsAsBudgets));
