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

import {exportQueryToCSV, exportToCSV} from './export/export-to-csv';
import {parseFile} from './import/parse-file';
import type {ParseFileOptions} from './import/parse-file';
import {mergeTransactions} from './merge';

import {batchUpdateTransactions} from '.';

export type TransactionHandlers = {
  'transactions-batch-update': typeof handleBatchUpdateTransactions;
  'transaction-add': typeof addTransaction;
  'transaction-update': typeof updateTransaction;
  'transaction-delete': typeof deleteTransaction;
  'transaction-move': typeof moveTransaction;
  'transactions-parse-file': typeof parseTransactionsFile;
  'transactions-export': typeof exportTransactions;
  'transactions-export-query': typeof exportTransactionsQuery;
  'transactions-merge': typeof mergeTransactions;
  'get-earliest-transaction': typeof getEarliestTransaction;
  'get-latest-transaction': typeof getLatestTransaction;
  'forecast-get-category-predictions': typeof getCategoryPredictions;
  'forecast-export-monthly-history': typeof exportForecastMonthlyHistory;
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

  console.log(
    'FORECAST TXN SAMPLE:',
    data.slice(0, 10).map(t => ({
      id: t.id,
      date: t.date,
      amount: t.amount,
      category: t.category,
      payee: t.payee,
      transfer_id: t.transfer_id,
    })),
  );
  const now = new Date();
  const currentMonth = `${now.getFullYear()}-${String(
    now.getMonth() + 1,
  ).padStart(2, '0')}`;

  const {data: categoryRows} = await aqlQuery(
    q('categories').select(['id', 'name']),
  );

  const sheetName = monthUtils.sheetForMonth(currentMonth);

  function value(name: string) {
    const v = sheet.get().getCellValue(sheetName, name);
    return v === '' ? 0 : v;
  }

  console.log(
  'BUDGET DEBUG SAMPLE:',
  categoryRows.slice(0, 5).map(cat => ({
    id: cat.id,
    name: cat.name,
    budgeted: value(`budget-${cat.id}`),
    spent: value(`sum-amount-${cat.id}`),
    balance: value(`leftover-${cat.id}`),
  })),
);
  const budgetMap = new Map<string, number>();

  for (const cat of categoryRows) {
    const value = sheet.get().getCellValue(sheetName, `budget-${cat.id}`);
    budgetMap.set(cat.id, value === '' ? 0 : Number(value || 0));
  }
  if (!data || data.length === 0) {
    return {forecasts: [], model_name: 'm3-forecast-v2'};
  }

  const categoryIds = [
    ...new Set(
      data
        .map((t: { category?: string | null }) => t.category)
        .filter((c): c is string => !!c),
    ),
  ];

  const categories = await Promise.all(
    categoryIds.map(id => db.getCategory(id)),
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
    return {forecasts: [], model_name: 'm3-forecast-v2'};
  }

  const monthlyMap = new Map<string, number>();

  for (const txn of filtered) {
    const categoryName = categoryMap.get(txn.category);
    if (!categoryName) continue;

    const yearMonth = String(txn.date).slice(0, 7);
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

    featureRows.push({
      project_category,
      monthly_spend: latest.monthly_spend,
      lag_1: lag(1),
      lag_2: lag(2),
      lag_3: lag(3),
      lag_6: lag(6),
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
    return {forecasts: [], model_name: 'm3-forecast-v2'};
  }

  const response = await fetch('http://127.0.0.1:8002/forecast/features', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({rows: featureRows}),
  });

  if (!response.ok) {
    throw new Error(`Forecast service returned ${response.status}`);
  }

  const result = await response.json();

  const enrichedForecasts = result.forecasts.map(
    (forecast: { category: string; forecast: number | null }) => {
      const match = featureRows.find(
        row => row.project_category === forecast.category,
      );

      const categoryId = [...categoryMap.entries()].find(
        ([, name]) => name === forecast.category,
      )?.[0];

      const budgeted =
        categoryId && budgetMap.has(categoryId)
          ? Number(budgetMap.get(categoryId)) / 100
          : 0;

      const gap_to_budget =
        forecast.forecast != null ? forecast.forecast - budgeted : null;

      return {
        ...forecast,
        last_month: match ? match.lag_1 : null,
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

export const app = createApp<TransactionHandlers>();

app.method(
  'transactions-batch-update',
  mutator(undoable(handleBatchUpdateTransactions)),
);
app.method('transactions-merge', mutator(undoable(mergeTransactions)));

app.method('transaction-add', mutator(addTransaction));
app.method('transaction-update', mutator(updateTransaction));
app.method('transaction-delete', mutator(deleteTransaction));
app.method('transaction-move', mutator(undoable(moveTransaction)));
app.method('transactions-parse-file', mutator(parseTransactionsFile));
app.method('transactions-export', mutator(exportTransactions));
app.method('transactions-export-query', mutator(exportTransactionsQuery));
app.method('get-earliest-transaction', getEarliestTransaction);
app.method('get-latest-transaction', getLatestTransaction);
app.method('forecast-get-category-predictions', getCategoryPredictions);
app.method('forecast-export-monthly-history', exportForecastMonthlyHistory);
