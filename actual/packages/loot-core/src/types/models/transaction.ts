import type { IntegerAmount } from '#shared/util';

import type { AccountEntity } from './account';
import type { CategoryEntity } from './category';
import type { PayeeEntity } from './payee';
import type { ScheduleEntity } from './schedule';

export type TransactionEntity = {
  id: string;
  is_parent?: boolean;
  is_child?: boolean;
  parent_id?: TransactionEntity['id'];
  account: AccountEntity['id'];
  category?: CategoryEntity['id'];
  amount: IntegerAmount;
  payee?: PayeeEntity['id'] | null;
  notes?: string;
  date: string;
  imported_id?: string;
  imported_payee?: string;
  starting_balance_flag?: boolean;
  transfer_id?: TransactionEntity['id'];
  sort_order?: number;
  cleared?: boolean;
  reconciled?: boolean;
  tombstone?: boolean;
  forceUpcoming?: boolean;
  schedule?: ScheduleEntity['id'];
  subtransactions?: TransactionEntity[];
  _unmatched?: boolean;
  _deleted?: boolean;
  error?: {
    type: 'SplitTransactionError';
    version: 1;
    difference: number;
  } | null;
  raw_synced_data?: string | undefined;
  _ruleErrors?: string[];
  // M2 anomaly detection fields (added by migration 1776000000001)
  anomaly_score?: number | null;
  anomaly_flags?: string | null;  // JSON: {duplicate_within_24h, subscription_jump, amount_spike}
  anomaly_dismissed?: number | null;  // 0 = active, 1 = dismissed
};
