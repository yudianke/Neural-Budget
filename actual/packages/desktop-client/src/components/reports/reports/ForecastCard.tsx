import {useCallback, useEffect, useMemo, useState} from 'react';

import {Trans, useTranslation} from 'react-i18next';

import {Button} from '@actual-app/components/button';
import {styles} from '@actual-app/components/styles';
import {theme} from '@actual-app/components/theme';
import {Text} from '@actual-app/components/text';
import {View} from '@actual-app/components/view';
import {send} from '@actual-app/core/platform/client/connection';

import {LoadingIndicator} from '#components/reports/LoadingIndicator';
import {ReportCard} from '#components/reports/ReportCard';

type ForecastCardProps = {
  widgetId: string;
  isEditing?: boolean;
  meta?: any;
  onMetaChange?: (meta: any) => void;
  onRemove: () => void;
  onCopy: (targetDashboardId: string) => void;
};

type CategoryForecast = {
  category: string;
  forecast: number | null;
  last_month?: number | null;
  budgeted?: number | null;
  gap_to_budget?: number | null;
};

type ForecastResponse = {
  forecasts: CategoryForecast[];
  model_name: string;
};

function fmt(n: number): string {
  return n.toLocaleString('en-US', {
    style: 'currency',
    currency: 'USD',
    maximumFractionDigits: 0,
  });
}

/** A single category row with a comparison bar */
function CategoryRow({
  item,
  scale,
  isTop,
}: {
  item: CategoryForecast;
  scale: number; // px-per-dollar for bar widths
  isTop: boolean;
}) {
  const forecast = item.forecast ?? 0;
  const budgeted = item.budgeted ?? 0;
  const lastMonth = item.last_month ?? 0;
  const gap = item.gap_to_budget;

  // Bar widths — cap at 100% of the container
  const forecastPct = Math.min((forecast / (scale || 1)) * 100, 100);
  const budgetedPct = Math.min((budgeted / (scale || 1)) * 100, 100);
  const lastPct = Math.min((lastMonth / (scale || 1)) * 100, 100);

  const hasBudget = budgeted > 0;
  const overBudget = hasBudget && gap != null && gap > 0;
  const underBudget = hasBudget && gap != null && gap < 0;

  // Only color red/green when there is an actual budget target to compare against.
  // Without a budget, use neutral blue — no false alarms.
  const barColor = hasBudget
    ? overBudget
      ? theme.reportsRed
      : theme.reportsGreen
    : theme.reportsBlue;

  const gapColor = overBudget
    ? theme.reportsNumberNegative
    : underBudget
      ? theme.reportsNumberPositive
      : theme.reportsNumberNeutral;

  return (
    <View style={{gap: 5}}>
      {/* Label row */}
      <View
        style={{
          flexDirection: 'row',
          justifyContent: 'space-between',
          alignItems: 'baseline',
        }}
      >
        <Text
          style={{
            fontSize: 13,
            fontWeight: isTop ? 600 : 400,
            color: theme.tableText,
            flexShrink: 1,
            marginRight: 8,
          }}
        >
          {item.category}
        </Text>

        <View style={{flexDirection: 'row', gap: 10, alignItems: 'baseline'}}>
          {/* Budget — explicit numeric reference */}
          <Text
            style={{
              fontSize: 11,
              color: theme.reportsBlue,
              fontVariantNumeric: 'tabular-nums',
            }}
          >
            Budget {fmt(budgeted)}
          </Text>

          {/* Last month — muted reference */}
          {lastMonth > 0 && (
            <Text
              style={{
                fontSize: 11,
                color: theme.pageTextSubdued,
                fontVariantNumeric: 'tabular-nums',
              }}
            >
              Last month {fmt(lastMonth)}
            </Text>
          )}

          {/* Forecast — primary number */}
          <Text
            style={{
              fontSize: 13,
              fontWeight: 600,
              color: theme.tableText,
              fontVariantNumeric: 'tabular-nums',
            }}
          >
            Forecast {item.forecast != null ? fmt(forecast) : '—'}
          </Text>

          {/* Gap chip */}
          {gap != null && (
            <Text
              style={{
                fontSize: 11,
                fontWeight: 500,
                color: gapColor,
                fontVariantNumeric: 'tabular-nums',
                minWidth: 48,
                textAlign: 'right',
              }}
            >
              {gap > 0 ? '+' : ''}{fmt(gap)}
            </Text>
          )}
        </View>
      </View>

      {/* Comparison bar */}
      <View
        style={{
          position: 'relative',
          height: 8,
          borderRadius: 4,
          backgroundColor: theme.tableBorder,
          overflow: 'visible',
        }}
      >
        {/* Budget target marker (dotted line) */}
        {budgeted > 0 && budgetedPct > 0 && (
          <View
            style={{
              position: 'absolute',
              left: `${budgetedPct}%`,
              top: -2,
              bottom: -2,
              width: 2,
              borderRadius: 1,
              backgroundColor: theme.reportsBlue,
              opacity: 0.7,
            }}
          />
        )}

        {/* Last month marker (subtle tick) */}
        {lastMonth > 0 && lastPct > 0 && (
          <View
            style={{
              position: 'absolute',
              left: `${lastPct}%`,
              top: 0,
              bottom: 0,
              width: 1,
              backgroundColor: theme.pageTextSubdued,
              opacity: 0.5,
            }}
          />
        )}

        {/* Forecast fill */}
        <View
          style={{
            position: 'absolute',
            left: 0,
            top: 0,
            bottom: 0,
            width: `${Math.max(forecastPct, forecastPct > 0 ? 2 : 0)}%`,
            borderRadius: 4,
            backgroundColor: barColor,
            opacity: 0.85,
          }}
        />
      </View>
    </View>
  );
}

export function ForecastCard({
  isEditing,
  onRemove,
  onCopy,
}: ForecastCardProps) {
  const {t} = useTranslation();
  const [data, setData] = useState<ForecastResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [applying, setApplying] = useState(false);
  const [applyResult, setApplyResult] = useState<{applied: number; month: string} | null>(null);

  async function load() {
    setLoading(true);
    setError(null);
    setApplyResult(null);
    try {
      const json = await send('forecast-get-category-predictions');
      setData(json);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setLoading(false);
    }
  }

  useEffect(
    () => {
      void load();
    },
    // load is a stable local function defined in the same component scope;
    // we only want to fetch on mount, not on every render.
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [],
  );

  // Sort by gap descending (highest over-budget risk first)
  const topForecasts = data?.forecasts
    ? [...data.forecasts]
        .sort(
          (a, b) =>
            (b.gap_to_budget ?? Number.NEGATIVE_INFINITY) -
            (a.gap_to_budget ?? Number.NEGATIVE_INFINITY),
        )
        .slice(0, 6)
    : [];

  // Total forecast across all categories (not just top-6)
  const totalForecast = data?.forecasts
    ? data.forecasts.reduce((acc, f) => acc + (f.forecast ?? 0), 0) || null
    : null;

  // Scale = the max value among forecast + budgeted + last_month across rows
  const barScale = topForecasts.length
    ? Math.max(
        ...topForecasts.flatMap(f => [
          f.forecast ?? 0,
          f.budgeted ?? 0,
          f.last_month ?? 0,
        ]),
        1,
      )
    : 1;

  // True when every visible category has no budget set yet
  const noBudgetsSet =
    topForecasts.length > 0 &&
    topForecasts.every(f => !f.budgeted || f.budgeted === 0);

  const now = new Date();
  const nowLabel = now.toLocaleString('default', {month: 'long', year: 'numeric'});
  const nextMonthDate = new Date(now.getFullYear(), now.getMonth() + 1, 1);
  const nextMonthLabel = nextMonthDate.toLocaleString('default', {month: 'long', year: 'numeric'});

  async function handleApplyAsBudgets() {
    if (!data?.forecasts) return;
    setApplying(true);
    setError(null);
    try {
      const entries = data.forecasts
        .filter(f => f.forecast != null && f.forecast > 0)
        .map(f => ({categoryName: f.category, amount: f.forecast as number}));

      const result = await send('forecast-apply-as-budgets', {entries});
      setApplyResult({applied: result.applied, month: result.month});
      // Reload so gap chips reflect the newly written budgets
      await load();
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setApplying(false);
    }
  }

  return (
    <ReportCard
      isEditing={isEditing}
      menuItems={[
        {name: 'remove', text: t('Remove')},
        {name: 'copy', text: t('Copy')},
      ]}
      onMenuSelect={item => {
        if (item === 'remove') onRemove();
        if (item === 'copy') onCopy('');
      }}
    >
      <View style={{flex: 1}}>
        {/* Header */}
        <View
          style={{
            flexDirection: 'row',
            justifyContent: 'space-between',
            alignItems: 'flex-start',
            padding: 20,
            paddingBottom: 12,
          }}
        >
          <View style={{flex: 1}}>
            <Text
              style={{
                ...styles.mediumText,
                fontWeight: 500,
                color: theme.tableText,
              }}
            >
              <Trans>Forecast</Trans>
            </Text>
            <Text style={{fontSize: 12, color: theme.pageTextSubdued, marginTop: 2}}>
              {nowLabel}
            </Text>
          </View>

          <View style={{alignItems: 'flex-end', gap: 4}}>
            {totalForecast != null && (
              <Text
                style={{
                  ...styles.mediumText,
                  fontWeight: 500,
                  color: theme.tableText,
                }}
              >
                {fmt(totalForecast)}
              </Text>
            )}
            <Button
              variant="bare"
              onPress={() => void load()}
              style={{
                padding: '2px 6px',
                fontSize: 11,
                color: theme.pageTextSubdued,
                marginTop: 2,
              }}
            >
              {loading ? <Trans>Loading…</Trans> : <Trans>Refresh</Trans>}
            </Button>
          </View>
        </View>

        {/* Body */}
        {error ? (
          <View style={{padding: '0 20px 20px'}}>
            <Text style={{fontSize: 13, color: theme.reportsRed}}>{error}</Text>
          </View>
        ) : loading && !data ? (
          <LoadingIndicator />
        ) : data && topForecasts.length > 0 ? (
          <View style={{flex: 1, padding: '0 20px 16px', gap: 12}}>
            {/* Success confirmation banner */}
            {applyResult && (
              <View
                style={{
                  backgroundColor: theme.noticeBackground,
                  borderRadius: 6,
                  padding: '8px 12px',
                }}
              >
                <Text style={{fontSize: 12, color: theme.noticeText}}>
                  <Trans>
                    Set {applyResult.applied} budget{applyResult.applied !== 1 ? 's' : ''} for{' '}
                    {new Date(applyResult.month + '-02').toLocaleString('default', {
                      month: 'long',
                      year: 'numeric',
                    })}
                  </Trans>
                </Text>
              </View>
            )}

            {/* "No budgets set" banner with apply button */}
            {noBudgetsSet && !applyResult && (
              <View
                style={{
                  flexDirection: 'row',
                  justifyContent: 'space-between',
                  alignItems: 'center',
                  backgroundColor: theme.upcomingBackground,
                  borderRadius: 6,
                  padding: '8px 12px',
                  gap: 8,
                }}
              >
                <Text
                  style={{
                    fontSize: 12,
                    color: theme.upcomingText,
                    flex: 1,
                  }}
                >
                  <Trans>No budgets set for {nextMonthLabel}</Trans>
                </Text>
                <Button
                  variant="bare"
                  onPress={() => void handleApplyAsBudgets()}
                  style={{
                    fontSize: 12,
                    fontWeight: 600,
                    color: theme.upcomingText,
                    padding: '3px 8px',
                    borderRadius: 4,
                    border: `1px solid ${theme.upcomingBorder}`,
                    flexShrink: 0,
                  }}
                >
                  {applying
                    ? <Trans>Applying…</Trans>
                    : <Trans>Use forecasts as budgets</Trans>}
                </Button>
              </View>
            )}

            {/* Legend */}
            <View style={{flexDirection: 'row', gap: 14, alignItems: 'center'}}>
              <LegendItem color={theme.reportsGreen} label={t('Forecast')} />
              <LegendItem color={theme.reportsBlue} label={t('Budget')} bar />
              <LegendItem color={theme.pageTextSubdued} label={t('Last mo.')} bar thin />
            </View>

            {/* Category rows */}
            {topForecasts.map((item, idx) => (
              <CategoryRow
                key={item.category}
                item={item}
                scale={barScale}
                isTop={idx === 0}
              />
            ))}

            {/* Footer */}
            <Text style={{fontSize: 11, color: theme.pageTextSubdued, marginTop: 4}}>
              {data.model_name}
            </Text>
          </View>
        ) : data && topForecasts.length === 0 ? (
          <View
            style={{
              flex: 1,
              justifyContent: 'center',
              alignItems: 'center',
              padding: 20,
            }}
          >
            <Text style={{fontSize: 13, color: theme.pageTextSubdued, textAlign: 'center'}}>
              <Trans>
                Not enough transaction history to generate forecasts. Add at least 3 months of
                spending data to get started.
              </Trans>
            </Text>
          </View>
        ) : null}
      </View>
    </ReportCard>
  );
}

function LegendItem({
  color,
  label,
  bar,
  thin,
}: {
  color: string;
  label: string;
  bar?: boolean;
  thin?: boolean;
}) {
  return (
    <View style={{flexDirection: 'row', gap: 4, alignItems: 'center'}}>
      {bar ? (
        <View
          style={{
            width: thin ? 1 : 2,
            height: 10,
            borderRadius: 1,
            backgroundColor: color,
            opacity: thin ? 0.5 : 0.7,
          }}
        />
      ) : (
        <View
          style={{
            width: 10,
            height: 6,
            borderRadius: 3,
            backgroundColor: color,
            opacity: 0.85,
          }}
        />
      )}
      <Text style={{fontSize: 11, color: theme.pageTextSubdued}}>{label}</Text>
    </View>
  );
}
