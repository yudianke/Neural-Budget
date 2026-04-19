import {useCallback, useEffect, useMemo, useState} from 'react';

import {Trans} from 'react-i18next';

import {Button} from '@actual-app/components/button';
import {Text} from '@actual-app/components/text';
import {View} from '@actual-app/components/view';
import {send} from '@actual-app/core/platform/client/connection';

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

export function ForecastCard({
  isEditing,
  onRemove,
  onCopy,
}: ForecastCardProps) {
  const [data, setData] = useState<ForecastResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const json = await send('forecast-get-category-predictions');
      setData(json);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void load();
  }, [load]);

  const sortedForecasts = useMemo(() => {
    if (!data?.forecasts) return [];
    return [...data.forecasts].sort(
      (a, b) =>
        (b.gap_to_budget ?? Number.NEGATIVE_INFINITY) -
        (a.gap_to_budget ?? Number.NEGATIVE_INFINITY),
    );
  }, [data]);

  const topForecasts = useMemo(() => sortedForecasts.slice(0, 6), [sortedForecasts]);

  const maxForecast = useMemo(
    () => (topForecasts.length ? Math.max(...topForecasts.map(item => item.forecast ?? 0)) : 0),
    [topForecasts],
  );

  const topCategory = topForecasts[0];

  return (
    <ReportCard
      isEditing={isEditing}
      menuItems={[
        {name: 'remove', text: 'Remove'},
        {name: 'copy', text: 'Copy'},
      ]}
      onMenuSelect={item => {
        if (item === 'remove') onRemove();
        if (item === 'copy') onCopy('');
      }}
    >
      <View style={{flex: 1, padding: 10, gap: 8}}>
        <View
          style={{
            flexDirection: 'row',
            justifyContent: 'space-between',
            alignItems: 'center',
          }}
        >
          <Text style={{fontSize: 18, fontWeight: 600}}>
            <Trans>Personalized Forecast</Trans>
          </Text>

          <Button
            variant="bare"
            onPress={() => void load()}
            style={{padding: '2px 6px', fontSize: 12, opacity: 0.7}}
          >
            {loading ? <Trans>Loading...</Trans> : <Trans>Refresh</Trans>}
          </Button>
        </View>

        <Text style={{fontSize: 14, opacity: 0.7}}>
          <Trans>Forecasted spend vs current budget by category</Trans>
        </Text>

        {error ? (
          <Text style={{color: '#ff7b7b'}}>{error}</Text>
        ) : loading && !data ? (
          // Skeleton placeholder while first load is in flight
          <View style={{marginTop: 8, gap: 10}}>
            {[1, 2, 3, 4].map(i => (
              <View key={i} style={{gap: 4}}>
                <View
                  style={{
                    height: 14,
                    borderRadius: 4,
                    backgroundColor: 'rgba(255,255,255,0.08)',
                    width: `${50 + i * 10}%`,
                  }}
                />
                <View
                  style={{
                    height: 12,
                    borderRadius: 999,
                    backgroundColor: 'rgba(255,255,255,0.05)',
                  }}
                />
              </View>
            ))}
          </View>
        ) : data ? (
          <View style={{marginTop: 8, gap: 10}}>
            {topCategory && (
              <Text style={{opacity: 0.7}}>
                <Trans>Highest budget risk:</Trans>{' '}
                {topCategory.category}
                {topCategory.gap_to_budget != null
                  ? ` (${topCategory.gap_to_budget >= 0 ? '+' : ''}$${topCategory.gap_to_budget.toFixed(0)} vs budget)`
                  : ''}
              </Text>
            )}

            {topForecasts.map((item, idx) => {
              const value = item.forecast ?? 0;
              const widthPct =
                maxForecast > 0 ? Math.max((value / maxForecast) * 100, 2) : 0;
              const isTop = idx === 0;

              return (
                <View key={item.category} style={{gap: 4}}>
                  <View
                    style={{
                      flexDirection: 'row',
                      justifyContent: 'space-between',
                    }}
                  >
                    <Text style={{fontWeight: isTop ? 600 : 400}}>
                      {item.category}
                    </Text>

                    <View style={{flexDirection: 'row', gap: 8, alignItems: 'center'}}>
                      {/* Last month actual — shown in muted text when available */}
                      {item.last_month != null && item.last_month > 0 && (
                        <Text style={{opacity: 0.5, fontSize: 12}}>
                          <Trans>last</Trans>{' '}${item.last_month.toFixed(0)}
                        </Text>
                      )}

                      <Text>
                        {item.forecast != null
                          ? `$${item.forecast.toFixed(0)}`
                          : 'N/A'}
                      </Text>

                      {item.gap_to_budget != null && (
                        <Text
                          style={{
                            color:
                              item.gap_to_budget > 0 ? '#ff7b7b' : '#7bffb0',
                            fontWeight: 500,
                          }}
                        >
                          {item.gap_to_budget > 0 ? '+' : ''}$
                          {item.gap_to_budget.toFixed(0)}
                        </Text>
                      )}
                    </View>
                  </View>

                  <View
                    style={{
                      height: 12,
                      borderRadius: 999,
                      backgroundColor: 'rgba(255,255,255,0.08)',
                      overflow: 'hidden',
                    }}
                  >
                    <View
                      style={{
                        width: `${widthPct}%`,
                        height: '100%',
                        borderRadius: 999,
                        backgroundColor: isTop
                          ? 'rgba(255, 180, 80, 0.95)'
                          : 'rgba(116, 255, 200, 0.85)',
                      }}
                    />
                  </View>
                </View>
              );
            })}

            <Text style={{marginTop: 10, opacity: 0.6}}>
              <Trans>Model:</Trans> {data.model_name}
            </Text>
          </View>
        ) : null}
      </View>
    </ReportCard>
  );
}
