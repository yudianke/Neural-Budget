import {useEffect, useMemo, useState} from 'react';

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

  useEffect(() => {
    async function run() {
      try {
        setError(null);
        console.log('UI calling forecast-get-category-predictions');
        const json = await send('forecast-get-category-predictions');
        setData(json);
      } catch (err) {
        console.error('ForecastCard failed FULL:', err);
        setError(err instanceof Error ? err.message : JSON.stringify(err));
      }
    }

    void run();
  }, []);
  const sortedForecasts = useMemo(() => {
    if (!data?.forecasts) return [];
    return [...data.forecasts].sort(
      (a, b) => (b.gap_to_budget ?? Number.NEGATIVE_INFINITY) - (a.gap_to_budget ?? Number.NEGATIVE_INFINITY)
    );
  }, [data]);

  const topForecasts = useMemo(() => {
    return sortedForecasts.slice(0, 6);
  }, [sortedForecasts]);

  const maxForecast = useMemo(() => {
    if (!topForecasts.length) return 0;
    return Math.max(...topForecasts.map(item => item.forecast ?? 0));
  }, [topForecasts]);

  const total = useMemo(() => {
    return topForecasts.reduce(
      (sum, x) => sum + (x.forecast ?? 0),
      0
    );
  }, [topForecasts]);

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
        <Text style={{fontSize: 18, fontWeight: 600}}>
          Personalized Forecast
        </Text>

        <Text style={{fontSize: 14, opacity: 0.7}}>
          Forecasted spend vs current budget by category
        </Text>

        {error ? (
          <Text>{error}</Text>
        ) : data ? (
          <View style={{marginTop: 8, gap: 10}}>
            {/* Insight line */}
            {topCategory && (
              <Text style={{opacity: 0.7}}>
                Highest budget risk: {topCategory.category} (
                {topCategory.gap_to_budget != null
                  ? `${topCategory.gap_to_budget >= 0 ? '+' : ''}$${topCategory.gap_to_budget.toFixed(0)} vs budget`
                  : 'No budget comparison'}
                )
              </Text>
            )}

            {/* Bars */}
            {topForecasts.map((item, idx) => {

              const value = item.forecast ?? 0;
              const widthPct =
                maxForecast > 0
                  ? Math.max((value / maxForecast) * 100, 2)
                  : 0;

              const isTop = idx === 0;
              const delta =
                item.forecast != null && item.last_month != null
                  ? item.forecast - item.last_month
                  : null;

              const deltaPct = delta != null && item.last_month
                ? (delta / item.last_month) * 100
                : null;

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

                    <View style={{flexDirection: 'row', gap: 8}}>
                      <Text>
                        {item.forecast != null
                          ? `$${item.forecast.toFixed(0)}`
                          : 'N/A'}
                      </Text>

                      {item.gap_to_budget != null && (
                        <Text
                          style={{
                            color: item.gap_to_budget > 0 ? '#ff7b7b' : '#7bffb0',
                            fontWeight: 500,
                          }}
                        >
                          {item.gap_to_budget > 0 ? '+' : ''}${item.gap_to_budget.toFixed(0)}
                        </Text>
                      )}
                    </View>
                  </View>

                  {/* Bar background */}
                  <View
                    style={{
                      height: 12,
                      borderRadius: 999,
                      backgroundColor: 'rgba(255,255,255,0.08)',
                      overflow: 'hidden',
                    }}
                  >
                    {/* Filled bar */}
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
              Model: {data.model_name}
            </Text>
          </View>
        ) : (
          <Text>Loading...</Text>
        )}
      </View>
    </ReportCard>
  );
}
