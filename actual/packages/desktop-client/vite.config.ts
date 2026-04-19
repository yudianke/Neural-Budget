import * as fs from 'node:fs';
import * as nodePath from 'node:path';
import * as path from 'path';
import { fileURLToPath } from 'url';

import babel from '@rolldown/plugin-babel';
import inject from '@rollup/plugin-inject';
import basicSsl from '@vitejs/plugin-basic-ssl';
import react, { reactCompilerPreset } from '@vitejs/plugin-react';
import type { PreRenderedAsset } from 'rolldown';
import { visualizer } from 'rollup-plugin-visualizer';
/// <reference types="vitest" />
import { defineConfig, loadEnv } from 'vite';
import type { Plugin } from 'vite';
import { VitePWA } from 'vite-plugin-pwa';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const reactCompilerInclude = new RegExp(
  `^${path
    .resolve(__dirname, 'src')
    .replaceAll(path.sep, '/')
    .replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}/.*\\.[jt]sx$`,
);

const addWatchers = (): Plugin => ({
  name: 'add-watchers',
  configureServer(server) {
    server.watcher
      .add([
        path.resolve('../loot-core/lib-dist/electron/*.js'),
        path.resolve('../loot-core/lib-dist/browser/*.js'),
      ])
      .on('all', function () {
        for (const wsc of server.ws.clients) {
          wsc.send(JSON.stringify({ type: 'static-changed' }));
        }
      });
  },
});

/**
 * Vite middleware plugin: GET /api/ml/actuals?months=N
 *
 * Reads budget SQLite files from ACTUAL_USER_FILES (default /data/user-files)
 * and returns real monthly spend per category aggregated across all budgets.
 *
 * Used by the M3 retrain daemon (m3-monitor-daemon) to compute production MAE
 * and decide whether to roll back a newly deployed model.
 *
 * Response shape:
 *   { "2024-01": { "groceries": 142.30, "restaurants": 31.00, ... }, ... }
 */
const mlActualsPlugin = (): Plugin => ({
  name: 'ml-actuals',
  configureServer(server) {
    server.middlewares.use('/api/ml/actuals', (req, res) => {
      try {
        // Dynamic require so Vite doesn't try to bundle better-sqlite3
        // eslint-disable-next-line @typescript-eslint/no-require-imports
        const Database = require('better-sqlite3');
        const url = new URL(req.url || '/', 'http://localhost');
        const months = Math.min(
          Math.max(parseInt(url.searchParams.get('months') || '12', 10), 1),
          36,
        );

        const userFilesDir =
          process.env.ACTUAL_USER_FILES ||
          (fs.existsSync('/data/user-files') ? '/data/user-files' : null);

        if (!userFilesDir || !fs.existsSync(userFilesDir)) {
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({}));
          return;
        }

        // Cutoff date: ActualBudget stores dates as YYYYMMDD integers
        const cutoffDate = new Date();
        cutoffDate.setMonth(cutoffDate.getMonth() - months);
        const cutoffInt = parseInt(
          `${cutoffDate.getFullYear()}` +
            String(cutoffDate.getMonth() + 1).padStart(2, '0') +
            '01',
          10,
        );

        const result: Record<string, Record<string, number>> = {};

        const budgetDirs = fs.readdirSync(userFilesDir).filter(entry => {
          const dbPath = nodePath.join(userFilesDir, entry, 'db.sqlite');
          return fs.existsSync(dbPath);
        });

        for (const budgetDir of budgetDirs) {
          const dbPath = nodePath.join(userFilesDir, budgetDir, 'db.sqlite');
          let db: InstanceType<typeof Database> | null = null;
          try {
            db = new Database(dbPath, { readonly: true, fileMustExist: true });
            const rows: Array<{
              year_month: string;
              category_name: string;
              total_dollars: number;
            }> = db
              .prepare(
                `SELECT
                  substr(CAST(t.date AS TEXT), 1, 4) || '-' ||
                  substr(CAST(t.date AS TEXT), 5, 2) AS year_month,
                  c.name                              AS category_name,
                  SUM(ABS(t.amount)) / 100.0          AS total_dollars
                FROM transactions t
                JOIN categories c ON c.id = t.category
                WHERE t.tombstone = 0
                  AND t.amount < 0
                  AND t.date >= ?
                  AND t.category IS NOT NULL
                  AND c.tombstone = 0
                GROUP BY year_month, c.name
                ORDER BY year_month, c.name`,
              )
              .all(cutoffInt);

            for (const { year_month, category_name, total_dollars } of rows) {
              const catKey = category_name.toLowerCase().replace(/\s+/g, '_');
              if (!result[year_month]) result[year_month] = {};
              result[year_month][catKey] =
                Math.round(((result[year_month][catKey] ?? 0) + total_dollars) * 100) /
                100;
            }
          } catch (dbErr) {
            console.warn(`[ml/actuals] skipping ${budgetDir}:`, dbErr);
          } finally {
            db?.close();
          }
        }

        res.setHeader('Content-Type', 'application/json');
        res.end(JSON.stringify(result));
      } catch (err) {
        console.error('[ml/actuals] error:', err);
        res.statusCode = 500;
        res.end(JSON.stringify({ error: 'failed to read actuals' }));
      }
    });
  },
});

const injectPlugin = (options?: Parameters<typeof inject>[0]): Plugin => {
  // Rollup plugins are currently slightly API-incompatible with Rolldown plugins, but not in a way that prevents them from working here.
  return inject(options) as unknown as Plugin;
};

// Inject build shims using the inject plugin
const injectShims = (): Plugin[] => {
  const buildShims = path.resolve('./src/build-shims.js');
  const serveInject: {
    exclude: string[];
    global: [string, string];
  } = {
    exclude: ['src/setupTests.ts'],
    global: [buildShims, 'global'],
  };
  const buildInject: {
    global: [string, string];
  } = {
    global: [buildShims, 'global'],
  };

  return [
    {
      name: 'define-build-process',
      config: () => ({
        // rename process.env in build mode so it doesn't get set to an empty object up by the vite:define plugin
        // this isn't needed in serve mode, because vite:define doesn't empty it in serve mode. And defines also happen last anyways in serve mode.
        environments: {
          client: {
            define: {
              'process.env': '_process.env',
            },
          },
        },
      }),
      apply: 'build',
    },
    {
      enforce: 'post',
      apply: 'serve',
      ...injectPlugin({
        ...serveInject,
        process: [buildShims, 'process'],
      }),
    },
    {
      name: 'inject-build-process',
      enforce: 'post',
      apply: 'build',
      config: () => ({
        build: {
          rolldownOptions: {
            transform: {
              inject: {
                ...buildInject,
                _process: [buildShims, 'process'],
              },
            },
          },
        },
      }),
    },
  ];
};

// https://vitejs.dev/config/

export default defineConfig(async ({ mode }) => {
  const env = loadEnv(mode, process.cwd(), '');
  const devHeaders = {
    'Cross-Origin-Opener-Policy': 'same-origin',
    'Cross-Origin-Embedder-Policy': 'require-corp',
  };

  // Forward Netlify env variables
  if (process.env.REVIEW_ID) {
    process.env.REACT_APP_REVIEW_ID = process.env.REVIEW_ID;
    process.env.REACT_APP_BRANCH = process.env.BRANCH;
  }

  const browserOpen = env.BROWSER_OPEN ? `//${env.BROWSER_OPEN}` : true;

  return {
    base: '/',
    envPrefix: 'REACT_APP_',
    build: {
      terserOptions: {
        compress: false,
        mangle: false,
      },
      target: 'es2022',
      sourcemap: true,
      outDir: mode === 'desktop' ? 'build-electron' : 'build',
      assetsDir: 'static',
      manifest: true,
      assetsInlineLimit: 0,
      chunkSizeWarningLimit: 1500,
      rolldownOptions: {
        output: {
          assetFileNames: (assetInfo: PreRenderedAsset) => {
            const info = assetInfo.name?.split('.') ?? [];
            let extType = info[info.length - 1];
            if (/png|jpe?g|svg|gif|tiff|bmp|ico/i.test(extType)) {
              extType = 'img';
            } else if (/woff|woff2/.test(extType)) {
              extType = 'media';
            }
            return `static/${extType}/[name].[hash][extname]`;
          },
          chunkFileNames: 'static/js/[name].[hash].chunk.js',
          entryFileNames: 'static/js/[name].[hash].js',
        },
      },
    },
    server: {
      host: true,
      headers: mode === 'development' ? devHeaders : undefined,
      port: +env.PORT || 5173,
      allowedHosts: ['actual-development', 'localhost', '127.0.0.1'],
      open: env.BROWSER
        ? ['chrome', 'firefox', 'edge', 'browser', 'browserPrivate'].includes(
            env.BROWSER,
          )
        : browserOpen,
      watch: {
        disableGlobbing: false,
      },
      proxy: {
        '/ml-api': {
          target: env.M1_SERVICE_URL || 'http://129.114.26.3:8001',
          changeOrigin: true,
          rewrite: (p: string) => p.replace(/^\/ml-api/, ''),
        },
        '/m3-api': {
          target: env.M3_SERVICE_URL || 'http://localhost:8002',
          changeOrigin: true,
          rewrite: (p: string) => p.replace(/^\/m3-api/, ''),
        },
      },
    },
    resolve: {
      ...(!env.IS_GENERIC_BROWSER && {
        conditions: ['electron-renderer', 'module', 'browser', 'default'],
      }),
      tsconfigPaths: true,
    },
    plugins: [
      // Disable PWA during local dev. The dev service worker causes Vite to
      // re-optimize workbox deps and occasionally request stale .vite chunks,
      // which leaves the Docker-based browser client on a blank page.
      mode === 'desktop' || mode === 'development'
        ? undefined
        : VitePWA({
            registerType: 'prompt',
            // TODO:  The plugin worker build is currently disabled due to issues with offline support. Fix this
            // strategies: 'injectManifest',
            // srcDir: 'service-worker',
            // filename: 'plugin-sw.js',
            // manifest: {
            //   name: 'Actual',
            //   short_name: 'Actual',
            //   description: 'A local-first personal finance tool',
            //   theme_color: '#5c3dbb',
            //   background_color: '#5c3dbb',
            //   display: 'standalone',
            //   start_url: './',
            // },
            // injectManifest: {
            //   maximumFileSizeToCacheInBytes: 10 * 1024 * 1024, // 10MB
            //   swSrc: `service-worker/plugin-sw.js`,
            // },
            devOptions: {
              enabled: false,
              type: 'module',
            },
            workbox: {
              globPatterns: [
                '**/*.{js,css,html,txt,wasm,sql,sqlite,ico,png,woff2,webmanifest}',
              ],
              ignoreURLParametersMatching: [/^v$/],
              navigateFallback: '/index.html',
              maximumFileSizeToCacheInBytes: 10 * 1024 * 1024, // 10MB
              navigateFallbackDenylist: [
                /^\/account\/.*$/,
                /^\/admin\/.*$/,
                /^\/secret\/.*$/,
                /^\/openid\/.*$/,
                /^\/plugins\/.*$/,
                /^\/kcab\/.*$/,
                /^\/plugin-data\/.*$/,
              ],
            },
          }),
      injectShims(),
      addWatchers(),
      mlActualsPlugin(),
      react(),
      babel({
        include: [reactCompilerInclude],
        // n.b. Must be a string to ensure plugin resolution order. See https://github.com/actualbudget/actual/pull/5853
        presets: [reactCompilerPreset()],
      }),
      visualizer({ template: 'raw-data' }),
      !!env.HTTPS && basicSsl(),
    ],
    test: {
      include: ['src/**/*.{test,spec}.?(c|m)[jt]s?(x)'],
      environment: 'jsdom',
      globals: true,
      setupFiles: './src/setupTests.ts',
      testTimeout: 10000,
      onConsoleLog(log: string, type: 'stdout' | 'stderr'): boolean | void {
        // print only console.error
        return type === 'stderr';
      },
      maxWorkers: 2,
    },
  };
});
