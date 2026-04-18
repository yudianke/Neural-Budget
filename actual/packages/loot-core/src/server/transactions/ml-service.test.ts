import * as db from '#server/db';

let predictCategoryWithSuggestions: typeof import('./ml-service').predictCategoryWithSuggestions;
let predictCategory: typeof import('./ml-service').predictCategory;
let logM1FeedbackBatch: typeof import('./ml-service').logM1FeedbackBatch;

describe('ml-service', () => {
  const fetchMock = vi.fn();

  beforeAll(async () => {
    ({
      predictCategoryWithSuggestions,
      predictCategory,
      logM1FeedbackBatch,
    } = await import('./ml-service'));
  });

  beforeEach(() => {
    fetchMock.mockReset();
    vi.stubGlobal('fetch', fetchMock);
    process.env.M1_SERVICE_URL = 'http://127.0.0.1:8001';
  });

  afterEach(() => {
    vi.unstubAllGlobals();
    delete process.env.M1_SERVICE_URL;
  });

  test('predictCategoryWithSuggestions returns null when merchant is missing', async () => {
    const result = await predictCategoryWithSuggestions({
      amount: -500,
      date: '2026-04-17',
    });

    expect(result).toBeNull();
    expect(fetchMock).not.toHaveBeenCalled();
  });

  test('predictCategoryWithSuggestions posts the expected request body and resolves top3 ids', async () => {
    await global.emptyDatabase()();
    const groupId = await db.insertCategoryGroup({ name: 'general' });
    await db.insertCategory({
      id: 'groceries-id',
      name: 'Groceries',
      cat_group: groupId,
    });
    await db.insertCategory({
      id: 'general-id',
      name: 'General',
      cat_group: groupId,
    });

    fetchMock.mockResolvedValue(
      new Response(
        JSON.stringify({
          predicted_category: 'Groceries',
          confidence: 0.91,
          top_3_suggestions: [
            { category: 'Groceries', confidence: 0.91 },
            { category: 'General', confidence: 0.07 },
          ],
        }),
        {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        },
      ),
    );

    const result = await predictCategoryWithSuggestions({
      id: 'txn-1',
      payee_name: 'Tesco',
      amount: -4215,
      date: '2026-04-17T12:00:00',
    });

    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, options] = fetchMock.mock.calls[0];
    expect(url).toBe('http://127.0.0.1:8001/predict/category');
    expect(options.method).toBe('POST');
    expect(options.headers).toEqual({ 'Content-Type': 'application/json' });

    const body = JSON.parse(options.body);
    expect(body).toMatchObject({
      transaction_id: 'txn-1',
      synthetic_user_id: 'actual_user',
      date: '2026-04-17T12:00:00',
      merchant: 'Tesco',
      amount: -42.15,
      transaction_type: 'DEB',
      account_type: 'checking',
      day_of_month: 17,
      month: 4,
      historical_majority_category_for_payee: '',
    });
    expect(body.log_abs_amount).toBeCloseTo(Math.log1p(42.15), 10);

    expect(result).toEqual({
      categoryId: 'groceries-id',
      confidence: 0.91,
      top3: [
        {
          category: 'Groceries',
          categoryId: 'groceries-id',
          confidence: 0.91,
        },
        {
          category: 'General',
          categoryId: 'general-id',
          confidence: 0.07,
        },
      ],
    });
  });

  test('predictCategoryWithSuggestions keeps top3 suggestions but suppresses categoryId below threshold', async () => {
    await global.emptyDatabase()();
    const groupId = await db.insertCategoryGroup({ name: 'general' });
    await db.insertCategory({
      id: 'groceries-id',
      name: 'Groceries',
      cat_group: groupId,
    });
    await db.insertCategory({
      id: 'general-id',
      name: 'General',
      cat_group: groupId,
    });

    fetchMock.mockResolvedValue(
      new Response(
        JSON.stringify({
          predicted_category: 'Groceries',
          confidence: 0.42,
          top_3_suggestions: [
            { category: 'Groceries', confidence: 0.42 },
            { category: 'General', confidence: 0.31 },
          ],
        }),
        {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        },
      ),
    );

    const result = await predictCategoryWithSuggestions({
      imported_payee: 'Aldi',
      amount: -1533,
      date: '2026-04-17T12:00:00',
    });

    expect(result).toEqual({
      categoryId: null,
      confidence: 0.42,
      top3: [
        {
          category: 'Groceries',
          categoryId: 'groceries-id',
          confidence: 0.42,
        },
        {
          category: 'General',
          categoryId: 'general-id',
          confidence: 0.31,
        },
      ],
    });
  });

  test('predictCategory returns only the winning category id', async () => {
    await global.emptyDatabase()();
    const groupId = await db.insertCategoryGroup({ name: 'general' });
    await db.insertCategory({
      id: 'groceries-id',
      name: 'Groceries',
      cat_group: groupId,
    });
    await db.insertCategory({
      id: 'general-id',
      name: 'General',
      cat_group: groupId,
    });

    fetchMock.mockResolvedValue(
      new Response(
        JSON.stringify({
          predicted_category: 'Groceries',
          confidence: 0.88,
          top_3_suggestions: [
            { category: 'Groceries', confidence: 0.88 },
            { category: 'General', confidence: 0.08 },
          ],
        }),
        {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        },
      ),
    );

    await expect(
      predictCategory({
        payee_name: 'Tesco',
        amount: -2500,
        date: '2026-04-17T12:00:00',
      }),
    ).resolves.toBe('groceries-id');
  });

  test('predictCategoryWithSuggestions maps model-only shopping labels to local budget categories', async () => {
    await global.emptyDatabase()();
    const groupId = await db.insertCategoryGroup({ name: 'general' });
    await db.insertCategory({
      id: 'shopping-id',
      name: 'Shopping',
      cat_group: groupId,
    });
    await db.insertCategory({
      id: 'general-id',
      name: 'General',
      cat_group: groupId,
    });

    fetchMock.mockResolvedValue(
      new Response(
        JSON.stringify({
          predicted_category: 'Other Shopping',
          confidence: 0.97,
          top_3_suggestions: [
            { category: 'Other Shopping', confidence: 0.97 },
            { category: 'Purchase of uk.eg.org', confidence: 0.02 },
          ],
        }),
        {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        },
      ),
    );

    await expect(
      predictCategoryWithSuggestions({
        payee_name: 'Tesco',
        amount: -4215,
        date: '2026-04-17T12:00:00',
      }),
    ).resolves.toEqual({
      categoryId: 'shopping-id',
      confidence: 0.97,
      top3: [
        {
          category: 'Other Shopping',
          categoryId: 'shopping-id',
          confidence: 0.97,
        },
        {
          category: 'Purchase of uk.eg.org',
          categoryId: 'shopping-id',
          confidence: 0.02,
        },
      ],
    });
  });

  test('logM1FeedbackBatch posts entries to the feedback endpoint', async () => {
    fetchMock.mockResolvedValue(
      new Response(JSON.stringify({ logged: 2 }), {
        status: 200,
        headers: { 'Content-Type': 'application/json' },
      }),
    );

    const entries = [
      {
        transaction_id: 'txn-1',
        date: '2026-04-17',
        amount: -42.15,
        merchant: 'Tesco',
        predicted_category: 'Groceries',
        chosen_category: 'Groceries',
        confidence: 0.91,
        feedback_type: 'accepted' as const,
        top_3_suggestions: [{ category: 'Groceries', confidence: 0.91 }],
        source: 'actual',
      },
      {
        transaction_id: 'txn-2',
        date: '2026-04-17',
        amount: -21.0,
        merchant: 'Aldi',
        predicted_category: 'General',
        chosen_category: 'Groceries',
        confidence: 0.64,
        feedback_type: 'overridden' as const,
        top_3_suggestions: [{ category: 'General', confidence: 0.64 }],
        source: 'actual',
      },
    ];

    await expect(logM1FeedbackBatch(entries)).resolves.toBe(2);
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, options] = fetchMock.mock.calls[0];
    expect(url).toBe('http://127.0.0.1:8001/feedback');
    expect(JSON.parse(options.body)).toEqual({ entries });
  });
});
