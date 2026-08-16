'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const fixtures = require('../fixtures');
const { buildEntityKey } = require('../entity-hierarchy');
const {
  TABLE,
  UPSERT_CONFLICT,
  canonicalToDbRow,
  dbToCanonicalRow,
  SupabaseDatasetRepository
} = require('../supabase-dataset-repository');

let passed = 0;

async function test(name, fn) {
  try {
    await fn();
    passed += 1;
    console.log(`PASS ${String(passed).padStart(2, '0')} - ${name}`);
  } catch (error) {
    console.error(`FAIL - ${name}`);
    throw error;
  }
}

function clone(value) {
  return JSON.parse(JSON.stringify(value));
}

class FakeQuery {
  constructor(client, table) {
    this.client = client;
    this.table = table;
    this.mode = null;
    this.payload = null;
    this.onConflict = null;
    this.filters = [];
    this.orders = [];
  }

  upsert(payload, options = {}) {
    this.mode = 'upsert';
    this.payload = clone(payload);
    this.onConflict = options.onConflict;
    return this;
  }

  select() {
    if (!this.mode) this.mode = 'select';
    return this;
  }

  eq(field, value) { this.filters.push({ op: 'eq', field, value }); return this; }
  gte(field, value) { this.filters.push({ op: 'gte', field, value }); return this; }
  lte(field, value) { this.filters.push({ op: 'lte', field, value }); return this; }
  order(field, { ascending = true } = {}) { this.orders.push({ field, ascending }); return this; }

  async execute() {
    if (this.table !== TABLE) return { data: null, error: { message: `Unknown table ${this.table}` } };

    if (this.mode === 'upsert') {
      this.client.lastUpsertConflict = this.onConflict;
      const keys = String(this.onConflict || '').split(',').filter(Boolean);
      const returned = [];
      for (const incoming of this.payload) {
        const index = this.client.rows.findIndex((row) => keys.every((key) => row[key] === incoming[key]));
        if (index === -1) {
          const now = new Date().toISOString();
          const inserted = {
            id: `fake-${++this.client.idCounter}`,
            created_at: now,
            ...clone(incoming),
            updated_at: incoming.updated_at || now
          };
          this.client.rows.push(inserted);
          returned.push(clone(inserted));
        } else {
          const existing = this.client.rows[index];
          const updated = {
            ...existing,
            ...clone(incoming),
            id: existing.id,
            created_at: existing.created_at
          };
          this.client.rows[index] = updated;
          returned.push(clone(updated));
        }
      }
      return { data: returned, error: null };
    }

    let rows = this.client.rows.map(clone);
    for (const filter of this.filters) {
      rows = rows.filter((row) => {
        if (filter.op === 'eq') return row[filter.field] === filter.value;
        if (filter.op === 'gte') return row[filter.field] >= filter.value;
        if (filter.op === 'lte') return row[filter.field] <= filter.value;
        return true;
      });
    }
    for (let i = this.orders.length - 1; i >= 0; i -= 1) {
      const { field, ascending } = this.orders[i];
      rows.sort((a, b) => {
        if (a[field] === b[field]) return 0;
        const cmp = a[field] < b[field] ? -1 : 1;
        return ascending ? cmp : -cmp;
      });
    }
    return { data: rows, error: null };
  }

  then(resolve, reject) {
    return this.execute().then(resolve, reject);
  }
}

class FakeSupabaseClient {
  constructor() {
    this.rows = [];
    this.idCounter = 0;
    this.lastUpsertConflict = null;
  }
  from(table) { return new FakeQuery(this, table); }
}

function comparableCanonical(row) {
  const copy = clone(row);
  delete copy.entity_key;
  delete copy.canonical_contract_version;
  delete copy.provenance.source_job_id;
  return copy;
}

(async () => {
  const migrationPath = path.resolve(__dirname, '../../supabase/migrations/20260816101220_create_performance_dataset_rows_v2.sql');
  const migration = fs.readFileSync(migrationPath, 'utf8');

  // Static migration boundary
  await test('Migration creates only performance_dataset_rows_v2', () => {
    assert.match(migration, /create table public\.performance_dataset_rows_v2/i);
    assert.doesNotMatch(migration, /alter table public\.performance_dataset_rows\s(?!_v2)/i);
    assert.doesNotMatch(migration, /drop table/i);
  });

  await test('V2 schema does not persist snapshot_id', () => {
    assert.doesNotMatch(migration, /^\s+snapshot_id\s+/mi);
  });

  await test('V2 raw table does not persist derived KPI columns', () => {
    for (const column of ['ctr','cpc','roas','cps','profit','margin','abandoned','abandoned_value','add_to_cart_rate','checkout_rate','abandoned_rate','purchase_rate']) {
      assert.doesNotMatch(migration, new RegExp(`^\\s+${column}\\s+`, 'mi'));
    }
  });

  await test('Migration contains canonical unique UPSERT index', () => {
    assert.match(migration, /performance_dataset_rows_v2_canonical_uidx/i);
    assert.match(migration, /user_id, platform, platform_account_id, business_date, traffic_type, entity_key/i);
  });

  await test('Migration contains exactly the four planned V2 indexes', () => {
    const matches = migration.match(/create\s+(?:unique\s+)?index\s+performance_dataset_rows_v2_/gi) || [];
    assert.equal(matches.length, 4);
  });

  await test('Migration enables RLS and exposes authenticated SELECT-only policy', () => {
    assert.match(migration, /enable row level security/i);
    assert.match(migration, /for select\s+to authenticated/i);
    assert.match(migration, /revoke all on table public\.performance_dataset_rows_v2 from anon, authenticated/i);
    assert.match(migration, /grant select on table public\.performance_dataset_rows_v2 to authenticated/i);
  });

  await test('Corrective migration explicitly rejects NULL Klaviyo Paid channel', () => {
    const fixPath = path.resolve(__dirname, '../../supabase/migrations/20260816101540_fix_v2_klaviyo_channel_constraint.sql');
    const fix = fs.readFileSync(fixPath, 'utf8');
    assert.match(fix, /platform = 'klaviyo'.*channel is not null.*channel in \('email', 'sms'\)/is);
  });

  await test('Migration carries all ten metric_support keys', () => {
    for (const key of ['impression','ad_click','session','spend_value','add_to_cart','add_to_cart_value','checkout','checkout_value','purchase','purchase_value']) {
      assert.ok(migration.includes(`'${key}'`));
    }
  });

  // Canonical -> DB physical mapping
  for (const [name, factory] of [
    ['Meta Paid', fixtures.metaPaid],
    ['Google Standard', fixtures.googleStandard],
    ['Google PMax', fixtures.googlePmax],
    ['TikTok Paid', fixtures.tiktokPaid],
    ['Klaviyo Email Campaign Message', fixtures.klaviyoCampaignEmail],
    ['Klaviyo SMS Flow Message', fixtures.klaviyoFlowSms],
    ['Meta Organic', fixtures.metaOrganic]
  ]) {
    await test(`${name} maps to Dataset V2 physical row and round-trips`, () => {
      const canonical = factory();
      const db = canonicalToDbRow(canonical);
      assert.equal(db.entity_key, buildEntityKey(canonical.identity, canonical.entity));
      assert.equal(db.canonical_contract_version, 'v1');
      const roundTrip = dbToCanonicalRow({ id: 'x', created_at: 'x', ...db });
      assert.deepEqual(comparableCanonical(roundTrip), comparableCanonical(canonical));
    });
  }

  await test('Canonical impression/ad_click/session map to plural physical columns', () => {
    const db = canonicalToDbRow(fixtures.metaPaid());
    assert.equal(db.impressions, 1000);
    assert.equal(db.ad_clicks, 100);
    assert.equal(db.sessions, 0);
    assert.equal(Object.prototype.hasOwnProperty.call(db, 'impression'), false);
    assert.equal(Object.prototype.hasOwnProperty.call(db, 'ad_click'), false);
  });

  await test('Measured zero survives physical mapping as numeric zero', () => {
    const row = fixtures.metaPaid({ metrics: { purchase: 0 } });
    const db = canonicalToDbRow(row);
    assert.equal(db.purchase, 0);
    assert.equal(db.metric_support.purchase, 'supported');
  });

  await test('Unsupported metric survives physical mapping as NULL + unsupported', () => {
    const row = fixtures.metaPaid({ metricSupport: { checkout_value: 'unsupported' } });
    const db = canonicalToDbRow(row);
    assert.equal(db.checkout_value, null);
    assert.equal(db.metric_support.checkout_value, 'unsupported');
  });

  await test('Organic keeps matched platform_account_id and separate GA4 Property', () => {
    const db = canonicalToDbRow(fixtures.metaOrganic());
    assert.equal(db.platform_account_id, 'meta-acct-1');
    assert.equal(db.ga4_property_id, 'ga4-property-1');
    assert.notEqual(db.platform_account_id, db.ga4_property_id);
  });

  await test('Klaviyo channel persists without splitting platform identity', () => {
    const email = canonicalToDbRow(fixtures.klaviyoCampaignEmail());
    const sms = canonicalToDbRow(fixtures.klaviyoFlowSms());
    assert.equal(email.platform, 'klaviyo');
    assert.equal(sms.platform, 'klaviyo');
    assert.equal(email.channel, 'email');
    assert.equal(sms.channel, 'sms');
  });

  await test('PMax persists Asset Group with no fake parent', () => {
    const db = canonicalToDbRow(fixtures.googlePmax());
    assert.equal(db.campaign_type, 'performance_max');
    assert.equal(db.entity_type, 'asset_group');
    assert.equal(db.parent_entity_type, null);
    assert.equal(db.parent_entity_id, null);
  });

  await test('Persistence adapter rejects cross-currency row without FX rate', () => {
    const row = fixtures.metaPaid({ sourceCurrency: 'USD', targetCurrency: 'TRY', fxRate: null });
    assert.throws(() => canonicalToDbRow(row), /fx_rate must be a positive number/);
  });

  await test('Persistence adapter refuses synthetic canonical rows', () => {
    const row = fixtures.metaPaid({ synthetic: true, sourceConfidence: 'fallback' });
    assert.throws(() => canonicalToDbRow(row), /Synthetic rows cannot/);
  });

  // Repository adapter behavior
  await test('Supabase repository requires a server-side client', () => {
    assert.throws(() => new SupabaseDatasetRepository(null), /Supabase client is required/);
  });

  await test('Supabase repository uses exact canonical conflict key', async () => {
    const client = new FakeSupabaseClient();
    const repo = new SupabaseDatasetRepository(client);
    await repo.upsertCanonicalRawFacts([fixtures.metaPaid()]);
    assert.equal(client.lastUpsertConflict, UPSERT_CONFLICT);
  });

  await test('Supabase repository same-key UPSERT keeps one row and updates metrics', async () => {
    const client = new FakeSupabaseClient();
    const repo = new SupabaseDatasetRepository(client);
    await repo.upsertCanonicalRawFacts([fixtures.metaPaid()]);
    const firstCreatedAt = client.rows[0].created_at;
    await repo.upsertCanonicalRawFacts([fixtures.metaPaid({ metrics: { purchase: 9, purchase_value: 999 } })]);
    assert.equal(client.rows.length, 1);
    assert.equal(client.rows[0].purchase, 9);
    assert.equal(client.rows[0].purchase_value, 999);
    assert.equal(client.rows[0].created_at, firstCreatedAt);
  });

  await test('Supabase repository different business date creates separate row', async () => {
    const client = new FakeSupabaseClient();
    const repo = new SupabaseDatasetRepository(client);
    await repo.upsertCanonicalRawFacts([fixtures.metaPaid()]);
    await repo.upsertCanonicalRawFacts([fixtures.metaPaid({ date: '2026-08-16', fxRateDate: '2026-08-16' })]);
    assert.equal(client.rows.length, 2);
  });

  await test('Supabase repository different entity key creates separate row', async () => {
    const client = new FakeSupabaseClient();
    const repo = new SupabaseDatasetRepository(client);
    const first = fixtures.metaPaid();
    const second = fixtures.metaPaid();
    second.entity.entity_id = 'meta-ad-2';
    second.entity.entity_name = 'Meta Ad 2';
    await repo.upsertCanonicalRawFacts([first, second]);
    assert.equal(client.rows.length, 2);
  });

  await test('Supabase repository reads by user/date/platform/account/traffic filters', async () => {
    const client = new FakeSupabaseClient();
    const repo = new SupabaseDatasetRepository(client);
    await repo.upsertCanonicalRawFacts([fixtures.metaPaid(), fixtures.metaOrganic(), fixtures.googleStandard()]);
    const rows = await repo.readCanonicalRawFacts({
      user_id: 'user-1', from: '2026-08-15', to: '2026-08-15', platform: 'meta', platform_account_id: 'meta-acct-1', traffic_type: 'paid'
    });
    assert.equal(rows.length, 1);
    assert.equal(rows[0].identity.platform, 'meta');
    assert.equal(rows[0].identity.traffic_type, 'paid');
  });

  await test('Supabase repository reads exact entity_key history', async () => {
    const client = new FakeSupabaseClient();
    const repo = new SupabaseDatasetRepository(client);
    const row = fixtures.metaPaid();
    await repo.upsertCanonicalRawFacts([row]);
    const key = buildEntityKey(row.identity, row.entity);
    const rows = await repo.readCanonicalRawFacts({ user_id: 'user-1', from: '2026-08-15', to: '2026-08-15', entity_key: key });
    assert.equal(rows.length, 1);
    assert.equal(rows[0].entity_key, key);
  });

  await test('Supabase repository keeps Klaviyo Campaign and Flow messages separate', async () => {
    const client = new FakeSupabaseClient();
    const repo = new SupabaseDatasetRepository(client);
    await repo.upsertCanonicalRawFacts([fixtures.klaviyoCampaignEmail(), fixtures.klaviyoFlowSms()]);
    const rows = await repo.readCanonicalRawFacts({ user_id: 'user-1', from: '2026-08-15', to: '2026-08-15', platform: 'klaviyo' });
    assert.equal(rows.length, 2);
    assert.notEqual(rows[0].entity_key, rows[1].entity_key);
  });

  await test('DB row with corrupted entity_key is rejected during read mapping', () => {
    const db = canonicalToDbRow(fixtures.metaPaid());
    db.entity_key = 'wrong-key';
    assert.throws(() => dbToCanonicalRow(db), /entity_key does not match/);
  });

  await test('Repository empty UPSERT is a no-op', async () => {
    const client = new FakeSupabaseClient();
    const repo = new SupabaseDatasetRepository(client);
    assert.deepEqual(await repo.upsertCanonicalRawFacts([]), []);
    assert.equal(client.rows.length, 0);
  });

  console.log(`\nPHASE 2 LOCAL TEST RESULT: ${passed} tests passed.`);
})().catch((error) => {
  console.error(error.stack || error);
  process.exitCode = 1;
});
