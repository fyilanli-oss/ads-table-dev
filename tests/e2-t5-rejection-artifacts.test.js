'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const root = path.join(__dirname, '..');
const read = (file) => fs.readFileSync(path.join(root, file), 'utf8');
const matrix = require('../artifacts/dataset-v2-acceptance/e2-t5-rejection/rejection-matrix.json');
const baseline = require('../artifacts/dataset-v2-acceptance/e2-t5-rejection/valid-baseline-canonical.json');
const liveAcceptance = require('../artifacts/dataset-v2-acceptance/e2-t5-rejection/live-acceptance-v2.json');
const { validateCanonicalRow } = require('../funnel-core/canonical-contract');
const { validateEntityHierarchy } = require('../funnel-core/entity-hierarchy');
const { TOP_LEVEL_FIELDS, CASE_FIELDS, buildEvidence } = require('../scripts/e2-t5-rejection-evidence');
const migration = read('supabase/migrations/20260816101220_create_performance_dataset_rows_v2.sql');
const corrective = read('supabase/migrations/20260816101540_fix_v2_klaviyo_channel_constraint.sql');
const preflight = read('docs/security/sql/E2_T5_REJECTION_PREFLIGHT.sql');
const transaction = read('docs/security/sql/E2_T5_REJECTION_TRANSACTION.sql');
const postcheck = read('docs/security/sql/E2_T5_REJECTION_POSTCHECK.sql');
const plan = read('codex-input/AdsTable_EXECUTION_PLAN_V4_2026-08-17_TR.md');
const files = [
  'artifacts/dataset-v2-acceptance/e2-t5-rejection/valid-baseline-canonical.json',
  'artifacts/dataset-v2-acceptance/e2-t5-rejection/rejection-matrix.json',
  'artifacts/dataset-v2-acceptance/e2-t5-rejection/live-acceptance-v2.json',
  'docs/security/E2_T5_REJECTION_RUNBOOK.md','docs/security/sql/E2_T5_REJECTION_PREFLIGHT.sql',
  'docs/security/sql/E2_T5_REJECTION_TRANSACTION.sql','docs/security/sql/E2_T5_REJECTION_POSTCHECK.sql',
  'scripts/e2-t5-rejection-evidence.js'
];
const strip = (sql) => sql.replace(/--.*$/gm, '').trim();
const caseCodes = [
  'INVALID_PLATFORM','INVALID_TRAFFIC_TYPE','INVALID_SOURCE_SYSTEM','INVALID_CHANNEL_ENUM','INVALID_CAMPAIGN_TYPE',
  'INVALID_ROOT_ENTITY_TYPE','INVALID_PARENT_ENTITY_TYPE','INVALID_ENTITY_TYPE','INVALID_SOURCE_CONFIDENCE',
  'INVALID_SOURCE_CURRENCY','INVALID_TARGET_CURRENCY','NON_POSITIVE_FX_RATE','METRIC_SUPPORT_NOT_OBJECT','RAW_NOT_OBJECT',
  'SYNTHETIC_TRUE','META_WRONG_SOURCE','PAID_WITH_GA4_PROPERTY','ORGANIC_WITHOUT_GA4_PROPERTY',
  'KLAVIYO_PAID_WITH_NULL_CHANNEL','KLAVIYO_PAID_WITH_INVALID_SOURCE_PAIR','META_WITH_ADGROUP_PARENT',
  'GOOGLE_STANDARD_WITHOUT_ADGROUP','GOOGLE_PMAX_WITH_FAKE_PARENT','TIKTOK_WITH_ADSET_PARENT',
  'KLAVIYO_CAMPAIGN_WITH_PARENT','KLAVIYO_FLOW_AS_CAMPAIGN','ORGANIC_WITH_PAID_HIERARCHY','MISSING_SUPPORT_KEY',
  'INVALID_SUPPORT_ENUM','SUPPORTED_METRIC_IS_NULL','UNSUPPORTED_METRIC_IS_NON_NULL','UNKNOWN_METRIC_IS_NON_NULL',
  'REQUIRED_ENTITY_ID_NULL','REQUIRED_PLATFORM_ACCOUNT_NULL','REQUIRED_BUSINESS_DATE_NULL'
];

function sample() {
  const cases = matrix.map((item) => ({
    case_code:item.case_code, expected_sqlstate:item.expected_sqlstate, actual_sqlstate:item.expected_sqlstate,
    expected_constraints:[...item.expected_constraints], actual_constraint:item.expected_constraints[0] || null,
    expected_column:item.expected_column, actual_column:item.expected_column, rejected:true, passed:true
  }));
  return {
    operation_code:'e2_t5_rejection_v1',expected_case_count:35,evaluated_case_count:35,passed_case_count:35,
    failed_case_count:0,unexpected_accept_count:0,residue_count:0,dataset_unchanged:true,v1_unchanged:true,
    snapshots_unchanged:true,oauth_unchanged:true,connected_unchanged:true,encrypted_unchanged:true,missing_encrypted_unchanged:true,orphan_encrypted_unchanged:true,
    plaintext_unchanged:true,ledger_unchanged:true,overall_passed:true,cases
  };
}

test('all E2-T5 artifacts exist and baseline passes canonical validators', () => {
  for (const file of files) assert.equal(fs.existsSync(path.join(root,file)), true, file);
  validateCanonicalRow(baseline); validateEntityHierarchy(baseline.identity, baseline.entity);
  assert.match(JSON.stringify(baseline), /e2_t5_rejection_v1/);
});

test('matrix has the exact 35 unique cases and exact schema', () => {
  assert.deepEqual(matrix.map(x=>x.case_code),caseCodes); assert.equal(new Set(caseCodes).size,35);
  for (const item of matrix) {
    assert.deepEqual(Object.keys(item),['case_code','category','intended_violation','expected_sqlstate','expected_constraints','expected_column','expected_outcome']);
    assert.equal(item.expected_outcome,'REJECTED'); assert.equal(typeof item.category,'string');
  }
});

test('CHECK and NOT NULL contracts are exact, sorted, unique, and closed', () => {
  const checks=matrix.filter(x=>x.expected_sqlstate==='23514'), notNull=matrix.filter(x=>x.expected_sqlstate==='23502');
  assert.equal(checks.length,32); assert.equal(notNull.length,3);
  for(const item of checks){assert.equal(item.expected_column,null);assert(item.expected_constraints.length>0);assert.deepEqual(item.expected_constraints,[...new Set(item.expected_constraints)].sort());for(const name of item.expected_constraints)assert.match(name,/^performance_dataset_rows_v2_[a-z_]+_chk$/);}
  assert.deepEqual(notNull.map(x=>x.expected_column),['entity_id','platform_account_id','business_date']);
  for(const item of notNull)assert.deepEqual(item.expected_constraints,[]);
});

test('all closed constraint names exist in the migration inventory', () => {
  const inventory=new Set([...migration.matchAll(/constraint\s+(performance_dataset_rows_v2_[a-z_]+_chk)\s+check/gi)].map(x=>x[1]));
  for(const item of matrix)for(const name of item.expected_constraints)assert(inventory.has(name),name);
  assert.equal(inventory.size,19); assert.match(corrective,/channel is not null and channel in \('email', 'sms'\)/i);
});

test('singleton and overlap sets encode expression-derived three-valued CHECK behavior',()=>{
  const checks=matrix.filter(x=>x.expected_sqlstate==='23514');
  assert.equal(checks.filter(x=>x.expected_constraints.length===1).length,22);
  assert.equal(checks.filter(x=>x.expected_constraints.length>1).length,10);
  const expected={
    INVALID_PLATFORM:['performance_dataset_rows_v2_hierarchy_chk','performance_dataset_rows_v2_platform_chk','performance_dataset_rows_v2_source_semantics_chk'],
    INVALID_TRAFFIC_TYPE:['performance_dataset_rows_v2_hierarchy_chk','performance_dataset_rows_v2_source_semantics_chk','performance_dataset_rows_v2_traffic_type_chk'],
    INVALID_SOURCE_SYSTEM:['performance_dataset_rows_v2_source_semantics_chk','performance_dataset_rows_v2_source_system_chk'],
    INVALID_CHANNEL_ENUM:['performance_dataset_rows_v2_channel_chk','performance_dataset_rows_v2_source_semantics_chk'],
    INVALID_CAMPAIGN_TYPE:['performance_dataset_rows_v2_campaign_type_chk','performance_dataset_rows_v2_hierarchy_chk'],
    INVALID_ROOT_ENTITY_TYPE:['performance_dataset_rows_v2_hierarchy_chk','performance_dataset_rows_v2_root_type_chk'],
    INVALID_PARENT_ENTITY_TYPE:['performance_dataset_rows_v2_hierarchy_chk','performance_dataset_rows_v2_parent_type_chk'],
    INVALID_ENTITY_TYPE:['performance_dataset_rows_v2_entity_type_chk','performance_dataset_rows_v2_hierarchy_chk'],
    METRIC_SUPPORT_NOT_OBJECT:['performance_dataset_rows_v2_metric_support_keys_chk','performance_dataset_rows_v2_metric_support_object_chk'],
    INVALID_SUPPORT_ENUM:['performance_dataset_rows_v2_metric_support_keys_chk','performance_dataset_rows_v2_metric_value_support_chk']
  };
  for(const [code,set] of Object.entries(expected))assert.deepEqual(matrix.find(x=>x.case_code===code).expected_constraints,set);
  assert.deepEqual(matrix.find(x=>x.case_code==='MISSING_SUPPORT_KEY').expected_constraints,['performance_dataset_rows_v2_metric_support_keys_chk']);
});

test('invalid non-null impression support makes both keys and metric-value CHECK expressions false',()=>{
  const supportKeys=['impression','ad_click','session','spend_value','add_to_cart','add_to_cart_value','checkout','checkout_value','purchase','purchase_value'];
  const support=Object.fromEntries(supportKeys.map(key=>[key,key==='impression'?'invalid':key==='session'?'unsupported':'supported']));
  const values={impression:100,ad_click:10,session:null,spend_value:25,add_to_cart:0,add_to_cart_value:0,checkout:2,checkout_value:40,purchase:1,purchase_value:30};
  const allowed=new Set(['supported','unsupported','unknown']);
  const keysExpression=supportKeys.every(key=>Object.hasOwn(support,key)&&allowed.has(support[key]));
  const valueSupportExpression=supportKeys.every(key=>(support[key]==='supported'&&values[key]!==null)||(new Set(['unsupported','unknown']).has(support[key])&&values[key]===null));
  assert.equal(support.impression,'invalid');assert.equal(values.impression,100);
  assert.equal(keysExpression,false);assert.equal(valueSupportExpression,false);
  const keysDef=migration.match(/constraint performance_dataset_rows_v2_metric_support_keys_chk\s+check \(([\s\S]*?)\n    \),/i);
  const valuesDef=migration.match(/constraint performance_dataset_rows_v2_metric_value_support_chk\s+check \(([\s\S]*?)\n    \)\n\);/i);
  assert(keysDef);assert(valuesDef);assert.match(keysDef[1],/metric_support->>'impression'\) in \('supported','unsupported','unknown'\)/);
  assert.match(valuesDef[1],/metric_support->>'impression'\) = 'supported' and impressions is not null/);
  assert.deepEqual(matrix.find(x=>x.case_code==='INVALID_SUPPORT_ENUM').expected_constraints,['performance_dataset_rows_v2_metric_support_keys_chk','performance_dataset_rows_v2_metric_value_support_chk']);
});

test('preflight and postcheck are one read-only WITH SELECT each with scalar baselines',()=>{
  for(const sql of [preflight,postcheck]){const clean=strip(sql);assert.match(clean,/^with\b/i);assert.equal((clean.match(/;/g)||[]).length,1);assert.doesNotMatch(clean,/\b(?:insert|update|delete|alter|create|drop|grant|revoke|truncate|call|do|begin|commit|rollback)\b/i);}
  assert.match(postcheck,/values \(\(-1\)::bigint,\(-1\)::bigint,\(-1\)::bigint,\(-1\)::bigint,\(-1\)::bigint\)/);
  assert.doesNotMatch(postcheck,/select\s+'(?:DATASET_ROWS|V1_ROWS|SNAPSHOT_ROWS)'\s*,\s*count\(\*\)\s*,\s*e\./i);
  for(const gate of ['LEDGER_TOTAL','DATASET_TABLE','RLS_STATE','NAMED_VALIDATED_CHECKS','REQUIRED_NOT_NULL_COLUMNS','KLAVIYO_CORRECTIVE_SEMANTICS','E2_T5_RESIDUE','ELIGIBLE_USERS','DATASET_ROWS','V1_ROWS','SNAPSHOT_ROWS','OAUTH_ROWS','CONNECTED_CONNECTIONS','ENCRYPTED_TOKEN_ROWS','MISSING_ENCRYPTED','PLAINTEXT_TOKENS'])assert.match(preflight,new RegExp(`'${gate}'`));
});

test('transaction has one outer BEGIN/ROLLBACK, no COMMIT, and only pg_temp DDL',()=>{
  const clean=strip(transaction);assert.match(clean,/^begin\s*;/i);assert.match(clean,/rollback\s*;$/i);
  assert.equal((clean.match(/^begin\s*;/gim)||[]).length,1);assert.equal((clean.match(/^rollback\s*;/gim)||[]).length,1);assert.doesNotMatch(clean,/^commit\s*;/im);
  assert.equal((clean.match(/create\s+temp\s+table/gi)||[]).length,2);assert.match(clean,/create temp table pg_temp\.e2_t5_rejection_evidence/i);
  assert.doesNotMatch(clean,/^(?:create(?!\s+temp\s+table)|alter|drop|grant|revoke)\b/im);
});

test('transaction contains 35 static INSERTs and separate exception subtransactions',()=>{
  assert.equal((transaction.match(/insert into public\.performance_dataset_rows_v2/gi)||[]).length,35);
  assert.equal((transaction.match(/^    begin$/gim)||[]).length,35);
  assert.equal((transaction.match(/when check_violation or not_null_violation then/gi)||[]).length,35);
  for(const code of caseCodes)assert.match(transaction,new RegExp(`'${code}'`),code);
  assert.doesNotMatch(transaction,/\bexecute\b|\bformat\s*\(|\|\||\bon conflict\b|\bupdate\b|\bdelete\b|\btruncate\b/i);
});

test('transaction mutates only Dataset V2 and diagnostics are safe',()=>{
  for(const relation of ['performance_dataset_rows','dashboard_snapshots','oauth_transactions','platform_connections','platform_connection_tokens','schema_migrations'])assert.doesNotMatch(transaction,new RegExp(`(?:insert\\s+into|update|delete\\s+from|truncate)\\s+(?:public\\.)?${relation}\\b`,'i'));
  assert.equal((transaction.match(/get stacked diagnostics/gi)||[]).length,70);
  for(const field of ['returned_sqlstate','constraint_name','column_name'])assert.match(transaction,new RegExp(field,'i'));
  assert.doesNotMatch(transaction,/SQLERRM|MESSAGE_TEXT|PG_EXCEPTION_DETAIL|PG_EXCEPTION_HINT|PG_EXCEPTION_CONTEXT/i);
});

test('single final response matches converter allowlists and deterministic case order',()=>{
  assert.equal((transaction.match(/select evidence from payload/gi)||[]).length,1);
  for(const field of TOP_LEVEL_FIELDS)assert.match(transaction,new RegExp(`'${field}'`),field);
  for(const field of CASE_FIELDS)assert.match(transaction,new RegExp(`'${field}'`),field);
  assert.match(transaction,/jsonb_agg\([\s\S]*order by array_position/i);
});

test('dataset unchanged means exact baseline parity and unexpected acceptance fails overall',()=>{
  assert.match(transaction,/\(select count\(\*\) from public\.performance_dataset_rows_v2\)=s\.dataset_before dataset_unchanged,/);
  assert.doesNotMatch(transaction,/dataset_unchanged[^\n]*\+\s*s?\.?unexpected_accept_count/i);
  const gates=['dataset_unchanged','v1_unchanged','snapshots_unchanged','oauth_unchanged','connected_unchanged','encrypted_unchanged','missing_encrypted_unchanged','orphan_encrypted_unchanged','plaintext_unchanged','ledger_unchanged'];
  for(const gate of gates){assert.match(transaction,new RegExp(`\\b${gate}\\b`));assert.match(transaction,new RegExp(`'overall_passed'[\\s\\S]*p\\.${gate}\\b`));}
  assert.match(transaction,/'overall_passed',s\.evaluated_case_count=35 and s\.passed_case_count=35 and s\.failed_case_count=0 and s\.unexpected_accept_count=0 and r\.residue_count=0/);
});

test('converter accepts exact evidence and fails wrong state, constraint, column, or acceptance',()=>{
  assert.equal(buildEvidence(sample()).status,'PASS');
  const state=sample();state.cases[0].actual_sqlstate='23502';assert.throws(()=>buildEvidence(state),/SQLSTATE/);
  const constraint=sample();constraint.cases.find(x=>x.case_code==='INVALID_SOURCE_CONFIDENCE').actual_constraint='performance_dataset_rows_v2_platform_chk';assert.throws(()=>buildEvidence(constraint),/closed allowlist/);
  const empty=sample();empty.cases[0].actual_constraint='';assert.throws(()=>buildEvidence(empty),/empty CHECK/);
  const column=sample();column.cases.find(x=>x.case_code==='REQUIRED_ENTITY_ID_NULL').actual_column='business_date';assert.throws(()=>buildEvidence(column),/column mismatch/);
  const accepted=sample();accepted.cases[0].rejected=false;accepted.cases[0].passed=false;assert.throws(()=>buildEvidence(accepted),/did not pass/);
});

test('converter fails missing, extra, duplicate, unknown fields, residue, and parity failures',()=>{
  const missing=sample();missing.cases.pop();assert.throws(()=>buildEvidence(missing),/exactly 35/);
  const duplicate=sample();duplicate.cases[34]={...duplicate.cases[0]};assert.throws(()=>buildEvidence(duplicate),/duplicate/);
  const unknown=sample();unknown.extra=true;assert.throws(()=>buildEvidence(unknown),/allowlist/);
  const raw=sample();raw.cases[0].error='raw';assert.throws(()=>buildEvidence(raw),/allowlist/);
  const residue=sample();residue.residue_count=1;assert.throws(()=>buildEvidence(residue),/residue/);
  for(const field of ['dataset_unchanged','v1_unchanged','snapshots_unchanged','oauth_unchanged','connected_unchanged','encrypted_unchanged','missing_encrypted_unchanged','orphan_encrypted_unchanged','plaintext_unchanged','ledger_unchanged','overall_passed']){const value=sample();value[field]=false;assert.throws(()=>buildEvidence(value),new RegExp(field));}
});

test('artifacts contain no UUID, email, connection URI, JWT, private key, or authorization value',()=>{
  const text=files.map(read).join('\n');
  assert.doesNotMatch(text,/postgres(?:ql)?:\/\/|authorization\s*:|bearer\s+|-----BEGIN [A-Z ]*PRIVATE KEY-----|\beyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\b|[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}|[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}/i);
});

test('execution plan preserves exact E2 statuses and E2-T5 deviation record',()=>{
  for(const n of [1,2,3,4,5])assert.match(plan,new RegExp(`E2-T${n} — `+'`Done`'));
  for(const n of [6,7,8])assert.match(plan,new RegExp(`E2-T${n} — `+'`Not started`'));
  assert.match(plan,/İlk tasarım exact tek constraint hedefledi/);assert.match(plan,/allowlist canlı sonuçtan öğrenilmedi/);
});

test('live V2 acceptance evidence is redacted, exact, and retry-free',()=>{
  assert.deepEqual(liveAcceptance,{
    evidence_version:'e2-t5-rejection-v2-live-acceptance',operation:'e2_t5_rejection_v2',
    approved_main_sha:'135c9e880dd6db22059175977a3c2850ebe079fa',accepted_at_utc:'2026-08-27T09:05:00Z',
    preflight:'18/18 PASS',expected_case_count:35,evaluated_case_count:35,passed_case_count:35,
    failed_case_count:0,unexpected_accept_count:0,residue_count:0,transaction_requests:1,
    transaction_retries:0,postcheck:'15/15 PASS',postcheck_requests:1,postcheck_retries:0,
    state:'CONSUMED',production_no_change:true,contains_production_counts:false,contains_identity_or_row_data:false
  });
});

test('E2-C1 captured provider-token parity is fail-closed',()=>{for(const sql of [preflight,transaction,postcheck])assert.doesNotMatch(sql,/(?:CONNECTED_CONNECTIONS|ENCRYPTED_TOKEN_ROWS)[^\n]*,\s*7\b/);for(const code of ['MISSING_ENCRYPTED','ORPHAN_ENCRYPTED','PLAINTEXT_TOKENS','CONNECTION_TOKEN_PARITY'])assert.match(preflight,new RegExp(`'${code}'`));assert.match(preflight,/'CONNECTED_CONNECTIONS'[^\n]*'capture'/);assert.match(transaction,/connected_count[\s\S]*encrypted_count/);for(const field of ['connected_unchanged','encrypted_unchanged','missing_encrypted_unchanged','orphan_encrypted_unchanged','plaintext_unchanged'])assert.match(transaction,new RegExp(`'${field}'`));assert.match(postcheck,/connected_rows/);assert.match(postcheck,/encrypted_rows/);assert.match(transaction,/rollback;\s*$/i);assert.throws(()=>buildEvidence({...sample(),actual_token_count:1}),/counts are forbidden/);});

test('T5 postcheck/runbook use five ordered baselines and overall requires all fifteen gates',()=>{assert.equal((postcheck.match(/\(-1\)::bigint/g)||[]).length,5);const runbook=read('docs/security/E2_T5_REJECTION_RUNBOOK.md');assert.match(runbook,/exactly five count baselines[\s\S]*in this order: Dataset V2, V1, snapshot, connected, encrypted/);assert.match(runbook,/exactly five `-1` scalar placeholders[\s\S]*in this order: Dataset V2, V1, snapshot, connected, encrypted/);assert.match(runbook,/Actual provider counts are never shared or committed/);const overall=transaction.slice(transaction.indexOf("'overall_passed'"),transaction.indexOf("'cases'"));for(const gate of ['s.evaluated_case_count=35','s.passed_case_count=35','s.failed_case_count=0','s.unexpected_accept_count=0','r.residue_count=0','p.dataset_unchanged','p.v1_unchanged','p.snapshots_unchanged','p.oauth_unchanged','p.connected_unchanged','p.encrypted_unchanged','p.missing_encrypted_unchanged','p.orphan_encrypted_unchanged','p.plaintext_unchanged','p.ledger_unchanged'])assert.match(overall,new RegExp(gate.replaceAll('.','\\.')));});
