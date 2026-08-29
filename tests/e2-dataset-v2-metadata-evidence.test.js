"use strict";
const assert=require("node:assert/strict");
const crypto=require("node:crypto");
const fs=require("node:fs");
const path=require("node:path");
const test=require("node:test");
const root=path.join(__dirname,"..");
const dir=path.join(root,"artifacts/dataset-v2-acceptance/20260824-metadata-acceptance");
const commit="d16e87449aeb9eefdea570f5bb8eca77b67ed8ef";
const jsonNames=["schema.json","constraints-indexes.json","rls-grants.json","migration-ledger.json","query-manifest.json"];
const readJson=(name)=>JSON.parse(fs.readFileSync(path.join(dir,name),"utf8"));
const evidence=Object.fromEntries(jsonNames.map((name)=>[name,readJson(name)]));
const schema=evidence["schema.json"], objects=evidence["constraints-indexes.json"], rls=evidence["rls-grants.json"], ledger=evidence["migration-ledger.json"], queries=evidence["query-manifest.json"];
const allowed=(object,keys)=>assert.deepEqual(Object.keys(object).sort(),[...keys].sort());
const migrationChecksums={
  "20260816101220_create_performance_dataset_rows_v2.sql":"27cd4bf405b12a7e6928e5c9742f709440da987738fb181bf6dbf5e8da8085f2",
  "20260816101540_fix_v2_klaviyo_channel_constraint.sql":"6402acdc9a6e426c138d797b7993462ee4bd0c5a0ca34fced7a73586dfb5d5f3",
};
const expectedColumns=[
 ["id","uuid",false,"generated_uuid"],["user_id","uuid",false,"none"],["platform","text",false,"none"],["traffic_type","text",false,"none"],["source_system","text",false,"none"],["channel","text",true,"none"],["platform_account_id","text",false,"none"],["business_date","date",false,"none"],["campaign_type","text",true,"none"],["root_entity_type","text",false,"none"],["root_entity_id","text",false,"none"],["root_entity_name","text",true,"none"],["parent_entity_type","text",true,"none"],["parent_entity_id","text",true,"none"],["parent_entity_name","text",true,"none"],["entity_type","text",false,"none"],["entity_id","text",false,"none"],["entity_name","text",false,"none"],["entity_key","text",false,"none"],["metric_support","jsonb",false,"none"],["impressions","numeric",true,"none"],["ad_clicks","numeric",true,"none"],["sessions","numeric",true,"none"],["spend","numeric",true,"none"],["add_to_cart","numeric",true,"none"],["add_to_cart_value","numeric",true,"none"],["checkout","numeric",true,"none"],["checkout_value","numeric",true,"none"],["purchase","numeric",true,"none"],["purchase_value","numeric",true,"none"],["source_currency","text",false,"none"],["target_currency","text",false,"none"],["fx_rate","numeric",false,"none"],["fx_rate_date","date",false,"none"],["fx_provider","text",false,"none"],["fx_engine_version","text",false,"none"],["source_timezone","text",false,"none"],["time_engine_version","text",false,"none"],["canonical_contract_version","text",false,"none"],["adapter_version","text",false,"none"],["source_confidence","text",false,"none"],["synthetic","boolean",false,"false"],["ga4_property_id","text",true,"none"],["source_job_id","uuid",true,"none"],["raw","jsonb",false,"empty_json_object"],["created_at","timestamp with time zone",false,"current_timestamp"],["updated_at","timestamp with time zone",false,"current_timestamp"]
];
const checkNames=["campaign_type","channel","entity_type","fx_rate","hierarchy","metric_support_keys","metric_support_object","metric_value_support","parent_type","platform","raw_object","root_type","source_confidence","source_currency","source_semantics","source_system","synthetic","target_currency","traffic_type"].map((x)=>`performance_dataset_rows_v2_${x}_chk`).sort();

test("six immutable evidence files exist and JSON is parseable",()=>{
 for(const name of ["summary.md",...jsonNames]) assert.equal(fs.existsSync(path.join(dir,name)),true,name);
 for(const value of Object.values(evidence)) assert.equal(typeof value,"object");
});

test("evidence uses allowlisted top-level fields and exact baseline",()=>{
 allowed(schema,["evidence_version","generated_date","repository_commit","table","expected_column_count","live_column_count","migration_checksums","columns","overall_result"]);
 allowed(objects,["evidence_version","repository_commit","table","expected_constraint_counts","live_constraint_counts","constraints","expected_physical_index_count","live_physical_index_count","indexes","missing_objects","extra_objects","invalid_or_not_ready_index_count","unvalidated_constraint_count","overall_result"]);
 allowed(rls,["evidence_version","repository_commit","table","rls","policies","privileges","overall_result"]);
 allowed(ledger,["evidence_version","repository_commit","total_ledger_count","targets","duplicate_target_version_count","dataset_v2_row_count","repository_migration_checksums","result"]);
 allowed(queries,["evidence_version","repository_commit","queries","overall_result"]);
 for(const value of Object.values(evidence)) assert.equal(value.repository_commit,commit);
});

test("migration checksums match exact repository sources",()=>{
 for(const [name,expected] of Object.entries(migrationChecksums)){
  const actual=crypto.createHash("sha256").update(fs.readFileSync(path.join(root,"supabase/migrations",name))).digest("hex");
  assert.equal(actual,expected); assert.equal(schema.migration_checksums[name],expected); assert.equal(ledger.repository_migration_checksums[name],expected);
 }
});

test("47-column type, nullability, order, and default contract is exact",()=>{
 assert.equal(schema.expected_column_count,47); assert.equal(schema.live_column_count,47); assert.equal(schema.columns.length,47);
 const actual=schema.columns.map((c,i)=>{ assert.equal(c.ordinal,i+1); assert.equal(c.expected_type,c.live_type); assert.equal(c.expected_nullable,c.live_nullable); assert.equal(c.expected_default_class,c.live_default_class); assert.equal(c.result,"PASS"); return [c.name,c.live_type,c.live_nullable,c.live_default_class]; });
 assert.deepEqual(actual,expectedColumns); assert.equal(schema.overall_result,"PASS");
});

test("PK, FK, and nineteen validated check constraints are exact",()=>{
 assert.deepEqual(objects.expected_constraint_counts,{check:19,foreign_key:1,primary_key:1}); assert.deepEqual(objects.live_constraint_counts,objects.expected_constraint_counts);
 const checks=objects.constraints.filter((x)=>x.type==="check"); assert.deepEqual(checks.map((x)=>x.name).sort(),checkNames);
 assert.equal(objects.constraints.filter((x)=>x.type==="primary_key"&&x.name==="performance_dataset_rows_v2_pkey").length,1);
 const fk=objects.constraints.find((x)=>x.name==="performance_dataset_rows_v2_user_id_fkey"); assert.equal(fk.type,"foreign_key"); assert.equal(fk.target_relation,"public.users");
 for(const item of objects.constraints){ assert.equal(item.validated,true); assert.match(item.fingerprint,/^[0-9a-f]{64}$/); assert.equal(item.result,"PASS"); }
 assert.equal(objects.unvalidated_constraint_count,0); assert.deepEqual(objects.missing_objects,[]); assert.deepEqual(objects.extra_objects,[]);
});

test("five physical indexes and four migration-created indexes are exact",()=>{
 const expected={performance_dataset_rows_v2_pkey:[true,true,["id"]],performance_dataset_rows_v2_canonical_uidx:[true,false,["user_id","platform","platform_account_id","business_date","traffic_type","entity_key"]],performance_dataset_rows_v2_user_date_idx:[false,false,["user_id","business_date"]],performance_dataset_rows_v2_account_scope_date_idx:[false,false,["user_id","platform","platform_account_id","traffic_type","business_date"]],performance_dataset_rows_v2_entity_history_idx:[false,false,["user_id","platform","platform_account_id","entity_key","business_date"]]};
 assert.equal(objects.expected_physical_index_count,5); assert.equal(objects.live_physical_index_count,5); assert.equal(objects.indexes.length,5);
 for(const item of objects.indexes){ assert.deepEqual([item.unique,item.primary,item.columns],expected[item.name]); assert.equal(item.valid,true); assert.equal(item.ready,true); assert.equal(item.result,"PASS"); }
 assert.equal(objects.invalid_or_not_ready_index_count,0);
});

test("RLS, own-row SELECT policy, and role boundaries are exact",()=>{
 assert.deepEqual(rls.rls,{expected_enabled:true,expected_forced:false,live_enabled:true,live_forced:false,result:"PASS"}); assert.equal(rls.policies.length,1);
 const policy=rls.policies[0]; assert.equal(policy.name,"performance_dataset_rows_v2_select_own"); assert.deepEqual(policy.roles,["authenticated"]); assert.equal(policy.command,"SELECT"); assert.equal(policy.permissive,"PERMISSIVE"); assert.match(policy.using_fingerprint,/^[0-9a-f]{64}$/); assert.equal(policy.with_check_fingerprint,null); assert.equal(policy.result,"PASS");
 assert.deepEqual(rls.privileges.anon.live,[]); assert.deepEqual(rls.privileges.authenticated.live,["SELECT"]); assert.equal(rls.privileges.authenticated.mutation_allowed,false); assert.deepEqual(rls.privileges.service_role.live,["SELECT","INSERT","UPDATE","DELETE","TRUNCATE","REFERENCES","TRIGGER"]); assert.equal(rls.overall_result,"PASS");
});

test("reconciled ledger and empty Dataset V2 safe state are exact",()=>{
 assert.equal(ledger.total_ledger_count,37); assert.equal(ledger.targets.length,2); assert.equal(ledger.duplicate_target_version_count,0); assert.equal(ledger.dataset_v2_row_count,0); assert.equal(ledger.result,"PASS");
 assert.deepEqual(ledger.targets.map((x)=>[x.version,x.live_name]),[["20260816101220","create_performance_dataset_rows_v2"],["20260816101540","fix_v2_klaviyo_channel_constraint"]]);
});

test("query manifest is five read-only metadata purposes with no credential or row data",()=>{
 assert.equal(queries.queries.length,5); assert.deepEqual(queries.queries.map((q)=>q.code),["Q1","Q2","Q3","Q4","Q5"]);
 for(const q of queries.queries){ assert.match(q.statement_class,/^(SELECT|WITH_SELECT)$/); assert.equal(q.result_status,"PASS"); assert.equal(q.mutation,false); assert.equal(q.row_data_returned,false); assert.equal(q.credential_logged,false); }
 assert.equal(queries.overall_result,"PASS");
});

test("evidence is redacted and E2 task states match the current execution plan",()=>{
 const combined=[...jsonNames.map((name)=>fs.readFileSync(path.join(dir,name),"utf8")),fs.readFileSync(path.join(dir,"summary.md"),"utf8")].join("\n");
 assert.doesNotMatch(combined,/postgres(?:ql)?:\/\/|authorization\s*:|bearer\s+|-----BEGIN [A-Z ]*PRIVATE KEY-----|\beyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\b|[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}|[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}/i);
 const plan=fs.readFileSync(path.join(root,"codex-input/AdsTable_EXECUTION_PLAN_V4_2026-08-17_TR.md"),"utf8");
 assert.match(plan,/E2-T1 — `Done`/); assert.match(plan,/E2-T2 — `Done`/);
 assert.match(plan,/E2-T3 — `Done`/);
 assert.match(plan,/E2-T4 — `Done`/);
 assert.match(plan,/E2-T5 — `Done`/);
 assert.match(plan,/E2-T6 — `Done`/);
 assert.match(plan,/E2-T7 — `Verification`/);
 assert.match(plan,/E2-T8 — `Verification`/);
});
