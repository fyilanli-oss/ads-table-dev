"use strict";

const assert=require("node:assert/strict");
const test=require("node:test");
const {OPERATIONS}=require("../security/codex-readonly-contract");
const {DATASET_TABLE,ALLOWED_RELATIONS,SENSITIVE_RELATIONS,createCodexReadonlyService}=require("../services/codex-readonly-service");

function fakeClient(result={count:7,error:null}){
  const calls=[];
  return {calls,from(relation){calls.push(["from",relation]);return {select(columns,options){calls.push(["select",columns,options]);return Promise.resolve(result);}};}};
}

test("every operation uses only the fixed Dataset V2 head/count SELECT",async()=>{
  const client=fakeClient();
  const service=createCodexReadonlyService({client,now:()=>new Date("2026-08-23T00:00:00.000Z")});
  for(const operation of OPERATIONS)assert.ok(await service.execute(operation));
  assert.equal(await service.execute("delete"),null);
  assert.equal(client.calls.length,(OPERATIONS.length-1)*2);
  for(let index=0;index<client.calls.length;index+=2){
    assert.deepEqual(client.calls[index],["from",DATASET_TABLE]);
    assert.deepEqual(client.calls[index+1],["select","id",{count:"exact",head:true}]);
  }
  assert.doesNotMatch(JSON.stringify(client.calls),/insert|update|upsert|delete|rpc|sql/i);
});

test("the relation boundary is a singleton allowlist disjoint from the sensitive denylist",()=>{
  assert.deepEqual(ALLOWED_RELATIONS,[DATASET_TABLE]);
  assert.equal(Object.isFrozen(ALLOWED_RELATIONS),true);
  assert.equal(Object.isFrozen(SENSITIVE_RELATIONS),true);
  assert.equal(SENSITIVE_RELATIONS.includes(DATASET_TABLE),false);
  for(const relation of ["users","platform_connections","platform_connection_tokens","oauth_transactions"]){
    assert.equal(SENSITIVE_RELATIONS.includes(relation),true);
    assert.equal(ALLOWED_RELATIONS.includes(relation),false);
  }
});

test("Dataset V2 contract exposes the complete repository constraint and index inventory",async()=>{
  const response=await createCodexReadonlyService({client:fakeClient()}).execute("dataset-v2-contract");
  assert.deepEqual(response.expectedConstraintNames,[
    "performance_dataset_rows_v2_pkey",
    "performance_dataset_rows_v2_user_id_fkey",
    "performance_dataset_rows_v2_platform_chk",
    "performance_dataset_rows_v2_traffic_type_chk",
    "performance_dataset_rows_v2_source_system_chk",
    "performance_dataset_rows_v2_channel_chk",
    "performance_dataset_rows_v2_campaign_type_chk",
    "performance_dataset_rows_v2_root_type_chk",
    "performance_dataset_rows_v2_parent_type_chk",
    "performance_dataset_rows_v2_entity_type_chk",
    "performance_dataset_rows_v2_source_confidence_chk",
    "performance_dataset_rows_v2_source_currency_chk",
    "performance_dataset_rows_v2_target_currency_chk",
    "performance_dataset_rows_v2_fx_rate_chk",
    "performance_dataset_rows_v2_metric_support_object_chk",
    "performance_dataset_rows_v2_raw_object_chk",
    "performance_dataset_rows_v2_synthetic_chk",
    "performance_dataset_rows_v2_source_semantics_chk",
    "performance_dataset_rows_v2_hierarchy_chk",
    "performance_dataset_rows_v2_metric_support_keys_chk",
    "performance_dataset_rows_v2_metric_value_support_chk"
  ]);
  assert.deepEqual(response.expectedIndexNames,[
    "performance_dataset_rows_v2_pkey",
    "performance_dataset_rows_v2_canonical_uidx",
    "performance_dataset_rows_v2_user_date_idx",
    "performance_dataset_rows_v2_account_scope_date_idx",
    "performance_dataset_rows_v2_entity_history_idx"
  ]);
});

test("safe count exposes only a global integer and no source rows",async()=>{
  const response=await createCodexReadonlyService({client:fakeClient({count:42,data:[{user_id:"never-exposed"}],error:null})}).execute("dataset-v2-safe-counts");
  assert.deepEqual(response,{datasetV2Rows:42});
});

test("raw datastore errors are sanitized and reachability operations remain truthful",async()=>{
  const secret="raw SQL error with service-role-secret and https://database.invalid";
  const service=createCodexReadonlyService({client:fakeClient({count:null,error:{message:secret}})});
  assert.equal((await service.execute("health")).supabaseConnectivity,false);
  assert.equal((await service.execute("dataset-v2-contract")).runtimeTableReachability,false);
  assert.equal((await service.execute("dataset-v2-access-boundary")).datasetV2SelectReachable,false);
  await assert.rejects(()=>service.execute("dataset-v2-safe-counts"),error=>!error.message.includes(secret));
});

test("thrown datastore errors are also reduced to safe reachability state",async()=>{
  const client={from(){throw new Error("raw database URL and credential");}};
  const service=createCodexReadonlyService({client});
  assert.equal((await service.execute("health")).supabaseConnectivity,false);
  await assert.rejects(()=>service.execute("dataset-v2-safe-counts"),/Read-only datastore operation failed/);
});

test("migration inventory is repository-only and declares live ledger unavailable",async()=>{
  const result=await createCodexReadonlyService({client:fakeClient()}).execute("migration-inventory");
  assert.equal(result.liveLedgerAvailable,false);
  assert.ok(result.repositoryMigrations.some(item=>item.fileName==="20260816101220_create_performance_dataset_rows_v2.sql"));
});
