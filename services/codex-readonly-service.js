"use strict";

const fs=require("node:fs");
const path=require("node:path");
const {CONTRACT_VERSION,OPERATIONS,validateOperationResponse}=require("../security/codex-readonly-contract");
const {CANONICAL_CONTRACT_VERSION}=require("../funnel-core/canonical-contract");

const DATASET_TABLE="performance_dataset_rows_v2";
const ALLOWED_RELATIONS=Object.freeze([DATASET_TABLE]);
const SENSITIVE_RELATIONS=Object.freeze(["users","subscriptions","platform_connections","platform_connection_tokens","oauth_transactions"]);
const CANONICAL_COLUMNS=["id","user_id","platform","traffic_type","source_system","channel","platform_account_id","business_date","campaign_type","root_entity_type","root_entity_id","root_entity_name","parent_entity_type","parent_entity_id","parent_entity_name","entity_type","entity_id","entity_name","entity_key","metric_support","impressions","ad_clicks","sessions","spend","add_to_cart","add_to_cart_value","checkout","checkout_value","purchase","purchase_value","source_currency","target_currency","fx_rate","fx_rate_date","fx_provider","fx_engine_version","source_timezone","time_engine_version","canonical_contract_version","adapter_version","source_confidence","synthetic","ga4_property_id","source_job_id","raw","created_at","updated_at"];
const NULLABLE=["channel","campaign_type","root_entity_name","parent_entity_type","parent_entity_id","parent_entity_name","impressions","ad_clicks","sessions","spend","add_to_cart","add_to_cart_value","checkout","checkout_value","purchase","purchase_value","ga4_property_id","source_job_id"];
const CONSTRAINTS=["performance_dataset_rows_v2_pkey","performance_dataset_rows_v2_platform_chk","performance_dataset_rows_v2_traffic_type_chk","performance_dataset_rows_v2_source_system_chk","performance_dataset_rows_v2_channel_chk","performance_dataset_rows_v2_campaign_type_chk","performance_dataset_rows_v2_root_type_chk","performance_dataset_rows_v2_parent_type_chk","performance_dataset_rows_v2_entity_type_chk","performance_dataset_rows_v2_source_confidence_chk","performance_dataset_rows_v2_source_currency_chk","performance_dataset_rows_v2_target_currency_chk","performance_dataset_rows_v2_fx_rate_chk","performance_dataset_rows_v2_metric_support_object_chk","performance_dataset_rows_v2_raw_object_chk","performance_dataset_rows_v2_synthetic_chk","performance_dataset_rows_v2_source_semantics_chk","performance_dataset_rows_v2_hierarchy_chk","performance_dataset_rows_v2_metric_support_keys_chk","performance_dataset_rows_v2_metric_value_support_chk"];
const INDEXES=["performance_dataset_rows_v2_canonical_uidx","performance_dataset_rows_v2_user_date_idx","performance_dataset_rows_v2_account_scope_date_idx","performance_dataset_rows_v2_entity_history_idx"];

function repositoryMigrations(rootDir){
  const dir=path.join(rootDir,"supabase","migrations");
  return fs.readdirSync(dir).filter(name=>/^\d+_[A-Za-z0-9_-]+\.sql$/.test(name)).sort().map(fileName=>({version:fileName.split("_")[0],fileName}));
}

function createCodexReadonlyService({client,rootDir=path.join(__dirname,".."),now=()=>new Date()}={}){
  const migrations=repositoryMigrations(rootDir);
  async function probe(){
    if(!client)return {reachable:false,count:0};
    let result;
    try{result=await client.from(ALLOWED_RELATIONS[0]).select("id",{count:"exact",head:true});}
    catch(_error){return {reachable:false,count:0,failed:true};}
    if(result&&result.error)return {reachable:false,count:0,failed:true};
    const count=result&&Number.isSafeInteger(result.count)&&result.count>=0?result.count:0;
    return {reachable:true,count};
  }
  async function execute(operation){
    if(!OPERATIONS.includes(operation))return null;
    const status=operation==="migration-inventory"?null:await probe();
    let response;
    if(operation==="health")response={serviceStatus:"ok",readOnly:true,contractVersion:CONTRACT_VERSION,timestamp:now().toISOString(),supabaseConnectivity:status.reachable};
    if(operation==="dataset-v2-contract")response={expectedContractVersion:CANONICAL_CONTRACT_VERSION,repositoryMigrationVersions:migrations.map(item=>item.version),canonicalColumns:CANONICAL_COLUMNS,nullability:{required:CANONICAL_COLUMNS.filter(column=>!NULLABLE.includes(column)),nullable:NULLABLE},expectedConstraintNames:CONSTRAINTS,expectedIndexNames:INDEXES,runtimeTableReachability:status.reachable,catalogMetadata:"unavailable",driftAssessment:"unavailable",capabilityLimitation:"PostgREST does not expose PostgreSQL catalog metadata through this gateway."};
    if(operation==="dataset-v2-access-boundary")response={gatewayAuthenticated:true,serviceClientConfigured:Boolean(client),datasetV2SelectReachable:status.reachable,writeOperationExposed:false,arbitraryQueryExposed:false,sensitiveRelationExposed:false};
    if(operation==="dataset-v2-safe-counts"){
      if(status.failed||!client)throw new Error("Read-only datastore operation failed");
      response={datasetV2Rows:status.count};
    }
    if(operation==="migration-inventory")response={repositoryMigrations:migrations,liveLedgerAvailable:false,capabilityLimitation:"The live Supabase migration ledger is not exposed by the available HTTPS API."};
    return validateOperationResponse(operation,response);
  }
  return {execute};
}

module.exports={DATASET_TABLE,ALLOWED_RELATIONS,SENSITIVE_RELATIONS,CANONICAL_COLUMNS,createCodexReadonlyService,repositoryMigrations};
