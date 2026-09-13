"use strict";

const {validateCanonicalRow,isDateOnly}=require("../../funnel-core/canonical-contract");
const {buildEntityKey,validateEntityHierarchy}=require("../../funnel-core/entity-hierarchy");

const MAX_FRESHNESS_DAYS=1;
function required(value,field){const clean=typeof value==="string"?value.trim():"";if(!clean)throw new TypeError(`${field} is required`);return clean}
function checkpointKey(item){return [required(item.user_id,"user_id"),required(item.platform,"platform").toLowerCase(),required(item.platform_account_id,"platform_account_id"),required(item.business_date,"business_date"),required(item.date_key,"date_key")].join(":")}
function rowKey(row){return [row.identity.user_id,row.identity.platform,row.identity.platform_account_id,row.time.business_date,row.identity.traffic_type,buildEntityKey(row.identity,row.entity)].join(":")}
function dateDays(from,to){if(!isDateOnly(from)||!isDateOnly(to))throw new TypeError("readiness dates must be YYYY-MM-DD");return Math.floor((Date.parse(`${to}T00:00:00Z`)-Date.parse(`${from}T00:00:00Z`))/86400000)}
function evaluateBackfillDataReadiness({expected_checkpoints=[],checkpoints=[],rows=[],as_of_business_date}={}){
  if(!Array.isArray(expected_checkpoints)||!Array.isArray(checkpoints)||!Array.isArray(rows))throw new TypeError("readiness inputs must be arrays");
  required(as_of_business_date,"as_of_business_date");if(!isDateOnly(as_of_business_date))throw new TypeError("as_of_business_date must be YYYY-MM-DD");
  const expectedKeys=new Set(expected_checkpoints.map(checkpointKey));if(expectedKeys.size!==expected_checkpoints.length)throw new Error("duplicate expected checkpoint identity");
  const actual=new Map();for(const item of checkpoints){const key=checkpointKey(item);if(actual.has(key))throw new Error("duplicate checkpoint identity");actual.set(key,item)}
  let completed=0;for(const key of expectedKeys)if(actual.get(key)?.status==="completed")completed++;
  const rowKeys=new Set();let duplicates=0,metricSupportValid=true,timezoneValid=true,fxValid=true;
  for(const row of rows){try{validateCanonicalRow(row);validateEntityHierarchy(row.identity,row.entity)}catch{metricSupportValid=false;timezoneValid=false;fxValid=false;continue}
    const key=rowKey(row);if(rowKeys.has(key))duplicates++;else rowKeys.add(key);
    timezoneValid=timezoneValid&&Boolean(row.time.source_timezone&&row.time.time_engine_version&&row.identity.date===row.time.business_date);
    fxValid=fxValid&&Boolean(Number.isFinite(row.currency.fx_rate)&&row.currency.fx_rate>0&&isDateOnly(row.currency.fx_rate_date)&&row.currency.fx_provider&&row.currency.fx_engine_version);
  }
  const latestCompleted=[...actual.values()].filter(item=>expectedKeys.has(checkpointKey(item))&&item.status==="completed").map(item=>item.business_date).sort().at(-1)||null;
  const freshnessDays=latestCompleted===null?null:dateDays(latestCompleted,as_of_business_date),freshnessValid=freshnessDays!==null&&freshnessDays>=0&&freshnessDays<=MAX_FRESHNESS_DAYS;
  const gates=Object.freeze({completeness:expectedKeys.size>0&&completed===expectedKeys.size,duplicates:duplicates===0,metric_support:metricSupportValid,timezone:timezoneValid,fx:fxValid,freshness:freshnessValid});
  const passed=Object.values(gates).every(Boolean);
  return Object.freeze({evidence_version:"e9-t5-v1",status:passed?"PASS":"FAIL",expected_checkpoints:expectedKeys.size,completed_checkpoints:completed,canonical_rows:rows.length,duplicate_rows:duplicates,latest_completed_business_date:latestCompleted,freshness_days:freshnessDays,max_freshness_days:MAX_FRESHNESS_DAYS,gates});
}
module.exports=Object.freeze({MAX_FRESHNESS_DAYS,checkpointKey,rowKey,evaluateBackfillDataReadiness});
