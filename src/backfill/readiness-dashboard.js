"use strict";

const PROVIDERS=Object.freeze(["meta","google","tiktok","klaviyo"]);
const READINESS_STATUSES=new Set(["PASS","FAIL"]),PARITY_STATUSES=new Set(["PASS","FAIL","NOT_AVAILABLE"]);
function platform(value){const clean=typeof value==="string"?value.trim().toLowerCase():"";if(!PROVIDERS.includes(clean))throw new Error("unsupported E9 dashboard provider");return clean}
function integer(value,field){if(!Number.isInteger(value)||value<0)throw new TypeError(`${field} must be a non-negative integer`);return value}
function buildProviderReadinessDashboard({readiness_evidence=[],parity_evidence=[]}={}){
  if(!Array.isArray(readiness_evidence)||!Array.isArray(parity_evidence))throw new TypeError("dashboard evidence must be arrays");
  const readiness=new Map(),parity=new Map();
  for(const item of readiness_evidence){const provider=platform(item?.platform);if(!READINESS_STATUSES.has(item?.status))throw new Error("invalid readiness status");const list=readiness.get(provider)||[];list.push(item);readiness.set(provider,list)}
  for(const item of parity_evidence){const provider=platform(item?.platform);if(!PARITY_STATUSES.has(item?.status))throw new Error("invalid parity status");const list=parity.get(provider)||[];list.push(item);parity.set(provider,list)}
  const rows=PROVIDERS.filter(provider=>readiness.has(provider)).map(provider=>{const checks=readiness.get(provider),comparisons=parity.get(provider)||[],readinessPassed=checks.length>0&&checks.every(item=>item.status==="PASS"),parityPassed=comparisons.length>0&&comparisons.every(item=>item.status==="PASS"),parityFailed=comparisons.some(item=>item.status==="FAIL");const status=!readinessPassed||parityFailed?"BLOCKED":parityPassed?"READY":"PARITY_PENDING";return Object.freeze({platform:provider,status,account_evidence:checks.length,readiness_passed:checks.filter(item=>item.status==="PASS").length,expected_checkpoints:checks.reduce((sum,item)=>sum+integer(item.expected_checkpoints,"expected_checkpoints"),0),completed_checkpoints:checks.reduce((sum,item)=>sum+integer(item.completed_checkpoints,"completed_checkpoints"),0),canonical_rows:checks.reduce((sum,item)=>sum+integer(item.canonical_rows,"canonical_rows"),0),duplicate_rows:checks.reduce((sum,item)=>sum+integer(item.duplicate_rows,"duplicate_rows"),0),parity_evidence:comparisons.length,parity_passed:comparisons.filter(item=>item.status==="PASS").length})});
  if(rows.length===0)throw new Error("dashboard requires readiness evidence");
  const ready=rows.filter(row=>row.status==="READY").length;
  return Object.freeze({evidence_version:"e9-t6-v1",status:ready===rows.length?"READY":"BLOCKED",providers_with_evidence:rows.length,providers_ready:ready,providers:Object.freeze(rows)});
}
module.exports=Object.freeze({PROVIDERS,buildProviderReadinessDashboard});
