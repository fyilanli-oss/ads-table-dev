"use strict";

const CONTRACT_VERSION="codex-readonly-v1";
const OPERATIONS=Object.freeze(["health","dataset-v2-contract","dataset-v2-access-boundary","dataset-v2-safe-counts","migration-inventory"]);
const SENSITIVE_KEY=/(^|_)(user|account|entity|email|ip|token|secret|credential|ciphertext|oauth|raw|uri|url|key|row)(_|$)/i;

const SCHEMAS={
  health:{serviceStatus:"string",readOnly:"boolean",contractVersion:"string",timestamp:"string",supabaseConnectivity:"boolean"},
  "dataset-v2-contract":{expectedContractVersion:"string",repositoryMigrationVersions:["string"],canonicalColumns:["string"],nullability:{required:["string"],nullable:["string"]},expectedConstraintNames:["string"],expectedIndexNames:["string"],runtimeTableReachability:"boolean",catalogMetadata:"string",driftAssessment:"string",capabilityLimitation:"string"},
  "dataset-v2-access-boundary":{gatewayAuthenticated:"boolean",serviceClientConfigured:"boolean",datasetV2SelectReachable:"boolean",writeOperationExposed:"boolean",arbitraryQueryExposed:"boolean",sensitiveRelationExposed:"boolean"},
  "dataset-v2-safe-counts":{datasetV2Rows:"integer"},
  "migration-inventory":{repositoryMigrations:[{version:"string",fileName:"string"}],liveLedgerAvailable:"boolean",capabilityLimitation:"string"}
};

function validateValue(value,schema,path){
  if(Array.isArray(schema)){
    if(!Array.isArray(value))throw new Error(`Invalid contract at ${path}`);
    for(const item of value)validateValue(item,schema[0],`${path}[]`);
    return;
  }
  if(typeof schema==="object"){
    if(!value||typeof value!=="object"||Array.isArray(value))throw new Error(`Invalid contract at ${path}`);
    const expected=Object.keys(schema),actual=Object.keys(value);
    if(actual.length!==expected.length||actual.some(key=>!Object.hasOwn(schema,key)))throw new Error(`Unexpected contract field at ${path}`);
    for(const key of expected)validateValue(value[key],schema[key],`${path}.${key}`);
    return;
  }
  if(schema==="integer"){
    if(!Number.isSafeInteger(value)||value<0)throw new Error(`Invalid contract at ${path}`);
  }else if(typeof value!==schema)throw new Error(`Invalid contract at ${path}`);
}

function assertNoSensitiveFields(value,schema,path="response"){
  if(Array.isArray(value)){for(const item of value)assertNoSensitiveFields(item,Array.isArray(schema)?schema[0]:undefined,path);return;}
  if(!value||typeof value!=="object")return;
  for(const [key,item] of Object.entries(value)){
    if(SENSITIVE_KEY.test(key))throw new Error(`Sensitive response field at ${path}`);
    if(!schema||!Object.hasOwn(schema,key))throw new Error(`Unapproved response field at ${path}`);
    assertNoSensitiveFields(item,schema[key],`${path}.${key}`);
  }
}

function validateOperationResponse(operation,value){
  const schema=SCHEMAS[operation];
  if(!schema)throw new Error("Unknown response contract");
  assertNoSensitiveFields(value,schema);
  validateValue(value,schema,"response");
  return value;
}

module.exports={CONTRACT_VERSION,OPERATIONS,SCHEMAS,SENSITIVE_KEY,validateOperationResponse};
