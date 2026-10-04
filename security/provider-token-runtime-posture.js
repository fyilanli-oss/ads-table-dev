"use strict";

const {isProductionRuntime}=require("./production-config");
const {parseKeyring}=require("./provider-token-vault");

const BOOLEAN_NAMES=Object.freeze([
  "PROVIDER_TOKEN_ENCRYPTION_ENABLED",
  "PROVIDER_TOKEN_LEGACY_READ_ENABLED",
]);

class ProviderTokenRuntimePostureError extends Error{
  constructor(reasonCodes){
    super("Production provider token runtime posture is not safe");
    this.name="ProviderTokenRuntimePostureError";
    this.code="PROVIDER_TOKEN_RUNTIME_POSTURE_UNSAFE";
    this.reasonCodes=Object.freeze([...reasonCodes]);
  }
}

function inspectBoolean(env,name){
  const present=env[name]!==undefined&&env[name]!==null&&String(env[name]).trim()!=="";
  if(!present)return Object.freeze({present:false,valid:false,enabled:null});
  const normalized=String(env[name]).trim().toLowerCase();
  if(normalized==="true"||normalized==="1")return Object.freeze({present:true,valid:true,enabled:true});
  if(normalized==="false"||normalized==="0")return Object.freeze({present:true,valid:true,enabled:false});
  return Object.freeze({present:true,valid:false,enabled:null});
}

function inspectProviderTokenRuntimePosture(env={}){
  if(!env||typeof env!=="object"||Array.isArray(env))throw new TypeError("env must be an object");
  const production=isProductionRuntime(env);
  const encryption=inspectBoolean(env,"PROVIDER_TOKEN_ENCRYPTION_ENABLED");
  const legacyReads=inspectBoolean(env,"PROVIDER_TOKEN_LEGACY_READ_ENABLED");
  const supabaseServerCredentialsPresent=Boolean(
    String(env.SUPABASE_URL||"").trim()&&String(env.SUPABASE_SERVICE_ROLE_KEY||"").trim()
  );
  let keyringValid=false;
  try{
    parseKeyring(env);
    keyringValid=true;
  }catch{
    keyringValid=false;
  }
  const reasons=[];
  if(!encryption.present)reasons.push("ENCRYPTION_FLAG_MISSING");
  else if(!encryption.valid)reasons.push("ENCRYPTION_FLAG_INVALID");
  else if(!encryption.enabled)reasons.push("ENCRYPTION_NOT_ENABLED");
  if(!legacyReads.present)reasons.push("LEGACY_READ_FLAG_MISSING");
  else if(!legacyReads.valid)reasons.push("LEGACY_READ_FLAG_INVALID");
  else if(legacyReads.enabled)reasons.push("LEGACY_READS_ENABLED");
  if(!keyringValid)reasons.push("KEYRING_INVALID_OR_INCOMPLETE");
  if(!supabaseServerCredentialsPresent)reasons.push("SUPABASE_SERVER_CREDENTIALS_MISSING");
  const securePosture=reasons.length===0;
  return Object.freeze({
    event:"PROVIDER_TOKEN_RUNTIME_POSTURE",
    production,
    enforcement_required:production,
    secure_posture:securePosture,
    startup_allowed:!production||securePosture,
    configuration:Object.freeze({
      encryption_flag_present:encryption.present,
      encryption_flag_valid:encryption.valid,
      encryption_enabled:encryption.enabled,
      legacy_read_flag_present:legacyReads.present,
      legacy_read_flag_valid:legacyReads.valid,
      legacy_reads_enabled:legacyReads.enabled,
      keyring_valid:keyringValid,
      supabase_server_credentials_present: supabaseServerCredentialsPresent,
    }),
    reason_codes:Object.freeze(reasons),
  });
}

function assertProductionProviderTokenPosture(env=process.env){
  const posture=inspectProviderTokenRuntimePosture(env);
  if(!posture.startup_allowed)throw new ProviderTokenRuntimePostureError(posture.reason_codes);
  return posture;
}

module.exports={
  BOOLEAN_NAMES,
  ProviderTokenRuntimePostureError,
  inspectProviderTokenRuntimePosture,
  assertProductionProviderTokenPosture,
};
