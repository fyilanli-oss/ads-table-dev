"use strict";
const ENDPOINT="https://dev.adstable.app/api/e10/activation-preflight";
async function run({env=process.env,fetchImpl=fetch}={}){
  const requestUrl=String(env.ACTIONS_ID_TOKEN_REQUEST_URL||""),requestToken=String(env.ACTIONS_ID_TOKEN_REQUEST_TOKEN||"");
  if(!requestUrl||!requestToken)throw new Error("ACTIONS_OIDC_UNAVAILABLE");
  const separator=requestUrl.includes("?")?"&":"?";
  const oidcResponse=await fetchImpl(`${requestUrl}${separator}audience=${encodeURIComponent(ENDPOINT)}`,{headers:{Authorization:`Bearer ${requestToken}`,Accept:"application/json"}});
  if(!oidcResponse.ok)throw new Error("ACTIONS_OIDC_REQUEST_FAILED");
  const oidc=await oidcResponse.json(); if(typeof oidc.value!=="string"||!oidc.value)throw new Error("ACTIONS_OIDC_RESPONSE_INVALID");
  const response=await fetchImpl(ENDPOINT,{method:"POST",headers:{Authorization:`Bearer ${oidc.value}`,Accept:"application/json"}});
  if(!response.ok)throw new Error("REMOTE_PREFLIGHT_REQUEST_FAILED");
  const result=await response.json();
  if(!result||result.contract_version!=="e10-t6c-activation-preflight-v1"||typeof result.ready!=="boolean"||result.values_or_lengths_exposed!==false||result.provider_contact!==false||result.production_contact!==false)throw new Error("REMOTE_PREFLIGHT_CONTRACT_INVALID");
  process.stdout.write(`${JSON.stringify(result,null,2)}\n`); if(!result.ready)throw new Error("REMOTE_PREFLIGHT_NOT_READY"); return result;
}
if(require.main===module)run().catch(error=>{process.stderr.write(`${error.message}\n`);process.exitCode=2;});
module.exports=Object.freeze({ENDPOINT,run});
