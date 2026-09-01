'use strict';

const REQUIRED_CODE_GATES=Object.freeze(['delivery_adapter','synthetic_isolation','time_fx','dataset_writer','refresh_runner','delivery_parity','shadow_dual_write']);
const MIN_LIVE_PARITY_PASSES=3;
function bool(value,field){if(typeof value!=='boolean')throw new TypeError(`${field} must be boolean`);return value;}
function count(value,field){if(!Number.isSafeInteger(value)||value<0)throw new TypeError(`${field} must be a non-negative integer`);return value;}

function evaluateTikTokActivationReadiness(input={}){
  const code=input.code_gates||{};for(const gate of REQUIRED_CODE_GATES)bool(code[gate],`code_gates.${gate}`);
  const mainChecks=bool(input.main_checks_passed,'main_checks_passed'),rollbackReady=bool(input.rollback_ready,'rollback_ready'),shadowApproval=bool(input.shadow_rollout_approved,'shadow_rollout_approved'),runtimeRegistered=bool(input.shadow_runtime_registered,'shadow_runtime_registered'),primaryApproval=bool(input.primary_activation_approved,'primary_activation_approved'),syntheticZero=bool(input.live_synthetic_writes_zero,'live_synthetic_writes_zero');
  const runs=count(input.live_shadow_runs,'live_shadow_runs'),passes=count(input.live_parity_passes,'live_parity_passes');if(passes>runs)throw new Error('live_parity_passes cannot exceed live_shadow_runs');
  const reasons=[];if(REQUIRED_CODE_GATES.some(gate=>!code[gate]))reasons.push('CODE_GATES_INCOMPLETE');if(!mainChecks)reasons.push('MAIN_CHECKS_REQUIRED');if(!rollbackReady)reasons.push('ROLLBACK_NOT_READY');if(!shadowApproval)reasons.push('SHADOW_ROLLOUT_APPROVAL_REQUIRED');if(!runtimeRegistered)reasons.push('SHADOW_RUNTIME_NOT_REGISTERED');if(runs<MIN_LIVE_PARITY_PASSES||passes<MIN_LIVE_PARITY_PASSES||passes!==runs)reasons.push('LIVE_PARITY_EVIDENCE_REQUIRED');if(!syntheticZero)reasons.push('LIVE_SYNTHETIC_ZERO_REQUIRED');if(!primaryApproval)reasons.push('PRIMARY_ACTIVATION_APPROVAL_REQUIRED');
  const codeReady=!reasons.some(reason=>['CODE_GATES_INCOMPLETE','MAIN_CHECKS_REQUIRED','ROLLBACK_NOT_READY'].includes(reason));
  const shadowReady=codeReady&&shadowApproval&&runtimeRegistered;
  const liveReady=shadowReady&&runs>=MIN_LIVE_PARITY_PASSES&&passes===runs&&syntheticZero;
  return Object.freeze({evidence_version:'e6-tiktok-activation-readiness-v1',status:liveReady&&primaryApproval?'READY':'BLOCKED',code_ready:codeReady,shadow_ready:shadowReady,live_evidence_ready:liveReady,minimum_live_parity_passes:MIN_LIVE_PARITY_PASSES,live_shadow_runs:runs,live_parity_passes:passes,live_synthetic_writes_zero:syntheticZero,primary_activation_approved:primaryApproval,production_activation_performed:false,reason_codes:Object.freeze(reasons)});
}

module.exports=Object.freeze({MIN_LIVE_PARITY_PASSES,REQUIRED_CODE_GATES,evaluateTikTokActivationReadiness});
