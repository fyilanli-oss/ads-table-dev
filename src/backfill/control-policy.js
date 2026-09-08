"use strict";

const ACTIONS=Object.freeze(["pause","resume","cancel"]),CONTROL_STATES=Object.freeze(["active","paused","cancelled"]);
function applyBackfillControl(checkpoint,action){
  if(!checkpoint||typeof checkpoint!=="object"||Array.isArray(checkpoint))throw new TypeError("checkpoint is required");
  if(!ACTIONS.includes(action))throw new Error("unsupported backfill control action");
  const control=checkpoint.control_state||"active";if(!CONTROL_STATES.includes(control))throw new Error("invalid checkpoint control state");
  if(["completed","skipped"].includes(checkpoint.status)||control==="cancelled")throw new Error("terminal checkpoint cannot be controlled");
  if(action==="pause"){if(control!=="active")throw new Error("only active checkpoint can be paused");return Object.freeze({...checkpoint,status:checkpoint.status==="running"?"queued":checkpoint.status,control_state:"paused",lease_token:null,lease_expires_at:null})}
  if(action==="resume"){if(control!=="paused")throw new Error("only paused checkpoint can be resumed");return Object.freeze({...checkpoint,control_state:"active",lease_token:null,lease_expires_at:null})}
  return Object.freeze({...checkpoint,status:"skipped",control_state:"cancelled",lease_token:null,lease_expires_at:null,last_error_code:null});
}
function canClaimCheckpoint(checkpoint){return Boolean(checkpoint&&checkpoint.control_state!=="paused"&&checkpoint.control_state!=="cancelled"&&!["completed","skipped"].includes(checkpoint.status))}
module.exports=Object.freeze({ACTIONS,CONTROL_STATES,applyBackfillControl,canClaimCheckpoint});
