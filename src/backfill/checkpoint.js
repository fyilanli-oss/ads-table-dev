"use strict";

const STATUSES=Object.freeze(["queued","running","completed","failed","skipped"]);
const TERMINAL=new Set(["completed","skipped"]);
const TRANSITIONS=Object.freeze({queued:Object.freeze(["running","skipped"]),running:Object.freeze(["running","completed","failed"]),failed:Object.freeze(["running","skipped"]),completed:Object.freeze([]),skipped:Object.freeze([])});
function text(value,name){const clean=typeof value==="string"?value.trim():"";if(!clean)throw new TypeError(`${name} is required`);return clean}
function cursor(value){if(value===null||value===undefined||value==="")return null;const clean=text(value,"cursor");if(clean.length>4096)throw new RangeError("cursor exceeds 4096 characters");return clean}
function checkpointIdentity({user_id,platform,platform_account_id,business_date,date_key}={}){return Object.freeze({user_id:text(user_id,"user_id"),platform:text(platform,"platform"),platform_account_id:text(platform_account_id,"platform_account_id"),business_date:text(business_date,"business_date"),date_key:text(date_key,"date_key")})}
function createCheckpoint(unit){const identity=checkpointIdentity(unit);return Object.freeze({...identity,finality:text(unit.finality,"finality"),priority:Number(unit.priority),status:"queued",cursor:null,attempt_count:0,last_error_code:null})}
function transitionCheckpoint(current,nextStatus,{next_cursor=null,error_code=null}={}){
  if(!current||!STATUSES.includes(current.status))throw new TypeError("valid current checkpoint is required");
  if(!STATUSES.includes(nextStatus)||!TRANSITIONS[current.status].includes(nextStatus))throw new Error(`checkpoint transition ${current.status} -> ${nextStatus} is not allowed`);
  const nextCursor=cursor(next_cursor),errorCode=error_code===null?null:text(error_code,"error_code");
  if(nextStatus==="completed"&&nextCursor!==null)throw new Error("completed checkpoint cannot retain a cursor");
  if(nextStatus==="failed"&&!errorCode)throw new Error("failed checkpoint requires an error_code");
  return Object.freeze({...current,status:nextStatus,cursor:nextCursor,attempt_count:current.attempt_count+(nextStatus==="running"?1:0),last_error_code:nextStatus==="failed"?errorCode:null});
}
function shouldResume(checkpoint){return Boolean(checkpoint&&!TERMINAL.has(checkpoint.status))}
module.exports=Object.freeze({STATUSES,checkpointIdentity,createCheckpoint,transitionCheckpoint,shouldResume});
