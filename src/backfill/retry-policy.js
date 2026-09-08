"use strict";

const PROVIDER_BUDGETS=Object.freeze({
  meta:Object.freeze({max_in_flight:1,min_interval_ms:1000,max_attempts:3}),
  google:Object.freeze({max_in_flight:1,min_interval_ms:1000,max_attempts:3}),
  tiktok:Object.freeze({max_in_flight:1,min_interval_ms:1100,max_attempts:3}),
  klaviyo:Object.freeze({max_in_flight:1,min_interval_ms:1000,max_attempts:3})
});
const RETRYABLE_HTTP=new Set([429,500,502,503,504]);
function budgetFor(platform){const budget=PROVIDER_BUDGETS[String(platform||"").toLowerCase()];if(!budget)throw new Error("provider has no E9 retry budget");return budget}
function retryAfterMs(value,{now=Date.now()}={}){if(value===null||value===undefined||value==="")return null;const seconds=Number(value);if(Number.isFinite(seconds)&&seconds>=0)return Math.min(Math.ceil(seconds*1000),300000);const date=Date.parse(String(value));return Number.isFinite(date)?Math.min(Math.max(date-now,0),300000):null}
function retryDecision({platform,attempt,http_status,retry_after=null}={}){const budget=budgetFor(platform),number=Number(attempt),status=Number(http_status);if(!Number.isInteger(number)||number<1)throw new TypeError("attempt must be a positive integer");if(number>=budget.max_attempts||!RETRYABLE_HTTP.has(status))return Object.freeze({retry:false,delay_ms:0,reason:number>=budget.max_attempts?"attempt_budget_exhausted":"non_retryable"});const providerDelay=retryAfterMs(retry_after),exponential=Math.min(budget.min_interval_ms*(2**(number-1)),30000);return Object.freeze({retry:true,delay_ms:Math.max(providerDelay??0,exponential),reason:providerDelay===null?"bounded_backoff":"provider_retry_after"})}
function createProviderGate({now=()=>Date.now()}={}){const state=new Map();return Object.freeze({reserve(platform){const budget=budgetFor(platform),current=state.get(platform)||{in_flight:0,last_started_at:null};if(current.in_flight>=budget.max_in_flight)return Object.freeze({accepted:false,wait_ms:budget.min_interval_ms});const wait=current.last_started_at===null?0:Math.max(current.last_started_at+budget.min_interval_ms-now(),0);if(wait>0)return Object.freeze({accepted:false,wait_ms:wait});state.set(platform,{in_flight:current.in_flight+1,last_started_at:now()});return Object.freeze({accepted:true,wait_ms:0})},release(platform){const current=state.get(platform);if(!current||current.in_flight<1)throw new Error("provider gate release without reservation");state.set(platform,{...current,in_flight:current.in_flight-1})}})}
module.exports=Object.freeze({PROVIDER_BUDGETS,retryAfterMs,retryDecision,createProviderGate});
