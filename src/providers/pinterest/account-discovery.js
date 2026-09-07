"use strict";

function text(value){return typeof value==="string"?value.trim():"";}
function pinterestAdAccountIds(payload){
  const rows=Array.isArray(payload?.items)?payload.items:Array.isArray(payload?.data)?payload.data:[];
  return Object.freeze([...new Set(rows.map(row=>text(row?.id||row?.ad_account_id)).filter(Boolean))]);
}
function discoverPinterestAdAccounts(payload){
  const rows=Array.isArray(payload?.items)?payload.items:Array.isArray(payload?.data)?payload.data:[];
  const seen=new Set(),accounts=[];
  for(const row of rows){
    if(!row||typeof row!=="object"||Array.isArray(row))continue;
    const id=text(row.id||row.ad_account_id),name=text(row.name||row.account_name),currency=text(row.currency||row.currency_code).toUpperCase(),timezone=text(row.time_zone||row.timezone||row.timezone_name);
    if(!id||!name||!currency||!timezone||seen.has(id))continue;
    seen.add(id);accounts.push(Object.freeze({platform:"pinterest",platform_account_id:id,account_name:name,currency,timezone}));
  }
  return Object.freeze(accounts);
}

module.exports=Object.freeze({pinterestAdAccountIds,discoverPinterestAdAccounts});
