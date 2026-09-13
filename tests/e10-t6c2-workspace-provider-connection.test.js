"use strict";
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const {createWorkspaceProviderConnectionStore} = require("../src/shopify/workspace-provider-connection-store");

function clientDouble() {
  const calls=[];
  return {calls, from(table) { return {upsert: async(row, options)=>(calls.push({table,row,options}),{error:null})}; }};
}
const vault={encrypt:(value,context)=>value?{sealed:value,context}:null,decrypt:(value,context)=>value?{value:value.sealed,context}:null};
const transaction={surface:"shopify_embedded",user_id:null,shop_id:"shop-1",workspace_id:"workspace-1",shopify_user_id:"merchant-1",provider:"meta",return_target:"/shopify/app/platforms"};

test("OAuth result is encrypted and written only under consumed workspace authority", async()=>{
  const client=clientDouble();
  const store=createWorkspaceProviderConnectionStore({client,vault,now:()=>new Date("2026-09-11T15:00:00.000Z")});
  const result=await store.writeFromOAuthTransaction({transaction,accessToken:"access",refreshToken:"refresh"});
  assert.deepEqual(result,{workspace_id:"workspace-1",provider:"meta",status:"pending_account_selection"});
  const call=client.calls[0];
  assert.equal(call.table,"shopify_workspace_provider_connections");
  assert.equal(call.options.onConflict,"workspace_id,provider");
  assert.equal(call.row.access_token_envelope.sealed,"access");
  assert.equal(call.row.access_token_envelope.context.userId,"workspace:workspace-1");
  assert.equal(JSON.stringify(call.row).includes("shopify_user_id"),false);
});

test("standalone, caller-shaped, and open-return transactions fail before persistence", async()=>{
  const client=clientDouble(); const store=createWorkspaceProviderConnectionStore({client,vault});
  await assert.rejects(()=>store.writeFromOAuthTransaction({transaction:{...transaction,surface:"standalone"},accessToken:"x"}),/EMBEDDED_OAUTH_TRANSACTION_REQUIRED/);
  await assert.rejects(()=>store.writeFromOAuthTransaction({transaction:{...transaction,workspace_id:""},accessToken:"x"}),/transaction.workspace_id/);
  await assert.rejects(()=>store.writeFromOAuthTransaction({transaction:{...transaction,return_target:"https://evil.example"},accessToken:"x"}),/INVALID_EMBEDDED_RETURN_TARGET/);
  assert.equal(client.calls.length,0);
});

test("schema is forced-RLS, service-role only, and workspace-installation bound",()=>{
  const sql=fs.readFileSync(path.join(__dirname,"..","supabase/migrations/20260911150000_create_shopify_workspace_provider_connections.sql"),"utf8");
  assert.match(sql,/primary key \(workspace_id, provider\)/i);
  assert.match(sql,/foreign key \(shop_id, workspace_id\)[\s\S]*references public\.shopify_installations/i);
  assert.match(sql,/enable row level security[\s\S]*force row level security/i);
  assert.match(sql,/revoke all[\s\S]*from public, anon, authenticated/i);
  assert.match(sql,/grant select, insert, update, delete[\s\S]*to service_role/i);
});
