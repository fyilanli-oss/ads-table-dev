"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const vm = require("node:vm");
const {createKlaviyoAccountSelection, planCost} = require("../src/shopify/klaviyo-account-selection");
const {createWorkspaceProviderConnectionStore} = require("../src/shopify/workspace-provider-connection-store");
const {registerShopifyKlaviyoAccountRoutes} = require("../src/routes/shopify-klaviyo-account-routes");
const {createEmbeddedOAuthReturn} = require("../src/shopify/embedded-oauth-return");
const {registerShopifyProviderOAuthRoutes, PROVIDERS} = require("../src/routes/shopify-provider-oauth-routes");
const {initializeKlaviyoAccounts} = require("../src/shopify/klaviyo-account-ui");

const authority = {authority: "shopify_verified_session", workspace_id: "workspace-a", shop_id: "shop-a"};
const payload = {data: [{id: "account-a", attributes: {contact_information: {organization_name: "Verified account"}, preferred_currency: "USD"}}]};
const response = (status, body) => ({status, ok: status >= 200 && status < 300, json: async () => body});

function fixture() {
  const calls = [];
  const connection = {status: "pending_account_selection", accessToken: "secret-access", refreshToken: "secret-refresh", updated_at: "version-1"};
  const store = {
    readKlaviyo: async auth => { assert.deepEqual(auth, authority); return connection; },
    readKlaviyoStatus: async auth => { assert.deepEqual(auth, authority); return connection; },
    completeKlaviyo: async input => calls.push(input),
    refreshKlaviyo: async input => {calls.push(input); Object.assign(connection, {accessToken: input.accessToken, updated_at: "version-2"});},
  };
  const fetchImpl = async (url, options) => {
    assert.equal(url, "https://a.klaviyo.com/api/accounts/");
    assert.equal(options.headers.Authorization, "Bearer secret-access");
    return response(200, payload);
  };
  return {store, connection, calls, selection: createKlaviyoAccountSelection({store, fetchImpl})};
}

test("reads stored Klaviyo status without contacting the provider or exposing account identity", async () => {
  let contacted = false;
  const store = {readKlaviyoStatus: async () => ({status: "connected", email_monthly_plan_cost: "5.00", account_currency: "USD"})};
  const selection = createKlaviyoAccountSelection({store, fetchImpl: async () => {contacted = true;}});
  assert.deepEqual(await selection.status(authority), {status: "connected", email_monthly_plan_cost: "5.00", currency: "USD"});
  assert.equal(contacted, false);
});

test("stored Klaviyo status fails closed when cost or currency is incomplete", async () => {
  for (const row of [
    {status: "connected", email_monthly_plan_cost: null, account_currency: "USD"},
    {status: "connected", email_monthly_plan_cost: "5.00", account_currency: null},
  ]) {
    const selection = createKlaviyoAccountSelection({store: {readKlaviyoStatus: async () => row}, fetchImpl: async () => assert.fail("provider must not be contacted")});
    assert.deepEqual(await selection.status(authority), {status: "temporarily_unavailable"});
  }
});

test("lists verified accounts without disclosing encrypted or plaintext credentials", async () => {
  const {selection} = fixture();
  const result = await selection.list(authority);
  assert.deepEqual(result.accounts, [{id: "account-a", name: "Verified account", currency: "USD"}]);
  assert.equal(result.status, "pending_account_selection");
  assert.doesNotMatch(JSON.stringify(result), /secret|token|envelope|version/);
});

test("save revalidates provider ownership and ignores client tenant/name/currency claims", async () => {
  const {selection, calls} = fixture();
  await assert.rejects(selection.complete(authority, {account_id: "another-account", email_monthly_plan_cost: "5"}), /INVALID_ACCOUNT/);
  assert.equal(calls.length, 0);
  const result = await selection.complete(authority, {account_id: "account-a", email_monthly_plan_cost: "0", workspace_id: "victim", currency: "EUR", name: "Fake"});
  assert.equal(result.status, "connected");
  assert.equal(result.currency, "USD");
  assert.equal(result.account_name, "Verified account");
  assert.equal(result.email_monthly_plan_cost, "0.00");
  assert.deepEqual(calls[0].authority, authority);
  assert.equal(calls[0].version, "version-1");
});

test("cost must be explicit, finite, non-negative, and at most two decimals", () => {
  for (const invalid of [undefined, null, 0, "", " ", "-1", "1e3", "NaN", "Infinity", "1.234", "100000000", "01"]) {
    assert.throws(() => planCost(invalid), /INVALID_PLAN_COST/);
  }
  assert.equal(planCost("12.3"), "12.30");
});

test("unavailable and revoked grants do not become connected", async () => {
  const {store, connection, calls} = fixture();
  connection.status = "revoked";
  let requested = false;
  const selection = createKlaviyoAccountSelection({store, fetchImpl: async () => {requested = true;}});
  assert.equal((await selection.list(authority)).status, "not_connected");
  await assert.rejects(selection.complete(authority, {account_id: "account-a", email_monthly_plan_cost: "10"}), /INVALID_ACCOUNT/);
  assert.equal(requested, false);
  assert.equal(calls.length, 0);
});

test("expired access token refreshes once, persists encrypted store update, and uses new version for save", async () => {
  const {store, calls} = fixture();
  let requests = 0;
  const selection = createKlaviyoAccountSelection({store, clientId: "client", clientSecret: "secret", fetchImpl: async (url, options) => {
    requests++;
    if (requests === 1) return response(401, {});
    if (requests === 2) {
      assert.equal(url, "https://a.klaviyo.com/oauth/token");
      assert.equal(new URLSearchParams(options.body).get("refresh_token"), "secret-refresh");
      return response(200, {access_token: "new-access", refresh_token: "new-refresh"});
    }
    assert.equal(options.headers.Authorization, "Bearer new-access");
    return response(200, payload);
  }});
  await selection.complete(authority, {account_id: "account-a", email_monthly_plan_cost: "4.99"});
  assert.equal(requests, 3);
  assert.equal(calls[0].refreshToken, "new-refresh");
  assert.equal(calls[1].version, "version-2");
});

test("provider 401 and 429 produce recoverable errors without retry loops", async () => {
  for (const [status, code] of [[401, "KLAVIYO_REAUTHORIZE"], [429, "KLAVIYO_UNAVAILABLE"]]) {
    const {store} = fixture();
    let calls = 0;
    const selection = createKlaviyoAccountSelection({store, fetchImpl: async () => {calls++; return response(status, {secret: "raw"});}});
    await assert.rejects(selection.list(authority), error => error.code === code);
    assert.equal(calls, 1);
  }
});

test("R5 read-only verification never refreshes or writes and returns no account identity", async () => {
  const connection = {
    status: "connected",
    active_account_id: "account-a",
    account_currency: "USD",
    accessToken: "secret-access",
    refreshToken: "secret-refresh",
    updated_at: "version-1",
  };
  let refreshCalls = 0;
  let requests = 0;
  const store = {
    readKlaviyo: async () => connection,
    refreshKlaviyo: async () => { refreshCalls++; },
  };
  const verified = createKlaviyoAccountSelection({store, clientId: "client", clientSecret: "secret", fetchImpl: async () => {
    requests++;
    return response(200, payload);
  }});
  const result = await verified.verifyReadOnly(authority);
  assert.deepEqual(result, {status: "verified", active_account_verified: true, currency: "USD"});
  assert.doesNotMatch(JSON.stringify(result), /account-a|secret|token|envelope/i);
  assert.equal(requests, 1);
  assert.equal(refreshCalls, 0);

  const expired = createKlaviyoAccountSelection({store, clientId: "client", clientSecret: "secret", fetchImpl: async () => {
    requests++;
    return response(401, {});
  }});
  await assert.rejects(expired.verifyReadOnly(authority), error => error.code === "KLAVIYO_READ_ONLY_VERIFICATION_EXPIRED");
  assert.equal(refreshCalls, 0);
});

test("persistence scopes updates to verified shop/workspace and rejects a concurrent reconnect", async () => {
  const filters = [];
  let mutation;
  const query = {eq(k,v) {filters.push([k,v]); return this;}, neq(k,v) {filters.push([k,v]); return this;}, select() {return this;}, maybeSingle: async () => ({data: null, error: null})};
  const store = createWorkspaceProviderConnectionStore({client: {from: () => ({update: row => {mutation = row; return query;}})}, vault: {encrypt: x=>x, decrypt: x=>x}});
  await assert.rejects(store.completeKlaviyo({authority, version: "old-version", account: {id: "a", currency: "USD"}, cost: "1.00"}), /CONNECTION_CHANGED/);
  assert.deepEqual(filters, [["workspace_id", "workspace-a"], ["shop_id", "shop-a"], ["provider", "klaviyo"], ["updated_at", "old-version"], ["status", "revoked"]]);
  assert.equal(mutation.email_monthly_plan_cost, "1.00");
});

test("account routes require a Shopify bearer session and redact unexpected errors", async () => {
  const routes = {};
  const app = {get: (p,h)=>routes[p]=h, post: (p,h)=>routes[p]=h};
  let authenticated = false;
  registerShopifyKlaviyoAccountRoutes(app, {authenticateEmbedded: async ({session_token}) => {assert.equal(session_token, "session"); authenticated=true; return authority;}, selection: {list: async () => {throw new Error("secret-provider-response");}}});
  const res = {set() {}, status(n) {this.code=n; return this;}, json(body) {this.body=body;}};
  await routes["/api/shopify/providers/klaviyo/accounts"]({get:()=>null}, res);
  assert.equal(res.code, 401);
  assert.equal(authenticated, false);
  await routes["/api/shopify/providers/klaviyo/accounts"]({get:()=>"Bearer session"}, res);
  assert.equal(res.code, 503);
  assert.deepEqual(res.body, {code:"KLAVIYO_UNAVAILABLE"});
});

test("R5 verification route stays session-bound and calls the read-only operation", async () => {
  const routes = {};
  const app = {get: (path, handler) => { routes[path] = handler; }, post() {}};
  let receivedAuthority;
  registerShopifyKlaviyoAccountRoutes(app, {
    authenticateEmbedded: async () => authority,
    selection: {verifyReadOnly: async input => { receivedAuthority = input; return {status: "verified", active_account_verified: true, currency: "USD"}; }},
  });
  const res = {set() {}, status(code) {this.code = code; return this;}, json(body) {this.body = body; return body;}};
  await routes["/api/shopify/providers/klaviyo/accounts/verify"]({get: () => "Bearer session"}, res);
  assert.deepEqual(receivedAuthority, authority);
  assert.equal(res.code, 200);
  assert.deepEqual(res.body, {status: "verified", active_account_verified: true, currency: "USD"});
});

test("OAuth returns to installed Shopify shop and rejects external redirect targets", async () => {
  const filters=[];
  const query={select(){return this;},eq(k,v){filters.push([k,v]);return this;},maybeSingle:async()=>({data:{shop_domain:"verified.myshopify.com"},error:null})};
  const resolve=createEmbeddedOAuthReturn({client:{from:()=>query},clientId:"client-id"});
  const target=await resolve({...authority,shop_domain:"evil.example"});
  assert.equal(target,"https://verified.myshopify.com/admin/apps/client-id");
  assert.deepEqual(filters,[["shop_id","shop-a"],["workspace_id","workspace-a"],["status","active"]]);
  const routes={};
  let destination=target;
  registerShopifyProviderOAuthRoutes({get:(p,h)=>routes[p]=h,post(){}},{adapters:Object.fromEntries(PROVIDERS.map(p=>[p,{start:async()=>{},callback:async()=>({redirect_to:destination})}]))});
  let redirected;
  const call=()=>routes["/api/shopify/providers/:provider/oauth/callback"]({params:{provider:"klaviyo"},query:{state:"s",code:"c"}},{redirect:url=>redirected=url});
  await call(); assert.equal(redirected,target);
  destination="https://evil.example/";
  await call(); assert.equal(redirected,"/shopify/app/platforms?oauth_error=connection_failed");
});

test("UI requires account choice then explicit cost save and prevents duplicate submits", async () => {
  const elements=new Map();
  const element=()=>({hidden:false,value:"",textContent:"",events:{},children:[],setAttribute(k,v){this[k]=v;},addEventListener(k,v){this.events[k]=v;},replaceChildren(){this.children=[];},append(x){this.children.push(x);}});
  for(const id of ["klaviyo-accounts","klaviyo-message","klaviyo-choice","klaviyo-choice-step","klaviyo-choose","klaviyo-cost-step","klaviyo-cost","klaviyo-save","klaviyo-retry","klaviyo-retry-step","klaviyo-connect"]) elements.set(id,element());
  const requests=[];
  let finishSave;
  const context={URLSearchParams,location:{search:"?oauth_connected=klaviyo&account_selection_required=1"},document:{getElementById:id=>elements.get(id),querySelector:()=>elements.get("connect"),createElement:element},window:{shopify:{idToken:async()=>"session"}},fetch:async(url,options)=>{
    requests.push({url,options});
    if(options.method==="POST") return new Promise(resolve=>{finishSave=()=>resolve(response(200,{status:"connected",account_name:"Verified",currency:"USD",email_monthly_plan_cost:"0.00"}));});
    return response(200,{status:"pending_account_selection",accounts:[{id:"a",name:"<script>untrusted</script>",currency:"USD"}]});
  }};
  vm.runInNewContext(`(${initializeKlaviyoAccounts.toString()})()`,context);
  await new Promise(resolve=>setImmediate(resolve));
  assert.equal(elements.get("klaviyo-cost-step").hidden,true);
  assert.equal(elements.get("klaviyo-choice").children[0].textContent,"<script>untrusted</script> (a)");
  elements.get("klaviyo-choose").events.click();
  assert.equal(elements.get("klaviyo-cost-step").hidden,false);
  elements.get("klaviyo-cost").value="0";
  const saving=elements.get("klaviyo-save").events.click();
  await elements.get("klaviyo-save").events.click();
  await new Promise(resolve=>setImmediate(resolve));
  assert.equal(requests.filter(r=>r.options.method==="POST").length,1);
  assert.deepEqual(JSON.parse(requests[1].options.body),{account_id:"a",email_monthly_plan_cost:"0"});
  finishSave(); await saving;
  assert.match(elements.get("klaviyo-message").textContent,/Connected: Verified/);
});

test("UI reads stored status on page load without requesting Klaviyo accounts", async () => {
  const elements=new Map();
  const element=()=>({hidden:false,value:"",textContent:"",events:{},children:[],setAttribute(k,v){this[k]=v;},addEventListener(k,v){this.events[k]=v;},replaceChildren(){this.children=[];},append(x){this.children.push(x);}});
  for(const id of ["klaviyo-accounts","klaviyo-message","klaviyo-choice","klaviyo-choice-step","klaviyo-choose","klaviyo-cost-step","klaviyo-cost","klaviyo-save","klaviyo-retry","klaviyo-retry-step","klaviyo-connect"]) elements.set(id,element());
  const requests=[];
  const context={URLSearchParams,location:{search:""},document:{getElementById:id=>elements.get(id),createElement:element},window:{shopify:{idToken:async()=>"session"}},fetch:async(url,options)=>{
    requests.push({url,options});
    return response(200,{status:"connected",email_monthly_plan_cost:"5.00",currency:"USD"});
  }};
  vm.runInNewContext(`(${initializeKlaviyoAccounts.toString()})()`,context);
  await new Promise(resolve=>setImmediate(resolve));
  assert.deepEqual(requests.map(item=>item.url),["/api/shopify/providers/klaviyo/accounts/status"]);
  assert.equal(elements.get("klaviyo-connect").hidden,true);
  assert.equal(elements.get("klaviyo-message").textContent,"Connected · 5.00 USD/month");
});

test("R5 operator parameter invokes only the no-refresh verification route", async () => {
  const elements = new Map();
  const element = () => ({hidden:false,value:"",textContent:"",events:{},children:[],setAttribute(k,v){this[k]=v;},addEventListener(k,v){this.events[k]=v;},replaceChildren(){this.children=[];},append(x){this.children.push(x);}});
  for (const id of ["klaviyo-accounts","klaviyo-message","klaviyo-choice","klaviyo-choice-step","klaviyo-choose","klaviyo-cost-step","klaviyo-cost","klaviyo-save","klaviyo-retry","klaviyo-retry-step","klaviyo-connect"]) elements.set(id,element());
  const requests = [];
  const context = {URLSearchParams,location:{search:"?r5_read_only_verify=1"},document:{getElementById:id=>elements.get(id),createElement:element},window:{shopify:{idToken:async()=>"session"}},fetch:async(url,options)=>{
    requests.push({url,options});
    return response(200,{status:"verified",active_account_verified:true,currency:"USD"});
  }};
  vm.runInNewContext(`(${initializeKlaviyoAccounts.toString()})()`,context);
  await new Promise(resolve=>setImmediate(resolve));
  assert.deepEqual(requests.map(item=>item.url),["/api/shopify/providers/klaviyo/accounts/verify"]);
  assert.equal(requests[0].options.method,"GET");
  assert.equal(elements.get("klaviyo-message").textContent,"Connected · Klaviyo account verified · USD");
});
