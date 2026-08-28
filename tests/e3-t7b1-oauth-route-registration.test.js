"use strict";
const assert = require("node:assert/strict");
const { test } = require("node:test");
const { registerOAuthProviderRoutes } = require("../src/oauth/provider-routes");
function app(){const routes=[];return{routes,get(path,handler){routes.push({path,handler});return this;}}}
test("registers canonical start and callback routes in order",()=>{const target=app(),start=()=>{},callback=()=>{};registerOAuthProviderRoutes({app:target,provider:"meta",startHandler:start,callbackHandler:callback});assert.deepEqual(target.routes,[{path:"/auth/meta",handler:start},{path:"/auth/meta/callback",handler:callback}]);});
test("fails closed for invalid dependencies",()=>{const valid={app:app(),provider:"meta",startHandler(){},callbackHandler(){}};assert.throws(()=>registerOAuthProviderRoutes(),/Express application/);assert.throws(()=>registerOAuthProviderRoutes({...valid,provider:"Meta Ads"}),/provider must be canonical/);assert.throws(()=>registerOAuthProviderRoutes({...valid,startHandler:null}),/startHandler/);assert.throws(()=>registerOAuthProviderRoutes({...valid,callbackHandler:null}),/callbackHandler/);});
