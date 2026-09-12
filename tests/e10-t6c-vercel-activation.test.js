"use strict";
const fs=require("node:fs"),path=require("node:path"),test=require("node:test"),assert=require("node:assert/strict");
const source=fs.readFileSync(path.join(__dirname,"../scripts/e10-t6c-vercel-activation.sh"),"utf8");
test("activation operator requires exact approval and authentication before contact",()=>{assert.match(source,/E10_T6C_CONFIRMATION_INVALID/);assert.match(source,/VERCEL_AUTH_UNAVAILABLE/);assert.ok(source.indexOf("VERCEL_AUTH_UNAVAILABLE")<source.indexOf("vercel@59.16.0 link"));});
test("operator preflights before one flag write and production deploy",()=>{const pre=source.indexOf("e10-t6c-activation-preflight.js"),write=source.indexOf("env add SHOPIFY_EMBEDDED_PROVIDER_OAUTH_ENABLED production"),deploy=source.indexOf("deploy --prod");assert.ok(pre>0&&write>pre&&deploy>write);assert.equal((source.match(/env add SHOPIFY_EMBEDDED_PROVIDER_OAUTH_ENABLED/g)||[]).length,1);assert.match(source,/trap cleanup EXIT/);});
