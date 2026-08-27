"use strict";

const assert = require("node:assert/strict");
const { after, before, test } = require("node:test");

process.env.VERCEL = "1";
delete process.env.TIKTOK_TEST_PAGE_ENABLED;

const app = require("../server");

let server;
let baseUrl;

before(async () => {
  server = app.listen(0, "127.0.0.1");
  await new Promise((resolve, reject) => {
    server.once("listening", resolve);
    server.once("error", reject);
  });
  const address = server.address();
  baseUrl = `http://127.0.0.1:${address.port}`;
});

after(async () => {
  if (!server) return;
  await new Promise((resolve, reject) => {
    server.close((error) => (error ? reject(error) : resolve()));
  });
});

test("serves the critical V1 public pages", async () => {
  const landing = await fetch(`${baseUrl}/`);
  assert.equal(landing.status, 200);
  assert.match(landing.headers.get("content-type"), /^text\/html\b/);
  assert.match(await landing.text(), /AdsTable/i);

  const login = await fetch(`${baseUrl}/login`);
  assert.equal(login.status, 200);
  assert.match(login.headers.get("content-type"), /^text\/html\b/);
});

test("keeps the public-config response shape stable", async () => {
  const response = await fetch(`${baseUrl}/api/public-config`);
  assert.equal(response.status, 200);
  assert.deepEqual(Object.keys(await response.json()).sort(), [
    "supabaseAnonKey",
    "supabaseUrl",
  ]);
});

test("rejects an unauthenticated critical API request", async () => {
  const response = await fetch(`${baseUrl}/api/account/status`);
  assert.equal(response.status, 401);
  assert.deepEqual(await response.json(), { error: "Not authenticated" });
});

test("keeps disabled and unknown surfaces fail-closed", async () => {
  const disabled = await fetch(`${baseUrl}/tiktok-test`);
  assert.equal(disabled.status, 404);

  const missing = await fetch(`${baseUrl}/api/__e3_characterization_missing__`);
  assert.equal(missing.status, 404);
});

