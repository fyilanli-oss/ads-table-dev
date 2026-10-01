"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const vm = require("node:vm");
const {initializeKlaviyoAccounts} = require("../src/shopify/klaviyo-account-ui");

const response = (status, body) => ({
  status,
  ok: status >= 200 && status < 300,
  json: async () => body,
});

test("Update spend posts the new amount and confirms derived saved-period ranges before closing", async () => {
  const elements = new Map();
  const element = () => ({
    hidden: false,
    display: "auto",
    value: "",
    textContent: "",
    events: {},
    children: [],
    setAttribute(key, value) { this[key] = value; },
    addEventListener(key, value) { this.events[key] = value; },
    replaceChildren() { this.children = []; },
    append(value) { this.children.push(value); },
    hideOverlay() { this.hidden = true; },
  });
  for (const id of [
    "klaviyo-accounts", "klaviyo-message", "klaviyo-choice", "klaviyo-choice-step",
    "klaviyo-choose", "klaviyo-cost-step", "klaviyo-cost", "klaviyo-save",
    "klaviyo-retry", "klaviyo-retry-step", "klaviyo-connect", "klaviyo-resume",
    "klaviyo-connected", "klaviyo-reset-step", "klaviyo-reset-confirm",
    "klaviyo-spend-controls", "klaviyo-spend-update-modal", "klaviyo-spend-update-value",
    "klaviyo-spend-update-save", "klaviyo-spend-update-message", "klaviyo-spend-correct-modal",
    "klaviyo-spend-correct-open", "klaviyo-spend-history-choice", "klaviyo-spend-correct-value",
    "klaviyo-spend-correct-save", "klaviyo-spend-correct-message",
  ]) elements.set(id, element());

  const requests = [];
  let updated = false;
  const context = {
    URLSearchParams,
    location: {search: ""},
    document: {getElementById: id => elements.get(id), createElement: element},
    window: {shopify: {idToken: async () => "session"}},
    fetch: async (url, options) => {
      requests.push({url, options});
      if (url.endsWith("/accounts/status")) {
        return response(200, {status: "connected", estimated_30_day_email_spend: updated ? "35.00" : "32.00", currency: "USD"});
      }
      if (url.endsWith("/spend-history/update")) {
        updated = true;
        return response(200, {status: "connected", estimated_30_day_email_spend: "35.00", currency: "USD", effective_from: "2026-09-30"});
      }
      if (url.endsWith("/spend-history")) {
        return response(200, {entries: updated ? [
          {effective_from: "2026-09-30", estimated_30_day_email_spend: "35.00", currency: "USD"},
          {effective_from: "2026-09-28", estimated_30_day_email_spend: "32.00", currency: "USD"},
        ] : [{effective_from: "2026-09-28", estimated_30_day_email_spend: "32.00", currency: "USD"}]});
      }
      return response(503, {code: "KLAVIYO_UNAVAILABLE"});
    },
  };

  vm.runInNewContext(`(${initializeKlaviyoAccounts.toString()})()`, context);
  for (let attempt = 0; attempt < 10 && requests.length < 2; attempt += 1) {
    await new Promise(resolve => setImmediate(resolve));
  }
  elements.get("klaviyo-spend-update-value").value = "35";
  await elements.get("klaviyo-spend-update-save").events.click();

  const update = requests.find(item => item.url.endsWith("/spend-history/update"));
  assert.equal(update.options.method, "POST");
  assert.deepEqual(JSON.parse(update.options.body), {estimated_30_day_email_spend: "35"});
  assert.equal(elements.get("klaviyo-spend-update-modal").hidden, true);
  assert.equal(elements.get("klaviyo-spend-history-choice").children[0].textContent, "2026-09-30 / Present · 35.00 USD");
  assert.equal(elements.get("klaviyo-spend-history-choice").children[1].textContent, "2026-09-28 / 2026-09-29 · 32.00 USD");
});
