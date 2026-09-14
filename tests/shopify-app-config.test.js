"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const {test} = require("node:test");

const config = fs.readFileSync(path.join(__dirname, "..", "shopify.app.toml"), "utf8");

test("Shopify app configuration targets the canonical embedded App Home", () => {
  assert.match(config, /^application_url = "https:\/\/dev\.adstable\.app\/shopify\/app"$/m);
  assert.match(config, /^embedded = true$/m);
  assert.match(config, /^include_config_on_deploy = true$/m);
  assert.doesNotMatch(config, /firats-projects|\.vercel\.app/i);
});

test("Shopify app configuration cannot request unapproved Shopify data scopes", () => {
  assert.match(config, /^\[access_scopes\]\nscopes = ""$/m);
  assert.doesNotMatch(config, /read_(?:products|customers|orders)|write_(?:products|customers)/);
});
