"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const { test } = require("node:test");

const root = path.resolve(__dirname, "..");
const config = JSON.parse(fs.readFileSync(path.join(root, "vercel.json"), "utf8"));

test("serves public entrypoints without invoking the serverless function", () => {
  const expected = new Map([
    ["/", "/landing.html"],
    ["/login", "/login.html"],
    ["/signup", "/signup.html"],
    ["/dashboard", "/dashboard.html"],
    ["/dashboard-demo", "/dashboard-demo.html"],
    ["/demo", "/dashboard-demo.html"],
    ["/privacy", "/privacy.html"],
    ["/terms", "/terms.html"],
    ["/data-deletion", "/data-deletion.html"],
  ]);

  assert.equal(config.rewrites.length, expected.size);
  for (const rewrite of config.rewrites) {
    assert.equal(expected.get(rewrite.source), rewrite.destination);
    assert.equal(
      fs.existsSync(path.join(root, "public", rewrite.destination.slice(1))),
      true,
      `${rewrite.destination} must exist in public`,
    );
  }
});

test("does not rewrite API routes away from the serverless function", () => {
  assert.equal(config.rewrites.some(({ source }) => source.startsWith("/api")), false);
});
