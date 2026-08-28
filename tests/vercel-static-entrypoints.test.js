"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const { test } = require("node:test");

const root = path.resolve(__dirname, "..");
const config = JSON.parse(fs.readFileSync(path.join(root, "vercel.json"), "utf8"));
const routeFor = (source) => config.routes.find((route) => route.src === source);

test("builds public files as static artifacts independently from Express", () => {
  assert.deepEqual(config.builds, [
    { src: "public/**", use: "@vercel/static" },
    { src: "server.js", use: "@vercel/node" },
  ]);
});

test("serves public entrypoints from static build outputs without booting Express", () => {
  const expected = new Map([
    ["/", "/public/landing.html"],
    ["/login", "/public/login.html"],
    ["/signup", "/public/signup.html"],
    ["/dashboard", "/public/dashboard.html"],
    ["/dashboard-demo", "/public/dashboard-demo.html"],
    ["/demo", "/public/dashboard-demo.html"],
    ["/privacy", "/public/privacy.html"],
    ["/terms", "/public/terms.html"],
    ["/data-deletion", "/public/data-deletion.html"],
  ]);

  for (const [source, destination] of expected) {
    assert.equal(routeFor(source)?.dest, destination);
    assert.equal(
      fs.existsSync(path.join(root, destination.slice(1))),
      true,
      `${destination} must exist in the static build input`,
    );
  }

  const serverFallbackIndex = config.routes.findIndex((route) => route.dest === "/server.js");
  for (const source of expected.keys()) {
    assert.ok(config.routes.indexOf(routeFor(source)) < serverFallbackIndex, `${source} must precede Express`);
  }
});

test("keeps API and OAuth application routes on the serverless function", () => {
  assert.equal(routeFor("/api/(.*)")?.dest, "/server.js");
  assert.equal(routeFor("/auth/(.*)")?.dest, "/server.js");
  assert.equal(config.routes.at(-1).dest, "/server.js");
});

test("serves committed verification and direct HTML files statically", () => {
  assert.equal(routeFor("/auth/tiktok/(.*\\.txt)")?.dest, "/public/auth/tiktok/$1");
  assert.equal(routeFor("/(.*\\.txt)")?.dest, "/public/$1");
  assert.equal(routeFor("/(.*\\.html)")?.dest, "/public/$1");
});
