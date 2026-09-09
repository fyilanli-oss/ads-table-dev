#!/usr/bin/env node
"use strict";

const {spawnSync} = require("node:child_process");
const {inspectDevelopmentBootstrap} = require("../src/shopify/development-bootstrap");

function hasShopifyCli() {
  const result = spawnSync("shopify", ["version"], {encoding: "utf8", stdio: "ignore"});
  return !result.error && result.status === 0;
}

function main() {
  const report = inspectDevelopmentBootstrap({env: process.env, cliAvailable: hasShopifyCli()});
  process.stdout.write(`${JSON.stringify(report)}\n`);
  if (report.status !== "READY_FOR_MANUAL_BOOTSTRAP") process.exitCode = 2;
}

if (require.main === module) main();
module.exports = Object.freeze({hasShopifyCli, main});
