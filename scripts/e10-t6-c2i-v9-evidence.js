#!/usr/bin/env node
"use strict";

const fs = require("node:fs");
const path = require("node:path");
const {validateV9Evidence} = require("../security/e10-v9-real-device-evidence");

function main(argv = process.argv.slice(2)) {
  if (argv.length !== 1) throw new Error("Usage: node scripts/e10-t6-c2i-v9-evidence.js <evidence.json>");
  const root = path.join(__dirname, "..");
  const evidencePath = path.resolve(root, argv[0]);
  if (!evidencePath.startsWith(`${root}${path.sep}`)) throw new Error("Evidence path must be inside the repository");
  const evidence = JSON.parse(fs.readFileSync(evidencePath, "utf8"));
  process.stdout.write(`${JSON.stringify(validateV9Evidence(evidence, {root}), null, 2)}\n`);
}

if (require.main === module) {
  try {
    main();
  } catch (error) {
    process.stderr.write(`${error.message}\n`);
    process.exitCode = 1;
  }
}

module.exports = Object.freeze({main});
