#!/usr/bin/env node
"use strict";

const {runDryRunOperator}=require("../security/provider-token-backfill-operator");

runDryRunOperator({argv:process.argv.slice(2)}).then(
  result=>process.stdout.write(`${JSON.stringify(result)}\n`),
  error=>{
    const code=typeof error?.code==="string"&&/^BACKFILL_[A-Z_]+$/.test(error.code)?error.code:"BACKFILL_DRY_RUN_FAILED";
    process.stderr.write(`${JSON.stringify({ok:false,code})}\n`);
    process.exitCode=1;
  }
);
