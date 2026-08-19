#!/usr/bin/env node
"use strict";
const {runWriteOperator}=require("../security/provider-token-backfill-write-operator");
runWriteOperator({argv:process.argv.slice(2)}).then(
  result=>process.stdout.write(`${JSON.stringify(result)}\n`),
  error=>{const output={ok:false,code:/^BACKFILL_[A-Z_]+$/.test(error?.code||"")?error.code:"BACKFILL_WRITE_FAILED",stage:["config","runtime","preflight","write","verification"].includes(error?.stage)?error.stage:"write"};if(error?.evidence)output.evidence=error.evidence;process.stderr.write(`${JSON.stringify(output)}\n`);process.exitCode=1;}
);
