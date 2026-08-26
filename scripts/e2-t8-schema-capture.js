'use strict';
const childProcess = require('node:child_process');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { validateCapturedSchema } = require('../security/e2-t8-captured-schema-validator');

const CONFIRMATION='E2_T8_SCHEMA_ONLY_CAPTURE';
const FIXED_ARGUMENTS=Object.freeze(['--schema-only','--schema=public','--no-owner']);
function parseArguments(argv){
  const allowed=new Set(['--execute','--confirm',CONFIRMATION]);
  if(argv.some(v=>/(?:\/\/|password|uri|table|data|extra|output)/i.test(v))) throw new Error('FORBIDDEN_ARGUMENT');
  for(const value of argv) if(!allowed.has(value)) throw new Error('UNKNOWN_ARGUMENT');
  const execute=argv.includes('--execute'); const i=argv.indexOf('--confirm'); const confirmed=i>=0&&argv[i+1]===CONFIRMATION;
  if(execute&&!confirmed) throw new Error('EXPLICIT_CONFIRMATION_REQUIRED'); return {execute,confirmed};
}
function capturePlan(){return Object.freeze({contract_version:'e2-t8-schema-capture-v2',mode:'schema-only',schema:'public',row_data:false,restore_owner:false,privileges:'captured_then_allowlisted',deterministic:false,determinism_gate:'PIN_TOOL_VERSION_AND_VALIDATE_NORMALIZED_SQL',arguments:FIXED_ARGUMENTS});}
function defaultDependencies(){return {spawnSync:childProcess.spawnSync,resolve:path.resolve,mkdir:(p)=>fs.mkdirSync(p,{recursive:true,mode:0o700}),write:(p,data)=>fs.writeFileSync(p,data,{mode:0o600,flag:'wx'}),repoRoot:path.resolve(__dirname,'..'),now:()=>Date.now(),pid:process.pid};}
function childEnvironment(source,env){const child={PATH:env.PATH||'',PGDATABASE:source}; if(env.HOME)child.HOME=env.HOME; for(const key of ['PGSSLMODE','PGSSLROOTCERT','PGSSLCERT','PGSSLKEY'])if(env[key])child[key]=env[key]; return child;}
function run(argv=[],injected={}){
  const options=parseArguments(argv),plan=capturePlan(); if(!options.execute)return {status:'PLAN_ONLY',plan};
  const env=injected.env||process.env,source=env.E2_T8_SOURCE_DATABASE_URL,quarantine=env.E2_T8_CAPTURE_QUARANTINE_DIR;
  if(!source)return {status:'SOURCE_CREDENTIAL_UNAVAILABLE',plan}; if(!quarantine)return {status:'QUARANTINE_DIRECTORY_REQUIRED',plan};
  const d={...defaultDependencies(),...injected}; const q=d.resolve(quarantine),root=d.resolve(d.repoRoot);
  if(q===root||q.startsWith(root+path.sep))return {status:'REPOSITORY_OUTPUT_FORBIDDEN',plan};
  d.mkdir(q); const tool=d.spawnSync('pg_dump',['--version'],{env:childEnvironment(source,env),encoding:'utf8',stdio:['ignore','pipe','pipe']});
  if(!tool||tool.status!==0)return {status:'CAPTURE_TOOL_UNAVAILABLE',plan};
  const result=d.spawnSync('pg_dump',FIXED_ARGUMENTS,{env:childEnvironment(source,env),encoding:'utf8',stdio:['ignore','pipe','pipe'],maxBuffer:64*1024*1024});
  if(!result||result.status!==0)return {status:'CAPTURE_COMMAND_FAILED',plan};
  const file=path.join(q,`e2-t8-capture-${d.now()}-${d.pid}.sql`); d.write(file,result.stdout||'');
  if(typeof injected.validationInputs!=='object')return {status:'CAPTURE_QUARANTINED_VALIDATION_REQUIRED',plan};
  const validation=validateCapturedSchema({capturedSql:result.stdout||'',...injected.validationInputs});
  return {status:validation.status==='ARTIFACT_CONTRACT_PASS'?'CAPTURE_QUARANTINED_CONTRACT_PASS':'CAPTURE_QUARANTINED_CONTRACT_FAIL',artifact_checksum:validation.checksum,plan};
}
module.exports={CONFIRMATION,FIXED_ARGUMENTS,parseArguments,capturePlan,childEnvironment,defaultDependencies,run};
if(require.main===module){try{const r=run(process.argv.slice(2));process.stdout.write(`${JSON.stringify(r)}\n`);if(/FAILED|FORBIDDEN|UNAVAILABLE/.test(r.status))process.exitCode=1;}catch(e){process.stderr.write(`${e.message}\n`);process.exitCode=1;}}
