"use strict";
const test=require("node:test");
const assert=require("node:assert/strict");
const fs=require("node:fs");
const os=require("node:os");
const path=require("node:path");
const {execFileSync}=require("node:child_process");
const {parseAuditDatabaseUrl,writeLibpqFiles}=require("../security/libpq-audit-config");

test("connection URL validation rejects unsafe or incomplete values",()=>{for(const value of ["https://host/db","postgresql:///db","postgresql://user@host","postgresql://user@host:70000/db","postgresql://user%0Aname@host/db"])assert.throws(()=>parseAuditDatabaseUrl(value),/AUDIT_CONNECTION_FAILED/);assert.deepEqual(parseAuditDatabaseUrl("postgresql://audit@db.invalid:6543/app"),{host:"db.invalid",port:"6543",database:"app",user:"audit",password:"",sslmode:"require"})});

test("generated pgpass is mode 600 and parsed by real libpq with reserved password characters",()=>{const dir=fs.mkdtempSync(path.join(os.tmpdir(),"libpq-audit-")),password="colon:slash\\at@percent%value",uri=`postgresql://audit:${encodeURIComponent(password)}@127.0.0.1:1/app`;const files=writeLibpqFiles(uri,dir);assert.equal(fs.statSync(files.passfile).mode&0o777,0o600);assert.equal(fs.statSync(files.envfile).mode&0o777,0o600);const source=path.join(dir,"pass.c"),binary=path.join(dir,"pass");fs.writeFileSync(source,"#include <stdio.h>\n#include <libpq-fe.h>\nint main(void){PGconn*c=PQconnectStart(\"host=127.0.0.1 port=1 dbname=app user=audit sslmode=require\");if(!c)return 2;printf(\"%s\",PQpass(c));PQfinish(c);return 0;}\n");execFileSync("cc",[source,"-I",execFileSync("pg_config",["--includedir"],{encoding:"utf8"}).trim(),"-L",execFileSync("pg_config",["--libdir"],{encoding:"utf8"}).trim(),"-lpq","-o",binary]);const parsed=execFileSync(binary,[],{encoding:"utf8",env:{...process.env,PGPASSFILE:files.passfile}});assert.equal(parsed,password)});
