"use strict";
const fs = require("node:fs");
function rejectControls(value) { if (/[\u0000-\u001f\u007f]/.test(value)) throw new Error("AUDIT_CONNECTION_FAILED"); return value; }
function parseAuditDatabaseUrl(value) {
  let url; try { url = new URL(value); } catch { throw new Error("AUDIT_CONNECTION_FAILED"); }
  if (!/^postgres(?:ql)?:$/.test(url.protocol) || !url.hostname || !url.username || url.pathname.length < 2) throw new Error("AUDIT_CONNECTION_FAILED");
  const port = url.port || "5432"; if (!/^\d+$/.test(port) || Number(port) < 1 || Number(port) > 65535) throw new Error("AUDIT_CONNECTION_FAILED");
  const decode = input => { try { return rejectControls(decodeURIComponent(input)); } catch { throw new Error("AUDIT_CONNECTION_FAILED"); } };
  const config = { host: rejectControls(url.hostname), port, database: decode(url.pathname.slice(1)), user: decode(url.username), password: decode(url.password), sslmode: "require" };
  if (!config.database || !config.user) throw new Error("AUDIT_CONNECTION_FAILED"); return config;
}
function pgpassEscape(value) { return String(value).replaceAll("\\", "\\\\").replaceAll(":", "\\:"); }
function writeLibpqFiles(uri, directory) {
  const config = parseAuditDatabaseUrl(uri), passfile = `${directory}/audit.pgpass`, envfile = `${directory}/audit.env0`;
  fs.writeFileSync(passfile, `${[config.host,config.port,config.database,config.user,config.password].map(pgpassEscape).join(":")}\n`, { mode: 0o600 });
  fs.writeFileSync(envfile, Buffer.from([config.host,config.port,config.database,config.user,config.sslmode].join("\0")+"\0"), { mode: 0o600 });
  fs.chmodSync(passfile,0o600); fs.chmodSync(envfile,0o600); return { passfile, envfile };
}
if(require.main===module){try{writeLibpqFiles(process.env.SUPABASE_AUDIT_DATABASE_URL,process.argv[2]);}catch{console.error("AUDIT_CONNECTION_FAILED");process.exitCode=1;}}
module.exports={parseAuditDatabaseUrl,pgpassEscape,writeLibpqFiles};
