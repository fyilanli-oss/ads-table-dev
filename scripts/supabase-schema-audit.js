#!/usr/bin/env node
"use strict";

const fs = require("node:fs");
const path = require("node:path");
const { execFileSync } = require("node:child_process");
const { sanitizeMetadata, validateReport } = require("../security/supabase-audit-contract");

const ROOT = path.resolve(__dirname, "..");
const CLASSES = ["E2_BLOCKER", "SECURITY_CORRECTIVE", "REPOSITORY_BASELINE", "PLAN_DRIFT", "E13_LEGACY", "BACKLOG", "INFORMATIONAL", "REVIEW_REQUIRED"];
const sortBy = key => (a, b) => String(a[key]).localeCompare(String(b[key]));
const DATASET_V2_COLUMNS = {
  id:["uuid",false],user_id:["uuid",false],platform:["text",false],traffic_type:["text",false],source_system:["text",false],channel:["text",true],platform_account_id:["text",false],business_date:["date",false],campaign_type:["text",true],root_entity_type:["text",false],root_entity_id:["text",false],root_entity_name:["text",true],parent_entity_type:["text",true],parent_entity_id:["text",true],parent_entity_name:["text",true],entity_type:["text",false],entity_id:["text",false],entity_name:["text",false],entity_key:["text",false],metric_support:["jsonb",false],impressions:["numeric",true],ad_clicks:["numeric",true],sessions:["numeric",true],spend:["numeric",true],add_to_cart:["numeric",true],add_to_cart_value:["numeric",true],checkout:["numeric",true],checkout_value:["numeric",true],purchase:["numeric",true],purchase_value:["numeric",true],source_currency:["text",false],target_currency:["text",false],fx_rate:["numeric",false],fx_rate_date:["date",false],fx_provider:["text",false],fx_engine_version:["text",false],source_timezone:["text",false],time_engine_version:["text",false],canonical_contract_version:["text",false],adapter_version:["text",false],source_confidence:["text",false],synthetic:["boolean",false],ga4_property_id:["text",true],source_job_id:["uuid",true],raw:["jsonb",false],created_at:["timestamp with time zone",false],updated_at:["timestamp with time zone",false]
};
const DATASET_V2_CONSTRAINTS = ["performance_dataset_rows_v2_pkey","performance_dataset_rows_v2_user_id_fkey","performance_dataset_rows_v2_platform_chk","performance_dataset_rows_v2_traffic_type_chk","performance_dataset_rows_v2_source_system_chk","performance_dataset_rows_v2_channel_chk","performance_dataset_rows_v2_campaign_type_chk","performance_dataset_rows_v2_root_type_chk","performance_dataset_rows_v2_parent_type_chk","performance_dataset_rows_v2_entity_type_chk","performance_dataset_rows_v2_source_confidence_chk","performance_dataset_rows_v2_source_currency_chk","performance_dataset_rows_v2_target_currency_chk","performance_dataset_rows_v2_fx_rate_chk","performance_dataset_rows_v2_metric_support_object_chk","performance_dataset_rows_v2_raw_object_chk","performance_dataset_rows_v2_synthetic_chk","performance_dataset_rows_v2_source_semantics_chk","performance_dataset_rows_v2_hierarchy_chk","performance_dataset_rows_v2_metric_support_keys_chk","performance_dataset_rows_v2_metric_value_support_chk"];
const DATASET_V2_INDEX_CONTRACT = {
  performance_dataset_rows_v2_pkey:{unique:true,primary:true,columns:["id"]},
  performance_dataset_rows_v2_canonical_uidx:{unique:true,primary:false,columns:["user_id","platform","platform_account_id","business_date","traffic_type","entity_key"]},
  performance_dataset_rows_v2_user_date_idx:{unique:false,primary:false,columns:["user_id","business_date"]},
  performance_dataset_rows_v2_account_scope_date_idx:{unique:false,primary:false,columns:["user_id","platform","platform_account_id","traffic_type","business_date"]},
  performance_dataset_rows_v2_entity_history_idx:{unique:false,primary:false,columns:["user_id","platform","platform_account_id","entity_key","business_date"]}
};
const DATASET_V2_INDEXES = Object.keys(DATASET_V2_INDEX_CONTRACT);

function balancedBody(text,start){let depth=0,quoted=false;for(let i=start;i<text.length;i++){const char=text[i];if(char==="'"&&text[i+1]==="'"&&quoted){i++;continue;}if(char==="'")quoted=!quoted;if(quoted)continue;if(char==="(")depth++;if(char===")"&&--depth===0)return text.slice(start+1,i);}return null;}
function repositoryConstraintDefinitions(root=ROOT){const result={};for(const file of ["20260816101220_create_performance_dataset_rows_v2.sql","20260816101540_fix_v2_klaviyo_channel_constraint.sql"]){const sql=fs.readFileSync(path.join(root,"supabase/migrations",file),"utf8");for(const match of sql.matchAll(/constraint\s+(performance_dataset_rows_v2_[a-z0-9_]+)\s+check\s*\(/gi)){const open=match.index+match[0].lastIndexOf("(");const body=balancedBody(sql,open);if(body==null)throw new Error("AUDIT_REPORT_INVALID");result[match[1].toLowerCase()]=`CHECK (${body})`;}}return result;}
const DATASET_V2_CHECK_DEFINITIONS=repositoryConstraintDefinitions();

function stripOuter(text){while(text.startsWith("(")&&text.endsWith(")")&&balancedBody(text,0)===text.slice(1,-1))text=text.slice(1,-1).trim();return text;}
function splitBoolean(text,operator){const parts=[];let start=0,round=0,square=0,quoted=false;for(let i=0;i<text.length;i++){const char=text[i];if(char==="'"&&text[i+1]==="'"&&quoted){i++;continue;}if(char==="'"){quoted=!quoted;continue;}if(quoted)continue;if(char==="(")round++;else if(char===")")round--;else if(char==="[")square++;else if(char==="]")square--;if(round===0&&square===0&&text.slice(i,i+operator.length)===operator){parts.push(text.slice(start,i));start=i+operator.length;i+=operator.length-1;}}if(parts.length)parts.push(text.slice(start));return parts;}
function canonicalBoolean(text){text=stripOuter(text.trim());for(const [word,label] of [[" or ","or"],[" and ","and"]]){const parts=splitBoolean(text,word);if(parts.length)return `${label}(${parts.map(canonicalBoolean).sort().join(",")})`;}return text.replace(/\s+/g,"");}
function canonicalExpression(value){let text=String(value||"").trim().toLowerCase().replace(/::(?:text|numeric|date|boolean|jsonb|uuid)\b/g,"").replace(/\bpublic\./g,"").replace(/"([a-z_][a-z0-9_]*)"/g,"$1").replace(/\s+/g," ");const check=text.match(/^check\s*\(([\s\S]*)\)$/);if(check)text=check[1].trim();text=text.replace(/([a-z_][a-z0-9_]*(?:->>'[^']+')?)\s*=\s*any\s*\(\s*array\[(.*?)\]\s*\)/gi,"$1 in ($2)");return canonicalBoolean(text);}
function indexSemantics(index){const definition=String(index.definition||"");const open=definition.indexOf("("),close=definition.lastIndexOf(")");if(open<0||close<open)return null;const suffix=definition.slice(close+1).trim();return {unique:index.unique,primary:index.primary,columns:definition.slice(open+1,close).split(",").map(x=>x.trim().toLowerCase().replace(/"/g,"").replace(/\s+/g," ")),suffix:suffix.toLowerCase().replace(/\s+/g," ")};}

function walk(dir, accept) {
  if (!fs.existsSync(dir)) return [];
  return fs.readdirSync(dir, { withFileTypes: true }).flatMap(entry => entry.isDirectory() ? (["node_modules", ".git", ".temp"].includes(entry.name) ? [] : walk(path.join(dir, entry.name), accept)) : accept(path.join(dir, entry.name)) ? [path.join(dir, entry.name)] : []);
}

function parseSql(text) {
  const targets = [];
  const recognized = /^\s*(?:--[^\n]*\n\s*)*(create|alter|drop|grant|revoke)\b/i;
  const statements = text.split(/;\s*(?:\n|$)/).map(x => x.trim()).filter(Boolean);
  const unclassified = [];
  for (const statement of statements) {
    const match = statement.match(/\b(create|alter|drop)\s+(?:table|index|policy|function)\s+(?:if\s+(?:not\s+)?exists\s+)?(?:public\.)?"?([a-z_][\w$]*)/i) || statement.match(/\b(grant|revoke)\b[\s\S]*?\bon\s+(?:table\s+)?(?:public\.)?"?([a-z_][\w$]*)/i);
    if (match) targets.push({ operation: match[1].toLowerCase(), target: match[2].toLowerCase() });
    else if (recognized.test(statement) || !/^(begin|commit|set|select|with|comment)\b/i.test(statement)) unclassified.push(statement.slice(0, 80));
  }
  return { targets, unclassified };
}

function repositoryInventory(root = ROOT) {
  const migrationDir = path.join(root, "supabase/migrations");
  const migrations = walk(migrationDir, file => file.endsWith(".sql")).map(file => {
    const filename = path.basename(file); const parsed = parseSql(fs.readFileSync(file, "utf8"));
    return { version: filename.match(/^(\d+)/)?.[1] ?? "", filename, targets: parsed.targets, unclassified: parsed.unclassified };
  }).sort(sortBy("version"));
  const operationalSql = walk(path.join(root, "docs/security/sql"), f => f.endsWith(".sql")).map(f => path.relative(root, f)).sort();
  const sources = walk(root, f => /\.(?:js|html|md)$/.test(f) && !f.includes(`${path.sep}node_modules${path.sep}`) && !f.includes(`${path.sep}.git${path.sep}`));
  const publicTables = new Set();
  for (const file of sources) for (const match of fs.readFileSync(file, "utf8").matchAll(/(?:from\(|public\.)["'`]?([a-z][a-z0-9_]*)/gi)) publicTables.add(match[1]);
  return { migrations, operationalSql, runtimePublicTables: [...publicTables].sort(), unclassifiedRepositorySql: migrations.flatMap(m => m.unclassified.map(statement => ({ file: m.filename, statement }))) };
}

function drift(id, classification, severity, evidence, impact, blocker = false) {
  return { id, severity, classification, liveEvidence: evidence.live || "not observed", repositoryEvidence: evidence.repository || "not observed", planEvidence: evidence.plan || "not assessed", impact, recommendedOwnerEpic: classification === "E2_BLOCKER" ? "E2" : classification, blocker, destructiveOperationRequired: false, missingMetadata: evidence.missing || null };
}

function datasetV2Drifts(relation, policies) {
  const findings=[];
  if (!relation) return [drift("dataset-v2-relation-missing","E2_BLOCKER","critical",{live:"public.performance_dataset_rows_v2 absent",repository:"canonical migration present"},"Dataset V2 schema is unavailable",true)];
  const add=(suffix,live,repo)=>findings.push(drift(`dataset-v2-${suffix}`,"E2_BLOCKER","high",{live,repository:repo},"Dataset V2 schema contract does not match the repository migration",true));
  const review=(suffix,missing)=>findings.push(drift(`dataset-v2-${suffix}`,"REVIEW_REQUIRED","unknown",{live:"metadata unavailable or redacted",repository:"canonical migration present",missing},"Dataset V2 schema readiness cannot be decided"));
  if (!Array.isArray(relation.columns) || relation.columnCount == null) return [drift("dataset-v2-metadata-incomplete","REVIEW_REQUIRED","unknown",{live:"column metadata incomplete",repository:"canonical migration present",missing:"Dataset V2 columns"},"Schema readiness cannot be decided")];
  const liveColumns=new Map(relation.columns.map(c=>[c.name,[String(c.type).toLowerCase(),c.nullable]]));
  for(const [name,contract] of Object.entries(DATASET_V2_COLUMNS)) if(JSON.stringify(liveColumns.get(name))!==JSON.stringify(contract)) add(`column-${name}`,JSON.stringify(liveColumns.get(name)??null),JSON.stringify(contract));
  for(const name of liveColumns.keys()) if(!(name in DATASET_V2_COLUMNS)) add(`unexpected-column-${name}`,name,"column absent");
  const constraintNames=new Set((relation.constraints||[]).map(x=>x.name));
  for(const name of DATASET_V2_CONSTRAINTS) if(!constraintNames.has(name)) add(`constraint-${name}`,"missing",name);
  for(const name of constraintNames) if(!DATASET_V2_CONSTRAINTS.includes(name)) add(`unexpected-constraint-${name}`,name,"constraint absent");
  if(relation.pkCount!==1||relation.fkCount!==1||relation.checkCount!==19||relation.uniqueCount!==0) add("constraint-counts",JSON.stringify({pk:relation.pkCount,fk:relation.fkCount,check:relation.checkCount,unique:relation.uniqueCount}),"pk 1; fk 1; check 19; unique constraints 0");
  for(const constraint of relation.constraints||[]){if(!constraint.name.endsWith("_chk"))continue;const definition=String(constraint.definition||"");if(!definition||definition==="[redacted-expression]"){review(`constraint-metadata-${constraint.name}`,`comparable definition for ${constraint.name}`);continue;}const expected=DATASET_V2_CHECK_DEFINITIONS[constraint.name];if(!expected){add(`constraint-definition-${constraint.name}`,definition,"no repository CHECK contract");continue;}if(canonicalExpression(definition)!==canonicalExpression(expected))add(`constraint-definition-${constraint.name}`,canonicalExpression(definition),canonicalExpression(expected));}
  const fk=(relation.constraints||[]).find(x=>x.name==="performance_dataset_rows_v2_user_id_fkey");
  if(fk&&fk.definition!=="[redacted-expression]") { const actual={type:fk.type,localColumns:fk.localColumns,referencedSchema:fk.referencedSchema,referencedTable:fk.referencedTable,referencedColumns:fk.referencedColumns,validated:fk.validated,updateAction:fk.updateAction,deleteAction:fk.deleteAction}; const expected={type:"f",localColumns:["user_id"],referencedSchema:"public",referencedTable:"users",referencedColumns:["id"],validated:true,updateAction:"NO ACTION",deleteAction:"NO ACTION"}; if(JSON.stringify(actual)!==JSON.stringify(expected)) add("foreign-key-contract",JSON.stringify(actual),JSON.stringify(expected)); }
  const indexes=new Map((relation.indexes||[]).map(x=>[x.name,x]));
  for(const name of DATASET_V2_INDEXES) if(!indexes.has(name)) add(`index-${name}`,"missing",name);
  for(const name of indexes.keys()) if(!DATASET_V2_INDEXES.includes(name)) add(`unexpected-index-${name}`,name,"index absent");
  for(const [name,expected] of Object.entries(DATASET_V2_INDEX_CONTRACT)){const row=indexes.get(name);if(!row)continue;if(row.definition==="[redacted-expression]"||!row.definition){review(`index-metadata-${name}`,`comparable definition for ${name}`);continue;}const actual=indexSemantics(row),contract={...expected,suffix:""};if(!actual){review(`index-metadata-${name}`,`parseable definition for ${name}`);continue;}if(JSON.stringify(actual)!==JSON.stringify(contract))add(`index-definition-${name}`,JSON.stringify(actual),JSON.stringify(contract));}
  if(!relation.rlsEnabled) add("rls","disabled","enabled");
  const policy=policies.find(p=>p.table==="performance_dataset_rows_v2"&&p.name==="performance_dataset_rows_v2_select_own");
  if(!policy||String(policy.command).toUpperCase()!=="SELECT"||JSON.stringify(policy.roles)!==JSON.stringify(["authenticated"])||String(policy.permissive).toUpperCase()!=="PERMISSIVE"||!String(policy.using||"").includes("user_id")||!String(policy.using||"").includes("auth.uid")||policy.withCheck!=null) add("policy",policy?"mismatch":"missing","permissive authenticated SELECT using auth.uid() = user_id; no WITH CHECK");
  const serviceExpected=["DELETE","INSERT","REFERENCES","SELECT","TRIGGER","TRUNCATE","UPDATE"];
  if((relation.anonPrivileges||[]).length||JSON.stringify(relation.authenticatedPrivileges||[])!==JSON.stringify(["SELECT"])||JSON.stringify(relation.serviceRolePrivileges||[])!==JSON.stringify(serviceExpected)) add("privileges",JSON.stringify({anon:relation.anonPrivileges,authenticated:relation.authenticatedPrivileges,serviceRole:relation.serviceRolePrivileges}),"anon none; authenticated SELECT; service_role ALL table privileges");
  return findings;
}

function privilegeDrifts(relation) {
  const anon=relation.anonPrivileges||[], auth=relation.authenticatedPrivileges||[], client=[...new Set([...anon,...auth])];
  if(!client.length)return [];
  const serverOnly=["platform_connection_tokens","oauth_transactions"].includes(relation.name);
  if(serverOnly)return [drift(`client-grant-${relation.name}`,"SECURITY_CORRECTIVE","high",{live:`client privileges: ${client.join(", ")}`,repository:"server-only relation contract"},"Client privilege violates the server-only boundary")];
  if(relation.name==="performance_dataset_rows_v2") return client.some(p=>p!=="SELECT")||anon.length?[drift("client-grant-performance_dataset_rows_v2","SECURITY_CORRECTIVE","high",{live:`anon: ${anon.join(",")||"none"}; authenticated: ${auth.join(",")||"none"}`,repository:"anon none; authenticated SELECT only"},"Dataset V2 client grant exceeds its contract")]:[];
  if(client.some(p=>["TRUNCATE","TRIGGER","REFERENCES"].includes(p)))return [drift(`dangerous-client-grant-${relation.name}`,"SECURITY_CORRECTIVE","high",{live:`client privileges: ${client.join(", ")}`,repository:"dangerous client privileges prohibited"},"Client structural privilege exceeds the safe boundary")];
  return [drift(`client-grant-review-${relation.name}`,"REVIEW_REQUIRED","unknown",{live:`client privileges: ${client.join(", ")}`,repository:"no explicit relation contract",missing:"RLS policy and application access intent"},"Legacy client access requires policy-aware review")];
}

function classify(meta, repo) {
  const result = []; const liveVersions = new Set(meta.migrations.map(x => x.version)); const repoVersions = new Set(repo.migrations.map(x => x.version));
  for (const m of meta.migrations) if (!repoVersions.has(m.version)) result.push(drift(`ledger-only-${m.version}`, "REPOSITORY_BASELINE", "high", { live: `ledger version ${m.version}`, repository: "migration file absent" }, "Production cannot be reproduced from repository evidence"));
  for (const m of repo.migrations) if (!liveVersions.has(m.version)) result.push(drift(`repository-only-${m.version}`, "E2_BLOCKER", "high", { live: "ledger version absent", repository: m.filename }, "Ledger state is ambiguous before Dataset V2 acceptance", true));
  const created = new Set(repo.migrations.flatMap(m => m.targets.filter(t => t.operation === "create").map(t => t.target)));
  for (const relation of meta.relations) {
    if (!created.has(relation.name)) result.push(drift(`live-only-relation-${relation.name}`, "REPOSITORY_BASELINE", "high", { live: `public.${relation.name} exists`, repository: "create target absent" }, "Schema baseline is incomplete"));
    result.push(...privilegeDrifts(relation));
    if (/snapshot|_v1$|legacy/.test(relation.name)) result.push(drift(`legacy-${relation.name}`, "E13_LEGACY", "medium", { live: relation.name, plan: "legacy retirement deferred" }, "Candidate for measured E13 retirement"));
    if (/^(users|subscriptions|user_settings)$/.test(relation.name)) result.push(drift(`plan-${relation.name}`, "PLAN_DRIFT", "medium", { live: relation.name, plan: "canonical/legacy status unresolved" }, "Execution Plan needs evidence-based reconciliation"));
  }
  for (const fn of meta.functions) if (fn.securityDefiner && (!fn.searchPathConfigured || fn.publicExecute || fn.anonExecute || fn.authenticatedExecute)) result.push(drift(`unsafe-function-${fn.identity}`, "SECURITY_CORRECTIVE", "critical", { live: "security-definer execution/search_path contract unsafe", repository: "requires corrective package" }, "Privilege escalation surface"));
  for (const item of repo.unclassifiedRepositorySql) result.push(drift(`unclassified-${item.file}-${result.length + 1}`, "REVIEW_REQUIRED", "unknown", { repository: `${item.file}: ${item.statement}`, missing: "statement classification" }, "Manual SQL classification is required"));
  result.push(...datasetV2Drifts(meta.relations.find(x=>x.name==="performance_dataset_rows_v2"),meta.policies));
  return result.sort(sortBy("id"));
}

function buildReport(raw, repo, options = {}) {
  const meta = sanitizeMetadata(raw); const drifts = classify(meta, repo); const counts = Object.fromEntries(CLASSES.map(c => [c, drifts.filter(d => d.classification === c).length]));
  const versions = meta.migrations.map(x => x.version); const duplicates = [...new Set(versions.filter((x, i) => versions.indexOf(x) !== i))].sort();
  const report = { reportVersion: 1, generatedAt: options.generatedAt || new Date().toISOString(), repositoryCommit: options.repositoryCommit || "unknown", databaseRole: "codex_auditor", readOnlyVerified: true, privilegeContractVerified: true, summary: { status: "PASS", relationCount: meta.relations.length, ledgerCount: meta.migrations.length, datasetV2SchemaReady: !drifts.some(d=>d.id.startsWith("dataset-v2-")&&["E2_BLOCKER","REVIEW_REQUIRED"].includes(d.classification)), driftCounts: counts }, migrationLedger: { count: meta.migrations.length, latestVersion: versions.at(-1) || null, duplicateVersions: duplicates, entries: meta.migrations }, repositoryMigrations: repo.migrations.map(m => ({ version: m.version, filename: m.filename, targets: m.targets })), relations: meta.relations, policies: meta.policies, functions: meta.functions, defaultPrivileges: meta.defaultPrivileges, drifts, limitations: ["Metadata-only audit; no application table rows or function bodies were read.", "Dataset V2 write/read/RLS user-matrix acceptance is outside this audit.", "Classification does not apply corrective changes."] };
  validateReport(report, options.knownValues || []); return report;
}

function markdown(report) {
  const sections = ["Audit Status", "Read-Only and Privilege Acceptance", "Executive Summary", "Migration Ledger Drift", "Repository Baseline Gaps", "Dataset V2 Readiness", "Security Correctives", "Users/Subscriptions/Identity Findings", "Execution Plan Drift", "E13 Legacy Candidates", "Review Required", "Recommended Package Order", "Limitations"];
  const lines = ["# DB–Execution Plan Drift Report", "", `Generated: ${report.generatedAt}`, `Commit: \`${report.repositoryCommit}\``, ""];
  for (const section of sections) {
    lines.push(`## ${section}`, "");
    if (section === "Audit Status") lines.push(`**${report.summary.status}** — role \`${report.databaseRole}\`; read-only and privilege acceptance verified.`, "");
    else if (section === "Executive Summary") lines.push(`Relations: ${report.summary.relationCount}; ledger entries: ${report.summary.ledgerCount}; E2 blockers: ${report.summary.driftCounts.E2_BLOCKER}.`, "");
    else if (section === "Read-Only and Privilege Acceptance") lines.push(`Read-only: ${report.readOnlyVerified}; privilege contract: ${report.privilegeContractVerified}.`, "");
    else if (section === "Dataset V2 Readiness" && report.summary.datasetV2SchemaReady) lines.push("Schema-level PASS. Runtime write/read/RLS user-matrix acceptance was not performed.", "");
    else if (section === "Recommended Package Order") { const order=[["E2_BLOCKER"],["SECURITY_CORRECTIVE"],["REPOSITORY_BASELINE"],["PLAN_DRIFT"],["E13_LEGACY"],["BACKLOG","REVIEW_REQUIRED"]]; let n=1; for(const group of order){const ids=report.drifts.filter(d=>group.includes(d.classification)).map(d=>d.id);if(ids.length)lines.push(`${n++}. ${group.join(" / ")}: ${ids.map(id=>`\`${id}\``).join(", ")}`);} if(n===1)lines.push("No corrective package is recommended.");lines.push(""); }
    else if (section === "Limitations") lines.push(...report.limitations.map(x => `- ${x}`), "");
    else {
      const map = { "Migration Ledger Drift": ["E2_BLOCKER"], "Repository Baseline Gaps": ["REPOSITORY_BASELINE"], "Dataset V2 Readiness": ["E2_BLOCKER"], "Security Correctives": ["SECURITY_CORRECTIVE"], "Users/Subscriptions/Identity Findings": ["PLAN_DRIFT"], "Execution Plan Drift": ["PLAN_DRIFT"], "E13 Legacy Candidates": ["E13_LEGACY"], "Review Required": ["REVIEW_REQUIRED"] };
      const items = report.drifts.filter(d => (map[section] || []).includes(d.classification));
      if (!items.length) lines.push("No finding in this category.", "");
      else for (const d of items) lines.push(`### ${d.id}`, `- Severity: ${d.severity}`, `- Classification: ${d.classification}`, `- Live evidence: ${d.liveEvidence}`, `- Repository evidence: ${d.repositoryEvidence}`, `- Plan evidence: ${d.planEvidence}`, `- Impact: ${d.impact}`, `- Owner/epic: ${d.recommendedOwnerEpic}`, `- Blocker: ${d.blocker}`, `- Destructive operation required: ${d.destructiveOperationRequired}`, "");
    }
  }
  return `${lines.join("\n")}\n`;
}

function runCli() {
  const [command, input, output = "audit-output"] = process.argv.slice(2);
  if (command === "report") {
    const raw = JSON.parse(fs.readFileSync(input, "utf8")); const repo = repositoryInventory(); const report = buildReport(raw, repo, { repositoryCommit: process.env.GITHUB_SHA || execFileSync("git", ["rev-parse", "HEAD"], { cwd: ROOT, encoding: "utf8" }).trim() });
    fs.mkdirSync(output, { recursive: true }); fs.writeFileSync(path.join(output, "db-execution-plan-drift-report.json"), `${JSON.stringify(report, null, 2)}\n`); fs.writeFileSync(path.join(output, "db-execution-plan-drift-report.md"), markdown(report));
  } else if (command === "validate") {
    for (const file of process.argv.slice(3)) validateReport(JSON.parse(fs.readFileSync(file, "utf8")), [process.env.SUPABASE_PROJECT_REF]);
  } else throw new Error("AUDIT_CONFIG_MISSING");
}

if (require.main === module) { try { runCli(); } catch (error) { console.error(/^AUDIT_/.test(error.message) ? error.message : "AUDIT_REPORT_INVALID"); process.exitCode = 1; } }
module.exports = { DATASET_V2_CHECK_DEFINITIONS, DATASET_V2_COLUMNS, DATASET_V2_CONSTRAINTS, DATASET_V2_INDEX_CONTRACT, DATASET_V2_INDEXES, buildReport, canonicalExpression, classify, datasetV2Drifts, markdown, parseSql, privilegeDrifts, repositoryInventory };
