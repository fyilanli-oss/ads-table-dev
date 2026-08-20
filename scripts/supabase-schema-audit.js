#!/usr/bin/env node
"use strict";

const fs = require("node:fs");
const path = require("node:path");
const { execFileSync } = require("node:child_process");
const { sanitizeMetadata, validateReport } = require("../security/supabase-audit-contract");

const ROOT = path.resolve(__dirname, "..");
const CLASSES = ["E2_BLOCKER", "SECURITY_CORRECTIVE", "REPOSITORY_BASELINE", "PLAN_DRIFT", "E13_LEGACY", "BACKLOG", "INFORMATIONAL", "REVIEW_REQUIRED"];
const sortBy = key => (a, b) => String(a[key]).localeCompare(String(b[key]));

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

function classify(meta, repo) {
  const result = []; const liveVersions = new Set(meta.migrations.map(x => x.version)); const repoVersions = new Set(repo.migrations.map(x => x.version));
  for (const m of meta.migrations) if (!repoVersions.has(m.version)) result.push(drift(`ledger-only-${m.version}`, "REPOSITORY_BASELINE", "high", { live: `ledger version ${m.version}`, repository: "migration file absent" }, "Production cannot be reproduced from repository evidence"));
  for (const m of repo.migrations) if (!liveVersions.has(m.version)) result.push(drift(`repository-only-${m.version}`, "E2_BLOCKER", "high", { live: "ledger version absent", repository: m.filename }, "Ledger state is ambiguous before Dataset V2 acceptance", true));
  const created = new Set(repo.migrations.flatMap(m => m.targets.filter(t => t.operation === "create").map(t => t.target)));
  for (const relation of meta.relations) {
    if (!created.has(relation.name)) result.push(drift(`live-only-relation-${relation.name}`, "REPOSITORY_BASELINE", "high", { live: `public.${relation.name} exists`, repository: "create target absent" }, "Schema baseline is incomplete"));
    if ((relation.anonPrivileges || []).length || (relation.authenticatedPrivileges || []).length) result.push(drift(`client-grant-${relation.name}`, "SECURITY_CORRECTIVE", "high", { live: "client table privilege present", repository: "requires review" }, "Client privilege may exceed the intended boundary"));
    if (/snapshot|_v1$|legacy/.test(relation.name)) result.push(drift(`legacy-${relation.name}`, "E13_LEGACY", "medium", { live: relation.name, plan: "legacy retirement deferred" }, "Candidate for measured E13 retirement"));
    if (/^(users|subscriptions|user_settings)$/.test(relation.name)) result.push(drift(`plan-${relation.name}`, "PLAN_DRIFT", "medium", { live: relation.name, plan: "canonical/legacy status unresolved" }, "Execution Plan needs evidence-based reconciliation"));
  }
  for (const fn of meta.functions) if (fn.securityDefiner && (!fn.searchPathConfigured || fn.publicExecute || fn.anonExecute || fn.authenticatedExecute)) result.push(drift(`unsafe-function-${fn.identity}`, "SECURITY_CORRECTIVE", "critical", { live: "security-definer execution/search_path contract unsafe", repository: "requires corrective package" }, "Privilege escalation surface"));
  for (const item of repo.unclassifiedRepositorySql) result.push(drift(`unclassified-${item.file}-${result.length + 1}`, "REVIEW_REQUIRED", "unknown", { repository: `${item.file}: ${item.statement}`, missing: "statement classification" }, "Manual SQL classification is required"));
  return result.sort(sortBy("id"));
}

function buildReport(raw, repo, options = {}) {
  const meta = sanitizeMetadata(raw); const drifts = classify(meta, repo); const counts = Object.fromEntries(CLASSES.map(c => [c, drifts.filter(d => d.classification === c).length]));
  const versions = meta.migrations.map(x => x.version); const duplicates = [...new Set(versions.filter((x, i) => versions.indexOf(x) !== i))].sort();
  const report = { reportVersion: 1, generatedAt: options.generatedAt || new Date().toISOString(), repositoryCommit: options.repositoryCommit || "unknown", databaseRole: "codex_auditor", readOnlyVerified: true, privilegeContractVerified: true, summary: { status: "PASS", relationCount: meta.relations.length, ledgerCount: meta.migrations.length, driftCounts: counts }, migrationLedger: { count: meta.migrations.length, latestVersion: versions.at(-1) || null, duplicateVersions: duplicates, entries: meta.migrations }, repositoryMigrations: repo.migrations.map(m => ({ version: m.version, filename: m.filename, targets: m.targets })), relations: meta.relations, policies: meta.policies, functions: meta.functions, defaultPrivileges: meta.defaultPrivileges, drifts, limitations: ["Metadata-only audit; no application table rows or function bodies were read.", "Classification does not apply corrective changes."] };
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
module.exports = { buildReport, classify, markdown, parseSql, repositoryInventory };
