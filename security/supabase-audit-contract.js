"use strict";

const REPORT_KEYS = ["reportVersion", "generatedAt", "repositoryCommit", "databaseRole", "readOnlyVerified", "privilegeContractVerified", "summary", "migrationLedger", "repositoryMigrations", "relations", "policies", "functions", "defaultPrivileges", "drifts", "limitations"];
const CLASSIFICATIONS = ["E2_BLOCKER", "SECURITY_CORRECTIVE", "REPOSITORY_BASELINE", "PLAN_DRIFT", "E13_LEGACY", "BACKLOG", "INFORMATIONAL", "REVIEW_REQUIRED"];

function assertAcceptance(value) {
  const expected = {
    currentUser: "codex_auditor", readOnly: true, databaseConnect: true,
    datasetSelect: true, datasetInsert: false, datasetUpdate: false,
    datasetDelete: false, datasetTruncate: false, tokenTableSelect: false,
    accessEnvelopeSelect: false, refreshEnvelopeSelect: false,
    legacyAccessSelect: false, legacyRefreshSelect: false, authUsersSelect: false,
    oauthTransactionsSelect: false, migrationLedgerSelect: true,
  };
  for (const [key, wanted] of Object.entries(expected)) {
    if (value?.[key] !== wanted) {
      if (key === "currentUser") throw new Error("AUDIT_ROLE_INVALID");
      if (key === "readOnly") throw new Error("AUDIT_READ_ONLY_REQUIRED");
      throw new Error("AUDIT_PRIVILEGE_CONTRACT_FAILED");
    }
  }
  return true;
}

function normalizeText(value) {
  return String(value ?? "").replace(/\s+/g, " ").trim();
}

function safeDefault(value) {
  const text = normalizeText(value);
  if (!text) return null;
  if (/^(now\(\)|current_timestamp|gen_random_uuid\(\)|nextval\('[a-z0-9_.]+'.*\)|true|false|null)$/i.test(text)) return text.toLowerCase();
  return "[redacted-default]";
}

function safeExpression(value) {
  const text = normalizeText(value);
  if (!text) return null;
  if (/['"](?:[^'"]{8,})['"]|\b[0-9a-f]{8}-[0-9a-f-]{27,}\b/i.test(text)) return "[redacted-expression]";
  return text;
}

function pick(source, keys) {
  return Object.fromEntries(keys.map((key) => [key, source?.[key] ?? null]));
}

function sanitizeMetadata(raw) {
  const sorted = (items, key) => [...(Array.isArray(items) ? items : [])].sort((a, b) => String(a[key] ?? "").localeCompare(String(b[key] ?? "")));
  return {
    migrations: sorted(raw.migrations, "version").map(x => pick(x, ["version", "name"])),
    relations: sorted(raw.relations, "name").map(x => ({ ...pick(x, ["name", "objectType", "owner", "rlsEnabled", "rlsForced", "estimatedRows", "columnCount", "pkCount", "fkCount", "uniqueCount", "checkCount", "indexCount", "policyCount", "triggerCount", "anonPrivileges", "authenticatedPrivileges", "serviceRolePrivileges", "auditorPrivileges"]), columns: sorted(x.columns, "ordinal").map(c => ({ ...pick(c, ["name", "ordinal", "type", "nullable"]), default: safeDefault(c.default) })), constraints: sorted(x.constraints, "name").map(c => ({ ...pick(c, ["name", "type", "referencedSchema", "referencedTable", "validated", "deferrable"]), definition: safeExpression(c.definition) })), indexes: sorted(x.indexes, "name").map(i => ({ ...pick(i, ["name", "unique", "primary"]), definition: safeExpression(i.definition) })) })),
    policies: sorted(raw.policies, "identity").map(x => ({ ...pick(x, ["identity", "table", "name", "roles", "command", "permissive"]), using: safeExpression(x.using), withCheck: safeExpression(x.withCheck) })),
    functions: sorted(raw.functions, "identity").map(x => pick(x, ["identity", "owner", "securityDefiner", "volatility", "searchPathConfigured", "publicExecute", "anonExecute", "authenticatedExecute", "serviceRoleExecute"])),
    defaultPrivileges: sorted(raw.defaultPrivileges, "identity").map(x => pick(x, ["identity", "owner", "schema", "objectType", "grantee", "privileges"])),
  };
}

function validateReport(report, knownValues = []) {
  if (!report || Object.keys(report).some(key => !REPORT_KEYS.includes(key)) || REPORT_KEYS.some(key => !(key in report))) throw new Error("AUDIT_REPORT_INVALID");
  const text = JSON.stringify(report);
  const forbidden = [/postgres(?:ql)?:\/\//i, /\bBearer\s+/i, /-----BEGIN [A-Z ]*PRIVATE KEY-----/, /eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}/, /\b[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\b/i, /(?:access_token|refresh_token|ciphertext|keyId|pkce_verifier|oauth_state)\s*[":=]/i, /(?:pooler|supabase)\.(?:com|net)(?:["/:]|\\)/i];
  if (forbidden.some(re => re.test(text)) || knownValues.filter(Boolean).some(value => text.includes(value))) throw new Error("AUDIT_REPORT_INVALID");
  return true;
}

module.exports = { CLASSIFICATIONS, REPORT_KEYS, assertAcceptance, normalizeText, safeDefault, safeExpression, sanitizeMetadata, validateReport };
