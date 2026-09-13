"use strict";

const crypto = require("node:crypto");
const fs = require("node:fs");
const path = require("node:path");

const CONTRACT_VERSION = "e10-t6-c2i-v9-real-device-evidence-v1";
const EVIDENCE_DIRECTORY = "artifacts/e10-shopify/e10-t6-c2i-v9";
const SURFACES = Object.freeze([
  Object.freeze({surface: "app_home", route: "/shopify/app", file: `${EVIDENCE_DIRECTORY}/app-home-redacted.png`}),
  Object.freeze({surface: "data_sources_platforms", route: "/shopify/app/platforms", file: `${EVIDENCE_DIRECTORY}/platforms-redacted.png`}),
]);
const VISUAL_GATES = Object.freeze([
  "inside_shopify_admin", "official_shopify_components_rendered", "app_navigation_visible",
  "app_home_manage_data_sources_action_visible", "platforms_provider_actions_visible",
  "custom_admin_shell_absent", "custom_component_css_absent", "technical_release_marker_absent",
  "nested_provider_iframe_absent",
]);
const PRIVACY_GATES = Object.freeze([
  "shop_identity_absent", "account_identity_absent", "person_identity_absent",
  "browser_chrome_identity_absent", "token_code_credential_absent",
  "redaction_completed_before_repository_add", "human_privacy_attestation",
]);
const FORBIDDEN_INTERACTIONS = Object.freeze([
  "provider_connect_pressed", "provider_consent_started", "provider_api_contact",
  "oauth_callback_exercised", "production_mutation",
]);
const PNG_SIGNATURE = Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]);
const FORBIDDEN_PNG_CHUNKS = new Set(["tEXt", "zTXt", "iTXt", "eXIf"]);

function exactKeys(value, expected, label) {
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error(`${label} must be an object`);
  const actual = Object.keys(value).sort();
  const wanted = [...expected].sort();
  if (actual.length !== wanted.length || actual.some((key, index) => key !== wanted[index])) {
    throw new Error(`${label} has an invalid shape`);
  }
}

function validateGateObject(value, expected, requiredValue, label) {
  exactKeys(value, expected, label);
  for (const key of expected) if (value[key] !== requiredValue) throw new Error(`${label}.${key} is invalid`);
}

function validateTimestamp(value) {
  if (typeof value !== "string" || !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{3})?Z$/.test(value)) {
    throw new Error("captured_at must be an ISO UTC timestamp");
  }
  const timestamp = Date.parse(value);
  if (!Number.isFinite(timestamp) || timestamp > Date.now()) throw new Error("captured_at is invalid");
}

function inspectPng(buffer) {
  if (buffer.length < 45 || !buffer.subarray(0, 8).equals(PNG_SIGNATURE)) throw new Error("capture must be a PNG image");
  let offset = 8;
  let width = null;
  let height = null;
  let sawIend = false;
  while (offset + 12 <= buffer.length) {
    const length = buffer.readUInt32BE(offset);
    const end = offset + 12 + length;
    if (end > buffer.length) throw new Error("PNG chunk is truncated");
    const type = buffer.toString("ascii", offset + 4, offset + 8);
    if (FORBIDDEN_PNG_CHUNKS.has(type)) throw new Error("PNG contains forbidden metadata");
    if (offset === 8 && type !== "IHDR") throw new Error("PNG must begin with IHDR");
    if (type === "IHDR") {
      if (length !== 13 || width !== null) throw new Error("PNG IHDR is invalid");
      width = buffer.readUInt32BE(offset + 8);
      height = buffer.readUInt32BE(offset + 12);
    }
    offset = end;
    if (type === "IEND") {
      if (length !== 0 || offset !== buffer.length) throw new Error("PNG IEND is invalid");
      sawIend = true;
      break;
    }
  }
  if (!sawIend || width < 320 || height < 480 || width > 10000 || height > 10000) {
    throw new Error("PNG dimensions or structure are invalid");
  }
  return Object.freeze({width, height});
}

function readCapture(root, expected, capture) {
  exactKeys(capture, ["surface", "route", "redacted_file", "sha256"], `capture ${expected.surface}`);
  if (capture.surface !== expected.surface || capture.route !== expected.route || capture.redacted_file !== expected.file) {
    throw new Error(`capture ${expected.surface} does not match the allowlist`);
  }
  if (!/^[a-f0-9]{64}$/.test(capture.sha256 || "")) throw new Error(`capture ${expected.surface} sha256 is invalid`);
  const absoluteRoot = path.resolve(root);
  const absoluteFile = path.resolve(absoluteRoot, capture.redacted_file);
  if (!absoluteFile.startsWith(`${absoluteRoot}${path.sep}`)) throw new Error("capture path escapes repository root");
  const stat = fs.lstatSync(absoluteFile);
  if (!stat.isFile() || stat.isSymbolicLink() || stat.size > 10 * 1024 * 1024) throw new Error("capture must be a bounded regular file");
  const contents = fs.readFileSync(absoluteFile);
  const dimensions = inspectPng(contents);
  const digest = crypto.createHash("sha256").update(contents).digest("hex");
  if (digest !== capture.sha256) throw new Error(`capture ${expected.surface} sha256 mismatch`);
  return Object.freeze({surface: capture.surface, sha256: digest, ...dimensions});
}

function validateV9Evidence(evidence, {root = path.join(__dirname, "..")} = {}) {
  exactKeys(evidence, [
    "contract_version", "status", "acceptance_passed", "captured_at", "captures",
    "visual_gates", "privacy_gates", "forbidden_interactions",
  ], "evidence");
  if (evidence.contract_version !== CONTRACT_VERSION || evidence.status !== "PASS" || evidence.acceptance_passed !== true) {
    throw new Error("evidence cannot claim V9 acceptance");
  }
  validateTimestamp(evidence.captured_at);
  validateGateObject(evidence.visual_gates, VISUAL_GATES, true, "visual_gates");
  validateGateObject(evidence.privacy_gates, PRIVACY_GATES, true, "privacy_gates");
  validateGateObject(evidence.forbidden_interactions, FORBIDDEN_INTERACTIONS, false, "forbidden_interactions");
  if (!Array.isArray(evidence.captures) || evidence.captures.length !== SURFACES.length) throw new Error("exactly two captures are required");
  const captures = SURFACES.map((expected, index) => readCapture(root, expected, evidence.captures[index]));
  if (captures[0].sha256 === captures[1].sha256) throw new Error("captures must be distinct images");
  return Object.freeze({contract_version: CONTRACT_VERSION, status: "PASS", acceptance_passed: true, captures: Object.freeze(captures)});
}

module.exports = Object.freeze({
  CONTRACT_VERSION, EVIDENCE_DIRECTORY, FORBIDDEN_INTERACTIONS, PRIVACY_GATES, SURFACES, VISUAL_GATES,
  inspectPng, validateV9Evidence,
});
