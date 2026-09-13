"use strict";

const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const assert = require("node:assert/strict");
const os = require("node:os");
const crypto = require("node:crypto");
const {SURFACES, VISUAL_GATES, PRIVACY_GATES, FORBIDDEN_INTERACTIONS, inspectPng, validateV9Evidence} = require("../security/e10-v9-real-device-evidence");

const root = path.join(__dirname, "..");
const contract = JSON.parse(fs.readFileSync(path.join(root, "contracts/shopify/e10-t6-c2i-v9-real-device-acceptance.json"), "utf8"));
const doc = fs.readFileSync(path.join(root, "docs/E10_T6_C2I_V9_REAL_DEVICE_ACCEPTANCE.md"), "utf8");
const template = JSON.parse(fs.readFileSync(path.join(root, "contracts/shopify/e10-t6-c2i-v9-evidence.template.json"), "utf8"));

test("V9 approval authorizes capture but cannot claim acceptance without evidence", () => {
  assert.equal(contract.execution_approved, true);
  assert.equal(contract.status, "AWAITING_REDACTED_REAL_DEVICE_EVIDENCE");
  assert.equal(contract.acceptance_passed, false);
  assert.equal(contract.capture_authority, "human_in_authenticated_shopify_admin");
  assert.deepEqual(contract.required_captures.map(({surface, route}) => ({surface, route})), [
    {surface: "app_home", route: "/shopify/app"},
    {surface: "data_sources_platforms", route: "/shopify/app/platforms"},
  ]);
  for (const capture of contract.required_captures) {
    assert.equal(capture.redacted_file, null);
    assert.equal(capture.sha256, null);
  }
});

test("V9 requires Shopify-native visuals and rejects imitation surfaces", () => {
  assert.deepEqual(contract.required_visual_gates, VISUAL_GATES);
  assert.match(doc, /source\/test kapıları ile insan render kanıtı birlikte geçmelidir/);
});

test("capture evidence is redacted before it enters the repository", () => {
  assert.deepEqual(contract.required_privacy_gates, PRIVACY_GATES);
  assert.match(doc, /Repository'ye eklemeden \*\*önce\*\*/);
  assert.match(contract.acceptance_rule, /both redacted captures/);
  assert.match(contract.acceptance_rule, /human privacy attestation/);
});

test("V9 never authorizes Connect or provider contact", () => {
  assert.deepEqual(contract.forbidden_interactions, FORBIDDEN_INTERACTIONS);
  assert.equal(contract.next_on_pass, "E10_T6_C2I_V10A_KLAVIYO_CONNECT_MODAL");
  assert.equal(contract.next_on_fail, "E10_T6_C2I_V9_CORRECTIVE");
  assert.match(doc, /Hiçbir provider `Connect` düğmesine basma/);
  assert.match(doc, /V9 `PASS` verilene kadar V10-A dahil hiçbir Provider Connect execution alt paketi başlamaz/);
});

function png(width, height, metadata = false) {
  const chunk = (type, data) => Buffer.concat([
    Buffer.from([data.length >>> 24, data.length >>> 16, data.length >>> 8, data.length]),
    Buffer.from(type), data, Buffer.alloc(4),
  ]);
  const ihdr = Buffer.alloc(13);
  ihdr.writeUInt32BE(width, 0);
  ihdr.writeUInt32BE(height, 4);
  ihdr[8] = 8;
  ihdr[9] = 6;
  return Buffer.concat([
    Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]), chunk("IHDR", ihdr),
    ...(metadata ? [chunk("tEXt", Buffer.from("identity=forbidden"))] : []),
    chunk("IDAT", Buffer.from([0])), chunk("IEND", Buffer.alloc(0)),
  ]);
}

function passingEvidence(root) {
  const captures = SURFACES.map((item, index) => {
    const contents = png(800 + index, 1200);
    const absolute = path.join(root, item.file);
    fs.mkdirSync(path.dirname(absolute), {recursive: true});
    fs.writeFileSync(absolute, contents);
    return {surface: item.surface, route: item.route, redacted_file: item.file, sha256: crypto.createHash("sha256").update(contents).digest("hex")};
  });
  return {
    contract_version: "e10-t6-c2i-v9-real-device-evidence-v1",
    status: "PASS", acceptance_passed: true, captured_at: "2026-09-13T12:00:00Z", captures,
    visual_gates: Object.fromEntries(VISUAL_GATES.map(key => [key, true])),
    privacy_gates: Object.fromEntries(PRIVACY_GATES.map(key => [key, true])),
    forbidden_interactions: Object.fromEntries(FORBIDDEN_INTERACTIONS.map(key => [key, false])),
  };
}

test("validator accepts only two distinct allowlisted, hashed, metadata-free PNG captures", () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "e10-v9-"));
  const result = validateV9Evidence(passingEvidence(root), {root});
  assert.equal(result.status, "PASS");
  assert.equal(result.captures.length, 2);
  assert.notEqual(result.captures[0].sha256, result.captures[1].sha256);
});

test("validator rejects missing attestations, interaction, hash drift, and PNG metadata", () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "e10-v9-"));
  const evidence = passingEvidence(root);
  assert.throws(() => validateV9Evidence({...evidence, privacy_gates: {...evidence.privacy_gates, human_privacy_attestation: false}}, {root}), /human_privacy_attestation/);
  assert.throws(() => validateV9Evidence({...evidence, forbidden_interactions: {...evidence.forbidden_interactions, provider_connect_pressed: true}}, {root}), /provider_connect_pressed/);
  assert.throws(() => validateV9Evidence({...evidence, captures: evidence.captures.map((capture, index) => index ? capture : {...capture, sha256: "0".repeat(64)})}, {root}), /sha256 mismatch/);
  assert.throws(() => inspectPng(png(800, 1200, true)), /forbidden metadata/);
});

test("intake template is visibly non-accepting and cannot pass the validator", () => {
  assert.equal(template.status, "REVIEW_REQUIRED");
  assert.equal(template.acceptance_passed, false);
  assert.ok(Object.values(template.visual_gates).every(value => value === false));
  assert.ok(Object.values(template.privacy_gates).every(value => value === false));
  assert.ok(Object.values(template.forbidden_interactions).every(value => value === false));
  assert.throws(() => validateV9Evidence(template, {root}), /cannot claim V9 acceptance/);
  assert.match(doc, /Template hiçbir koşulda acceptance evidence veya `PASS` sayılamaz/);
});
