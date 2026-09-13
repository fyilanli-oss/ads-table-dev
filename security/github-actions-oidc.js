"use strict";
const crypto = require("node:crypto");
const ISSUER = "https://token.actions.githubusercontent.com";
const JWKS_URL = `${ISSUER}/.well-known/jwks`;
function decodePart(value) { return JSON.parse(Buffer.from(value, "base64url").toString("utf8")); }
async function verifyGitHubActionsOidc(token, {audience, fetchImpl = fetch, now = () => Date.now()} = {}) {
  const parts = String(token || "").split(".");
  if (parts.length !== 3 || !audience) throw new Error("OIDC_TOKEN_INVALID");
  const header = decodePart(parts[0]); const claims = decodePart(parts[1]);
  if (header.alg !== "RS256" || typeof header.kid !== "string") throw new Error("OIDC_HEADER_INVALID");
  const response = await fetchImpl(JWKS_URL, {headers: {Accept: "application/json"}});
  if (!response.ok) throw new Error("OIDC_JWKS_UNAVAILABLE");
  const body = await response.json();
  const key = Array.isArray(body.keys) && body.keys.find(item => item.kid === header.kid && item.kty === "RSA");
  if (!key) throw new Error("OIDC_SIGNING_KEY_UNKNOWN");
  const valid = crypto.verify("RSA-SHA256", Buffer.from(`${parts[0]}.${parts[1]}`), crypto.createPublicKey({key, format: "jwk"}), Buffer.from(parts[2], "base64url"));
  if (!valid) throw new Error("OIDC_SIGNATURE_INVALID");
  const seconds = Math.floor(now() / 1000);
  const expected = {iss: ISSUER, aud: audience, repository: "fyilanli-oss/ads-table-dev", ref: "refs/heads/main", event_name: "workflow_dispatch", environment: "Production"};
  for (const [name, value] of Object.entries(expected)) if (claims[name] !== value) throw new Error(`OIDC_${name.toUpperCase()}_INVALID`);
  if (!Number.isInteger(claims.exp) || claims.exp <= seconds || !Number.isInteger(claims.iat) || claims.iat > seconds + 30) throw new Error("OIDC_TIME_INVALID");
  if (claims.nbf !== undefined && (!Number.isInteger(claims.nbf) || claims.nbf > seconds + 30)) throw new Error("OIDC_TIME_INVALID");
  const allowedWorkflows = [
    "/.github/workflows/e10-t6c-production-activation.yml@refs/heads/main",
    "/.github/workflows/e10-t6c-production-readiness.yml@refs/heads/main"
  ];
  if (typeof claims.workflow_ref !== "string" || !allowedWorkflows.some(workflow => claims.workflow_ref.endsWith(workflow))) throw new Error("OIDC_WORKFLOW_INVALID");
  if (!/^\d+$/.test(String(claims.run_id || "")) || !/^\d+$/.test(String(claims.run_attempt || ""))) throw new Error("OIDC_RUN_INVALID");
  return Object.freeze({run_id: String(claims.run_id), run_attempt: String(claims.run_attempt)});
}
module.exports = Object.freeze({ISSUER, JWKS_URL, verifyGitHubActionsOidc});
