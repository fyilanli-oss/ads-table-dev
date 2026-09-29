"use strict";

const DEFAULT_URL = "https://dev.adstable.app/";
const EXPECTED_RELEASE = "r7b2-settings-v1";

function validateSurface({status, headers, body}, expectedRelease = EXPECTED_RELEASE) {
  const release = headers.get("x-adstable-release");
  const checks = Object.freeze({
    status_ok: status === 200,
    release_header_matches: release === expectedRelease,
    app_bridge_present: body.includes("https://cdn.shopify.com/shopifycloud/app-bridge.js"),
    polaris_present: body.includes("https://cdn.shopify.com/shopifycloud/polaris-1.js"),
    shopify_page_present: body.includes('<s-page heading="Dashboard">'),
    settings_navigation_present: body.includes('<s-link href="/shopify/app/settings">Settings</s-link>'),
    data_sources_promo_absent: !body.includes("Manage data sources") && !body.includes("Set up data sources"),
    custom_style_absent: !body.includes("<style>"),
    nested_iframe_absent: !/<iframe/i.test(body),
  });
  return Object.freeze({
    contract_version: "e10-production-surface-probe-v1",
    observed_status: Number.isInteger(status) ? status : "invalid",
    expected_release: expectedRelease,
    observed_release: typeof release === "string" && /^[a-z0-9-]{1,32}$/.test(release) ? release : "missing_or_invalid",
    checks,
    pass: Object.values(checks).every(Boolean),
  });
}

async function probe({url = DEFAULT_URL, expectedRelease = EXPECTED_RELEASE, fetchImpl = fetch} = {}) {
  const target = new URL(url);
  if (target.protocol !== "https:" || target.hostname !== "dev.adstable.app" || target.pathname !== "/") {
    throw new TypeError("production probe URL must be the canonical App Home root");
  }
  target.searchParams.set("surface_probe", expectedRelease);
  const response = await fetchImpl(target, {
    method: "GET",
    redirect: "error",
    headers: {Accept: "text/html", "Cache-Control": "no-cache"},
  });
  const body = await response.text();
  return validateSurface({status: response.status, headers: response.headers, body}, expectedRelease);
}

async function main() {
  const result = await probe();
  process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
  if (!result.pass) process.exitCode = 1;
}

if (require.main === module) main().catch(() => {
  process.stderr.write('{"contract_version":"e10-production-surface-probe-v1","pass":false,"error":"PROBE_FAILED"}\n');
  process.exitCode = 1;
});

module.exports = Object.freeze({DEFAULT_URL, EXPECTED_RELEASE, probe, validateSurface});
