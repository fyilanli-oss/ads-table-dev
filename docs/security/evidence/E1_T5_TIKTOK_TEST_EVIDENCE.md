# E1-T5 — TikTok test and unsafe-default evidence

**Date:** 2026-08-18
**Evidence format:** Text/Markdown only
**Status:** Done; E1-T6 is next

## Implemented controls

- Review hard-routing, review fallback, and TikTok sandbox behavior default to disabled.
- Production startup fails before serving traffic if any review or sandbox switch is enabled.
- The TikTok test page is hidden unless sandbox mode is explicitly enabled outside production.
- Manual sandbox tokens are accepted only through `X-Sandbox-Access-Token`; query-string token transport is rejected by construction.
- The normal TikTok connection-token path remains in place when `sandbox=false`.
- Known Google and TikTok review identities were removed from backend and UI runtime sources.
- The test advertiser input is empty and the sandbox checkbox is unchecked by default.

## Automated evidence

`tests/production-config.test.js` covers pure configuration defaults, production fail-fast behavior, secret-safe errors, unsafe-source scanning, safe UI defaults, header-only token transport, and this route matrix:

| Environment | Explicit sandbox | `/tiktok-test` |
|---|---:|---:|
| production | false | 404 |
| development | false | 404 |
| development | true | 200 |

The acceptance run also includes the repository test suite, JavaScript syntax checks, the required runtime/UI source scan, diff whitespace validation, changed-file allowlisting, and binary-extension/numstat checks. No screenshot or binary evidence is created or tracked.

## Remaining boundary

E1-T6 provider token-at-rest protection and E1-T7 full regression/CI gating remain pending. Automated tests do not execute production provider consent flows.
