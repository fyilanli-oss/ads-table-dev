# E3-T1 — V1 critical-route characterization baseline

## Purpose

This baseline freezes a deliberately small set of externally visible V1 HTTP
behaviours before the E3 composition-root and route extractions begin. It is a
characterization contract, not a redesign of the current monolith.

## Protected behaviours

| Surface | Request | Characterized result |
|---|---|---|
| Landing page | `GET /` | `200`, HTML response containing the AdsTable product name |
| Login page | `GET /login` | `200`, HTML response |
| Public runtime config | `GET /api/public-config` | `200`, JSON with the stable `supabaseUrl` and `supabaseAnonKey` keys |
| Protected account status | `GET /api/account/status` without bearer auth | `401`, exact `{ "error": "Not authenticated" }` contract |
| TikTok test surface | `GET /tiktok-test` with the production flag disabled | `404` |
| Unknown API route | `GET /api/__e3_characterization_missing__` | `404` |

The test binds the exported Express application to an ephemeral loopback port.
It neither starts the production entrypoint nor calls provider, Supabase, or
other network services.

## Current responsibility map

- `server.js` creates the Express application, reads environment configuration,
  constructs shared clients, registers static/public/authenticated routes, owns
  provider and snapshot orchestration, starts the process listener, and exports
  the application.
- `security/` owns the already-extracted E1 configuration, authorization,
  OAuth-transaction, and provider-token security boundaries.
- `funnel-core/` owns the canonical analytics contracts and query services.
- `public/` owns the existing static V1 web surfaces.

## Extraction rule

E3-T2 and later extractions must keep this test green. A deliberate contract
change requires an explicit Execution Plan decision record; updating the
expected values merely to accommodate an accidental extraction regression is
not acceptable.

