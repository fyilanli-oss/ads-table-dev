# E1-T1 — OAuth Security Baseline

**Date:** 2026-08-18  
**Status:** E1-T1 baseline retained; E1-T2 bearer-bound identity implemented
**Next gate:** E1-T3 single-use OAuth transaction store

## Purpose

This baseline freezes the current OAuth surface before security behavior is changed. It is deliberately split from the remediation so the next changes can be reviewed against executable current-state evidence.

## Route matrix

| Provider | Start | Callback | Current user source | State/session | PKCE | Status |
|---|---|---|---|---|---|---|
| Meta | `/auth/meta` | `/auth/meta/callback` | Verified bearer user | `metaOAuthState`, `oauthUserId` | No | Active |
| Google Ads | `/auth/google` | `/auth/google/callback` | Verified bearer user | `googleOAuthState`, `oauthUserId` | No | Active |
| Google Sheets | `/auth/google-sheets` | `/auth/google-sheets/callback` | Verified bearer user | `googleSheetsOAuthState`, `googleSheetsOAuthUserId` | No | Active |
| GA4 Organic | `/auth/organic` | `/auth/organic/callback` | Verified bearer user | `organicOAuthState`, `oauthUserId` | No | Active |
| Pinterest | `/auth/pinterest` | `/auth/pinterest/callback` | None; passive legacy redirect | Legacy fields cleared | No | Passive |
| Klaviyo | `/auth/klaviyo` | `/auth/klaviyo/callback` | Verified bearer user | `klaviyoOAuthState`, `oauthUserId`, verifier | Yes | Active |
| TikTok | `/auth/tiktok` | `/auth/tiktok/callback` | Verified bearer user | `tiktokOAuthState`, `oauthUserId` | No | Active |

The executable inventory is `security/oauth-route-inventory.js`. The baseline test fails when an `/auth` route is introduced without an inventory decision.

## Identity flow

```text
Dashboard Supabase access token
→ bearer-authenticated JSON OAuth start handshake
→ requireUser
→ reject any legacy query user_id
→ subscription lookup for verified user.id
→ provider-specific state + session user field
→ provider callback
→ state comparison
→ session user field
→ token exchange
→ saveConnection(session user, provider tokens)
```

The shared start guard now makes the verified bearer identity authoritative. The callback is still correlated through provider-specific session fields; transaction-specific TTL, atomic consume and multi-instance persistence remain E1-T3/E1-T4 work.

## Threat model

| ID | Threat | Current control | Required E1 control |
|---|---|---|---|
| OAUTH-IDENTITY-001 | Query `user_id` selects another AdsTable user | **Resolved:** query rejected before subscription lookup | Retain tamper regression test |
| OAUTH-AUTH-002 | Unauthenticated OAuth start | **Resolved:** `requireUser` returns `401` | Retain unauthenticated regression test |
| OAUTH-REPLAY-003 | Callback transaction replay | Session state comparison; inconsistent cleanup | Single-use atomic transaction consume with TTL |
| OAUTH-RACE-004 | Concurrent provider flows overwrite shared `oauthUserId` | Provider state is separate; user field is often shared | Transaction-specific user/provider binding |
| OAUTH-EXPIRY-005 | Stale state remains usable | No common transaction TTL | Server-enforced expiry |
| SESSION-STORE-006 | Callback reaches another instance | Default in-memory session store | Shared TTL-capable store |
| SESSION-SECRET-007 | Production starts with known fallback secret | Development fallback is unconditional | Production fail-fast configuration |
| TOKEN-LOG-008 | Provider token leaks through logs/evidence | No centralized evidence guard | Redaction tests and structured logging policy |

## Characterization evidence

`tests/oauth-security-baseline.test.js` verifies that:

1. The inventory covers every registered `/auth` route.
2. Every active start route currently uses the shared guard.
3. The shared guard resolves the verified bearer user and rejects legacy query-user input.
4. Active callbacks currently use their documented session state/user fields.
5. The known session-secret fallback and default-store debt remain visible until E1-T4.

The original assertions described current debt rather than the desired end state. E1-T2 replaced the query-user assertion with executable bearer-user, unauthenticated and tamper rejection acceptance tests; E1-T4 still owns the session fallback assertions.

## E1-T1 acceptance

- All OAuth start/callback routes are represented.
- Identity, state, session and PKCE fields are explicit.
- The historical shared query-user risk is represented and its remediation is programmatically enforced.
- No real provider call, token, secret, database write or session migration is performed.
- The next remediation boundary is explicit.

## Rollback

E1-T1 added inventory, documentation and tests only. E1-T2 changes OAuth start authentication and the dashboard handshake without changing callback token exchange or database schema. Rollback may disable the new start flow, but must not restore query-controlled identity.

## Next implementation package

E1-T2 makes the verified bearer user authoritative for every active OAuth start route and adds negative tests for unauthenticated access and query-user tampering. It does not introduce the shared transaction store; that remains E1-T3.
