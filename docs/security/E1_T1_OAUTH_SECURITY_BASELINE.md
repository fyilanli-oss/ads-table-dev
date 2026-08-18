# E1-T1 — OAuth Security Baseline

**Date:** 2026-08-18  
**Status:** E1-T1 baseline retained; E1-T2 through E1-T5 implemented

**Next gate:** E1-T6B/C/D live deployment readiness: encrypted schema acceptance, key provisioning, dry-run review and feature-flag activation. Vault, migration, dual-path and backfill/rotation code artefacts are complete.

## Purpose

This baseline freezes the current OAuth surface before security behavior is changed. It is deliberately split from the remediation so the next changes can be reviewed against executable current-state evidence.

## Route matrix

| Provider | Start | Callback | Current user source | State/session | PKCE | Status |
|---|---|---|---|---|---|---|
| Meta | `/auth/meta` | `/auth/meta/callback` | Verified bearer user | Atomic transaction | No | Active |
| Google Ads | `/auth/google` | `/auth/google/callback` | Verified bearer user | Atomic transaction | No | Active |
| Google Sheets | `/auth/google-sheets` | `/auth/google-sheets/callback` | Verified bearer user | Atomic transaction | No | Active |
| GA4 Organic | `/auth/organic` | `/auth/organic/callback` | Verified bearer user | Atomic transaction | No | Active |
| Pinterest | `/auth/pinterest` | `/auth/pinterest/callback` | None; passive legacy redirect | None | No | Passive |
| Klaviyo | `/auth/klaviyo` | `/auth/klaviyo/callback` | Verified bearer user | Atomic transaction + verifier | Yes | Active |
| TikTok | `/auth/tiktok` | `/auth/tiktok/callback` | Verified bearer user | Atomic transaction | No | Active |

The executable inventory is `security/oauth-route-inventory.js`. The baseline test fails when an `/auth` route is introduced without an inventory decision.

## Identity flow

```text
Dashboard Supabase access token
→ bearer-authenticated JSON OAuth start handshake
→ requireUser
→ reject any legacy query user_id
→ subscription lookup for verified user.id
→ SHA-256 state digest + user/provider/exact redirect transaction
→ provider callback
→ atomic, expiring DELETE ... RETURNING consume
→ transaction-bound verified user
→ token exchange
→ saveConnection(transaction user, provider tokens)
```

The shared start guard makes the verified bearer identity authoritative. E1-T3 correlates callbacks through a server-only PostgreSQL record containing only a SHA-256 state digest. It binds user, provider and exact redirect URI, expires after ten minutes, and is consumed using `DELETE ... RETURNING`. Klaviyo's PKCE verifier travels in that record. Active callbacks no longer use session identity or state. E1-T4 verified there were no remaining runtime session consumers and removed the obsolete Express session layer instead of adding an unused shared store.

## Threat model

| ID | Threat | Current control | Required E1 control |
|---|---|---|---|
| OAUTH-IDENTITY-001 | Query `user_id` selects another AdsTable user | **Resolved:** query rejected before subscription lookup | Retain tamper regression test |
| OAUTH-AUTH-002 | Unauthenticated OAuth start | **Resolved:** `requireUser` returns `401` | Retain unauthenticated regression test |
| OAUTH-REPLAY-003 | Callback transaction replay | Session state comparison; inconsistent cleanup | Single-use atomic transaction consume with TTL |
| OAUTH-RACE-004 | Concurrent provider flows overwrite shared `oauthUserId` | Provider state is separate; user field is often shared | Transaction-specific user/provider binding |
| OAUTH-EXPIRY-005 | Stale state remains usable | No common transaction TTL | Server-enforced expiry |
| SESSION-STORE-006 | Callback reaches another instance | **Resolved:** callbacks use the shared atomic transaction store; the unused session layer was removed | Retain session-elimination and transaction-store regression guards |
| SESSION-SECRET-007 | Production starts with known fallback secret | **Resolved:** session middleware, secret dependency and fallback were removed | Retain runtime-source and dependency regression guards |
| TOKEN-LOG-008 | Provider token leaks through logs/evidence | No centralized evidence guard | Redaction tests and structured logging policy |

## Characterization evidence

`tests/oauth-security-baseline.test.js` verifies that:

1. The inventory covers every registered `/auth` route.
2. Every active start route currently uses the shared guard.
3. The shared guard resolves the verified bearer user and rejects legacy query-user input.
4. Every active start/callback uses the transaction store and callbacks contain no OAuth session identity/state fields.
5. Runtime and package guards prove the session middleware, session access, secret dependency, known fallback and package dependency are absent.
6. Pinterest remains a passive legacy dashboard redirect and never accesses a session object.

The original assertions described current debt rather than the desired end state. E1-T2 replaced the query-user assertion with executable bearer-user, unauthenticated and tamper rejection acceptance tests. E1-T4 replaced the session-debt characterization with executable elimination guards.

## E1-T1 acceptance

- All OAuth start/callback routes are represented.
- Identity, state, session and PKCE fields are explicit.
- The historical shared query-user risk is represented and its remediation is programmatically enforced.
- No real provider call, token, secret, database write or session migration is performed.
- The next remediation boundary is explicit.

## Rollback

E1-T1 added inventory, documentation and tests. E1-T2 changed start authentication and the dashboard handshake. E1-T3 adds the transaction schema and swaps callback correlation without changing provider token exchange or `platform_connections` behavior. E1-T4 removes only the now-unused session layer. The transaction migration must be applied before deploying the server. Rollback may disable new connection starts, but must not restore query-controlled or session-bound OAuth identity, and the known fallback secret must never be restored. Session infrastructure may be reconsidered only if an unknown consumer is demonstrated and receives a new security review.

## Next implementation package

E1-T5 is complete: production rejects review/sandbox switches, test routes require explicit non-production sandbox mode, and known review identities and query-string sandbox tokens are absent from runtime/UI sources. E1-T6A adds the versioned AES-256-GCM vault; E1-T6B adds the additive forced-RLS token table; E1-T6C adds feature-flagged encrypted writes, explicit legacy reads, fail-closed decrypt and encrypted disconnect cleanup. Runtime behavior remains unchanged while `PROVIDER_TOKEN_ENCRYPTION_ENABLED` is disabled. Live schema acceptance and key provisioning must precede activation.

E1-T6D is **`activation tooling ready / live acceptance pending`**. Its operator defaults to dry-run, normalizes the Supabase project origin, requires an exact project-ref guard, and gates write mode behind both an environment policy and explicit confirmation. Evidence excludes tokens, keys, ciphertext, exception messages and real user IDs. Production key provisioning was not performed, no live dry-run or write-mode backfill was run, the encryption flag was not enabled, plaintext columns were not cleaned, and E1-T6E retirement has not started.
