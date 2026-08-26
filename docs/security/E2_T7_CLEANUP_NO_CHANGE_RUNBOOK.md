# E2-T7 Fixture Cleanup and No-Change Acceptance

## Status and boundary

**Status: `Verification`.** This is a preparation-only repository package. PR #27 performs no live Supabase query, Management API call, cleanup, final check, fixture mutation, schema/policy/grant/ledger change, environment change, or deployment. Static tests do not replace live acceptance. E2-T3, E2-T4, E2-T5, and E2-T6 remain `Verification`; E2-T8 restore readiness remains open. E2-T7 cannot become `Done` without live final evidence and human review.

The mandatory normal cleanup for every E2-T3–T6 acceptance transaction is its intact outer `ROLLBACK`. E2-T7 never runs an automatic `DELETE` or an ad hoc cleanup statement. A non-zero aggregate residue is a fail-closed **STOP**: report only the affected namespace and aggregate count, expose no user/entity identity, run no cleanup, and request separate human investigation and recovery approval.

## Exact source and fixture contract

Before use, an operator must verify the exact approved `main` commit and checksums of this runbook, both SQL files, both contracts, the converter, and the merged E2-T3–T6 source SQL. Fixture selectors are taken verbatim from those merged artifacts:

- E2-T3 exact equality: `meta:e2_t3_static_v2_account:paid:none:campaign:e2_t3_static_v2_campaign:ad:e2_t3_static_v2_ad`
- E2-T4 exact equality: `meta:e2_t4_same_key_v2_account:paid:none:campaign:e2_t4_same_key_v2_campaign:ad:e2_t4_same_key_v2_ad`
- E2-T5 escaped literal prefix: `e2\_t5\_rejection\_v1:`
- E2-T6 escaped literal prefix: `e2\_t6\_rls\_v1:`

Broad wildcard matching is prohibited.

## Human-approved operation order

1. With separately approved credentials, copy `E2_T7_BASELINE.sql`, verify its checksum, and execute it first under the approved read-only procedure. Without credentials, **STOP**; this PR supplies none.
2. Require every baseline gate to pass. Capture exactly five baselines in this order: Dataset V2, V1, snapshot, connected, encrypted. Store them only in operator-local evidence. Actual provider counts are never shared or committed. Any residue or security/metadata gate failure stops the entire later E2 series.
3. Run each E2-T3–T6 live operation only under its own separate human approval and intact outer rollback. Never retry an operation whose outcome is ambiguous.
4. Only after all approved operations, make an operator-local copy of `E2_T7_FINAL_CHECK.sql`; replace its exactly five explicit negative placeholders, in this order: Dataset V2, V1, snapshot, connected, encrypted. Verify the edited copy locally and execute only through the approved read-only procedure.
5. Pass the separate baseline and final result arrays directly to `buildEvidence`. No manual result merge is required. Retain only its redacted allowlisted summary for review.

The final check proves exact V2/V1/snapshot parity, zero fixture residue, unchanged ledger/OAuth/encrypted-token/plaintext-token postconditions, unchanged Dataset V2 constraint/index/RLS/policy/grant state, and zero persistent evidence objects. Neither SQL emits identities or production rows. Production counts remain operator-local.

## STOP and review

A failed check, missing/extra/duplicate check, unknown field, unsafe value, sensitive pattern, non-zero residue, or parity difference is a STOP. Do not reveal raw input, retry an ambiguous operation, or improvise cleanup. Record only safe check codes and aggregate residue counts, then obtain separate human review and recovery authorization. E2-T7 remains `Verification` until approved live final evidence exists.

## E2-C1 deviation / decision (corrective preparation)

- İlk repository hazırlığı, E1 kapanış anındaki 7/7 provider bağlantı/token nüfusunu sabit kabul etmişti.
- Canlı read-only E2-T3 preflight, connection/token nüfusunun değişebildiğini kanıtladı; hardcoded kontroller bu nedenle başarısız oldu.
- Actual production sayıları evidence'a veya repository'ye alınmadı; yalnız operator-local baseline olarak tutulmalıdır.
- Missing encrypted ve plaintext güvenlik kontrolleri geçti; corrective sözleşme ayrıca orphan encrypted kontrolünü zorunlu kılar.
- Güvenlik contract'ı fixed population yerine captured connected/encrypted parity ile missing/orphan/plaintext zero olarak düzeltildi.
- Bu değişiklik production data correction değildir. Bu corrective PR kapsamında canlı transaction, INSERT veya postcheck çalıştırılmadı ve production değişmedi.
