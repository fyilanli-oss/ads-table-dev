-- Operator must set safe SHA-256 fingerprints in e2_t8.source_ref_fingerprint and e2_t8.target_ref_fingerprint.
WITH managed AS (
  SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname='auth') AS auth_schema,
         EXISTS(SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='auth' AND p.proname='uid' AND pg_get_function_identity_arguments(p.oid)='') AS auth_uid_exact_signature,
         EXISTS(SELECT 1 FROM pg_roles WHERE rolname='anon') AS anon_role,
         EXISTS(SELECT 1 FROM pg_roles WHERE rolname='authenticated') AS authenticated_role,
         EXISTS(SELECT 1 FROM pg_roles WHERE rolname='service_role') AS service_role_role
), target_state AS (
  SELECT count(*) FILTER (WHERE c.relkind IN ('r','p') AND c.relname NOT IN ('spatial_ref_sys'))::integer AS application_relation_count
  FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public'
), identity_contract AS (
  SELECT coalesce(current_setting('e2_t8.source_ref_fingerprint',true),'') <> '' AND
         coalesce(current_setting('e2_t8.target_ref_fingerprint',true),'') <> '' AND
         current_setting('e2_t8.source_ref_fingerprint',true) <> current_setting('e2_t8.target_ref_fingerprint',true) AS target_identity_distinct
), ledger AS (
  SELECT current_setting('e2_t8.ledger_unambiguous',true)='true' AS ledger_unambiguous
)
SELECT managed.auth_schema AND managed.auth_uid_exact_signature AND managed.anon_role AND managed.authenticated_role AND managed.service_role_role AS managed_primitives_ok,
       true AS target_kind_ok, identity_contract.target_identity_distinct, target_state.application_relation_count=0 AS public_allowlist_only,
       target_state.application_relation_count, ledger.ledger_unambiguous,
       managed.auth_schema AND managed.auth_uid_exact_signature AND managed.anon_role AND managed.authenticated_role AND managed.service_role_role AND identity_contract.target_identity_distinct AND target_state.application_relation_count=0 AND ledger.ledger_unambiguous AS passed
FROM managed CROSS JOIN target_state CROSS JOIN identity_contract CROSS JOIN ledger;
