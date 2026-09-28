-- R6-D5-K2: make the canonical Dataset V2 workspace key inferable by
-- PostgREST's ON CONFLICT clause.
--
-- R3-C1 already made workspace_id NOT NULL. The former partial predicate is
-- therefore redundant, and PostgreSQL cannot infer that partial index when
-- PostgREST sends the six conflict columns without an index predicate.
-- This migration changes no Dataset V2 rows.

drop index if exists public.performance_dataset_rows_v2_workspace_canonical_uidx;

create unique index performance_dataset_rows_v2_workspace_canonical_uidx
  on public.performance_dataset_rows_v2
  (workspace_id, platform, platform_account_id, business_date, traffic_type, entity_key);
