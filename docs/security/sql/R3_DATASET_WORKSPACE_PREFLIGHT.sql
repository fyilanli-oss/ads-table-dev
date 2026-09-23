-- R3-A read-only production preflight. Expected gate: PASS.

with state as (
  select
    to_regclass('public.workspaces') is not null as workspaces_exists,
    to_regclass('public.performance_dataset_rows_v2') is not null as dataset_v2_exists,
    exists (
      select 1 from information_schema.columns
      where table_schema = 'public'
        and table_name = 'performance_dataset_rows_v2'
        and column_name = 'workspace_id'
    ) as workspace_column_exists,
    (select count(*) from public.performance_dataset_rows_v2) as dataset_v2_rows,
    (select count(*) from public.workspaces) as workspace_rows,
    to_regclass('public.backfill_checkpoints') is not null as backfill_checkpoints_exists
)
select case
  when not workspaces_exists then 'BLOCK_WORKSPACES_MISSING'
  when not dataset_v2_exists then 'BLOCK_DATASET_V2_MISSING'
  when workspace_column_exists then 'BLOCK_ALREADY_APPLIED'
  when dataset_v2_rows <> 0 then 'BLOCK_NONEMPTY_DATASET_WITHOUT_EXPLICIT_BINDING'
  when backfill_checkpoints_exists then 'BLOCK_UNEXPECTED_BACKFILL_TABLE'
  else 'PASS'
end as r3_preflight_gate,
state.*
from state;
