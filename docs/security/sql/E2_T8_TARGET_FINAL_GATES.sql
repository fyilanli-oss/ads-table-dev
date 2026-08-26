-- This exact aggregate list is generated only from the approved source-inventory relation allowlist.
WITH approved_application_tables(table_name) AS (
  VALUES ('dashboard_snapshots'),('fx_rates_daily'),('oauth_transactions'),('performance_dataset_rows'),('performance_dataset_rows_v2'),('platform_account_ownerships'),('platform_ad_accounts'),('platform_connection_tokens'),('platform_connections'),('snapshot_jobs'),('snapshot_schedules'),('users')
), exact_counts AS (
  SELECT table_name,(xpath('/row/count/text()',query_to_xml(format('SELECT count(*) AS count FROM public.%I',table_name),false,true,'')))[1]::text::bigint AS row_count
  FROM approved_application_tables
), managed AS (
  SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname='auth') AND EXISTS(SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='auth' AND p.proname='uid' AND pg_get_function_identity_arguments(p.oid)='') AND (SELECT count(*)=3 FROM pg_roles WHERE rolname IN ('anon','authenticated','service_role')) AS managed_primitives_ok
)
SELECT managed.managed_primitives_ok,coalesce(sum(exact_counts.row_count),0)::bigint AS application_row_count_exact,count(*)=(SELECT count(*) FROM approved_application_tables) AS allowlist_complete
FROM managed CROSS JOIN exact_counts GROUP BY managed.managed_primitives_ok;
