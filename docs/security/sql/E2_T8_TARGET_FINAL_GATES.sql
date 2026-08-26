-- Operator settings are generated from the accepted source inventory manifest and checksum-bound by the evidence converter.
WITH supplied AS (
  SELECT current_setting('e2_t8.source_inventory_sha256',true) AS source_inventory_sha256,
         current_setting('e2_t8.application_tables_json',true)::jsonb AS application_tables
), approved_application_tables AS (
  SELECT value #>> '{}' AS table_name FROM supplied,jsonb_array_elements(supplied.application_tables)
), catalog_application_tables AS (
  SELECT c.relname AS table_name FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
  LEFT JOIN pg_depend e ON e.classid='pg_class'::regclass AND e.objid=c.oid AND e.deptype='e'
  WHERE n.nspname='public' AND c.relkind IN ('r','p') AND e.objid IS NULL
), exact_counts AS (
  SELECT table_name,(xpath('/row/count/text()',query_to_xml(format('SELECT count(*) AS count FROM public.%I',table_name),false,true,'')))[1]::text::bigint AS row_count
  FROM approved_application_tables
), completeness AS (
  SELECT NOT EXISTS((SELECT table_name FROM approved_application_tables EXCEPT SELECT table_name FROM catalog_application_tables)
                    UNION ALL (SELECT table_name FROM catalog_application_tables EXCEPT SELECT table_name FROM approved_application_tables)) AS allowlist_complete
), managed AS (
  SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname='auth') AND EXISTS(SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='auth' AND p.proname='uid' AND pg_get_function_identity_arguments(p.oid)='') AND (SELECT count(*)=3 FROM pg_roles WHERE rolname IN ('anon','authenticated','service_role')) AS managed_primitives_ok
)
SELECT managed.managed_primitives_ok,supplied.source_inventory_sha256,
       coalesce(jsonb_agg(jsonb_build_object('table_name',exact_counts.table_name,'row_count',exact_counts.row_count) ORDER BY exact_counts.table_name),'[]'::jsonb) AS application_table_counts,
       completeness.allowlist_complete
FROM managed CROSS JOIN supplied CROSS JOIN completeness LEFT JOIN exact_counts ON true
GROUP BY managed.managed_primitives_ok,supplied.source_inventory_sha256,completeness.allowlist_complete;
