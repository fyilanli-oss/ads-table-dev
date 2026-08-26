-- E2-T8 normalized public application metadata only; application rows are never read.
-- Application allowlists are reviewable repository provenance: current migrations plus E2 metadata/security contracts.
WITH application_relations(relname) AS (
  VALUES ('dashboard_snapshots'),('fx_rates_daily'),('oauth_transactions'),('performance_dataset_rows'),('performance_dataset_rows_v2'),('platform_account_ownerships'),('platform_ad_accounts'),('platform_connection_tokens'),('platform_connections'),('snapshot_jobs'),('snapshot_schedules'),('users')
), managed_relations(relname) AS (
  VALUES ('spatial_ref_sys')
), application_functions(proname) AS (
  VALUES ('consume_oauth_transaction'),('cleanup_expired_oauth_transactions')
), managed_functions(proname) AS (
  SELECT NULL::name WHERE false
), relation_ownership AS (
  SELECT c.oid,CASE WHEN e.objid IS NOT NULL THEN 'managed_extension_owned' WHEN a.relname IS NOT NULL THEN 'application_owned' WHEN m.relname IS NOT NULL THEN 'excluded_managed' ELSE 'unclassified' END AS ownership_class
  FROM pg_class c LEFT JOIN pg_depend e ON e.classid='pg_class'::regclass AND e.objid=c.oid AND e.deptype='e'
  LEFT JOIN application_relations a ON a.relname=c.relname LEFT JOIN managed_relations m ON m.relname=c.relname
), normalized_inventory AS (
  SELECT CASE WHEN c.relkind='S' THEN 'sequence:' WHEN c.relkind IN ('v','m') THEN 'view:' ELSE 'relation:' END||c.relname AS object_key,CASE WHEN c.relkind='S' THEN 'sequence' WHEN c.relkind IN ('v','m') THEN 'view' ELSE 'relation' END AS object_class,o.ownership_class,concat_ws('|',c.relkind,c.relrowsecurity,c.relforcerowsecurity) AS normalized
  FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace JOIN relation_ownership o ON o.oid=c.oid WHERE n.nspname='public' AND c.relkind IN ('r','p','v','m','S')
  UNION ALL SELECT 'column:'||c.relname||'.'||a.attname,'column',o.ownership_class,concat_ws('|',a.atttypid::regtype::text,a.attnotnull,coalesce(pg_get_expr(d.adbin,d.adrelid),'')) FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid JOIN pg_namespace n ON n.oid=c.relnamespace JOIN relation_ownership o ON o.oid=c.oid LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum WHERE n.nspname='public' AND a.attnum>0 AND NOT a.attisdropped
  UNION ALL SELECT 'constraint:'||c.relname||'.'||x.conname,'constraint',o.ownership_class,concat_ws('|',x.contype,x.convalidated,pg_get_constraintdef(x.oid,false)) FROM pg_constraint x JOIN pg_class c ON c.oid=x.conrelid JOIN relation_ownership o ON o.oid=c.oid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public'
  UNION ALL SELECT 'index:'||i.relname,'index',o.ownership_class,concat_ws('|',x.indisunique,x.indisprimary,x.indisvalid,x.indisready,pg_get_indexdef(x.indexrelid)) FROM pg_index x JOIN pg_class i ON i.oid=x.indexrelid JOIN pg_class t ON t.oid=x.indrelid JOIN relation_ownership o ON o.oid=t.oid JOIN pg_namespace n ON n.oid=t.relnamespace WHERE n.nspname='public'
  UNION ALL SELECT 'function:'||p.proname||'('||pg_get_function_identity_arguments(p.oid)||')','function',CASE WHEN e.objid IS NOT NULL THEN 'managed_extension_owned' WHEN af.proname IS NOT NULL THEN 'application_owned' WHEN mf.proname IS NOT NULL THEN 'excluded_managed' ELSE 'unclassified' END,concat_ws('|',p.prosecdef,coalesce(array_to_string(p.proconfig,','),''),pg_get_functiondef(p.oid)) FROM pg_proc p LEFT JOIN pg_depend e ON e.classid='pg_proc'::regclass AND e.objid=p.oid AND e.deptype='e' LEFT JOIN application_functions af ON af.proname=p.proname LEFT JOIN managed_functions mf ON mf.proname=p.proname JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='public'
  UNION ALL SELECT 'trigger:'||c.relname||'.'||t.tgname,'trigger',o.ownership_class,pg_get_triggerdef(t.oid,false) FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN relation_ownership o ON o.oid=c.oid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND NOT t.tgisinternal
  UNION ALL SELECT 'policy:'||schemaname||'.'||tablename||'.'||policyname,'policy',o.ownership_class,concat_ws('|',permissive,roles,cmd,qual,with_check) FROM pg_policies p JOIN pg_class c ON c.relname=p.tablename JOIN pg_namespace n ON n.oid=c.relnamespace AND n.nspname=p.schemaname JOIN relation_ownership o ON o.oid=c.oid WHERE schemaname='public'
  UNION ALL SELECT 'grant:'||table_name||'.'||grantee||'.'||privilege_type,'grant',o.ownership_class,concat_ws('|',is_grantable,with_hierarchy) FROM information_schema.role_table_grants g JOIN pg_class c ON c.relname=g.table_name JOIN pg_namespace n ON n.oid=c.relnamespace AND n.nspname=g.table_schema JOIN relation_ownership o ON o.oid=c.oid WHERE table_schema='public'
)
SELECT object_key,object_class,ownership_class,encode(digest(convert_to(normalized,'UTF8'),'sha256'),'hex') AS fingerprint FROM normalized_inventory ORDER BY object_key,object_class;
