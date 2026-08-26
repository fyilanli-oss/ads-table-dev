-- E2-T8 read-only metadata inventory. It never reads application rows or managed-schema rows.
WITH inventory AS (
  SELECT 'relation:' || c.relname AS object_key, 'relation' AS object_class,
         md5(concat_ws('|', c.relkind, c.relrowsecurity, c.relforcerowsecurity,
           CASE WHEN e.objid IS NULL THEN 'application_owned' ELSE 'extension_owned' END,
           CASE WHEN c.relowner = ANY (ARRAY(SELECT oid FROM pg_roles WHERE rolname IN ('postgres','supabase_admin'))) THEN 'managed_role' ELSE 'application_role' END)) AS fingerprint
  FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
  LEFT JOIN pg_depend e ON e.classid='pg_class'::regclass AND e.objid=c.oid AND e.deptype='e'
  WHERE n.nspname='public' AND c.relkind IN ('r','p','v','m','S')
  UNION ALL
  SELECT 'column:' || c.relname || '.' || a.attname, 'column', md5(concat_ws('|',a.atttypid::regtype::text,a.attnotnull,coalesce(pg_get_expr(d.adbin,d.adrelid),'')))
  FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid JOIN pg_namespace n ON n.oid=c.relnamespace LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum
  WHERE n.nspname='public' AND a.attnum>0 AND NOT a.attisdropped
  UNION ALL
  SELECT 'constraint:' || c.relname || '.' || x.conname, 'constraint', md5(concat_ws('|',x.contype,x.convalidated,pg_get_constraintdef(x.oid,false)))
  FROM pg_constraint x JOIN pg_class c ON c.oid=x.conrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public'
  UNION ALL
  SELECT 'index:' || i.relname, 'index', md5(concat_ws('|',x.indisunique,x.indisprimary,x.indisvalid,x.indisready,pg_get_indexdef(x.indexrelid)))
  FROM pg_index x JOIN pg_class i ON i.oid=x.indexrelid JOIN pg_class t ON t.oid=x.indrelid JOIN pg_namespace n ON n.oid=t.relnamespace WHERE n.nspname='public'
  UNION ALL
  SELECT 'function:' || p.proname || '(' || pg_get_function_identity_arguments(p.oid) || ')', 'function',
         md5(concat_ws('|',p.prosecdef,coalesce(array_to_string(p.proconfig,','),''),pg_get_functiondef(p.oid)))
  FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='public'
  UNION ALL
  SELECT 'trigger:' || c.relname || '.' || t.tgname, 'trigger', md5(pg_get_triggerdef(t.oid,false))
  FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND NOT t.tgisinternal
  UNION ALL
  SELECT 'policy:' || schemaname || '.' || tablename || '.' || policyname, 'policy', md5(concat_ws('|',permissive,roles,cmd,qual,with_check))
  FROM pg_policies WHERE schemaname='public'
  UNION ALL
  SELECT 'grant:' || table_name || '.' || grantee || '.' || privilege_type, 'grant', md5(concat_ws('|',is_grantable,with_hierarchy))
  FROM information_schema.role_table_grants WHERE table_schema='public'
)
SELECT object_key, object_class, fingerprint FROM inventory ORDER BY object_key, object_class;
