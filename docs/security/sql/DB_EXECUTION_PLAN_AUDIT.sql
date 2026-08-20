BEGIN READ ONLY;
SET LOCAL statement_timeout = '30s';
SET LOCAL lock_timeout = '3s';

WITH relation_base AS (
  SELECT c.oid, c.relname AS name,
    CASE c.relkind WHEN 'r' THEN 'table' WHEN 'p' THEN 'partitioned_table' WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized_view' ELSE c.relkind::text END AS object_type,
    pg_get_userbyid(c.relowner) AS owner, c.relrowsecurity AS rls_enabled,
    c.relforcerowsecurity AS rls_forced, c.reltuples::bigint AS estimated_rows
  FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
  WHERE n.nspname = 'public' AND c.relkind IN ('r', 'p', 'v', 'm')
), relations AS (
  SELECT r.*,
    (SELECT count(*) FROM pg_catalog.pg_attribute x WHERE x.attrelid=r.oid AND x.attnum > 0 AND NOT x.attisdropped) AS column_count,
    (SELECT count(*) FROM pg_catalog.pg_constraint x WHERE x.conrelid=r.oid AND x.contype='p') AS pk_count,
    (SELECT count(*) FROM pg_catalog.pg_constraint x WHERE x.conrelid=r.oid AND x.contype='f') AS fk_count,
    (SELECT count(*) FROM pg_catalog.pg_constraint x WHERE x.conrelid=r.oid AND x.contype='u') AS unique_count,
    (SELECT count(*) FROM pg_catalog.pg_constraint x WHERE x.conrelid=r.oid AND x.contype='c') AS check_count,
    (SELECT count(*) FROM pg_catalog.pg_indexes x WHERE x.schemaname='public' AND x.tablename=r.name) AS index_count,
    (SELECT count(*) FROM pg_catalog.pg_policies x WHERE x.schemaname='public' AND x.tablename=r.name) AS policy_count,
    (SELECT count(*) FROM pg_catalog.pg_trigger x WHERE x.tgrelid=r.oid AND NOT x.tgisinternal) AS trigger_count,
    ARRAY(SELECT p FROM unnest(ARRAY['SELECT','INSERT','UPDATE','DELETE','TRUNCATE','REFERENCES','TRIGGER']) p WHERE pg_catalog.has_table_privilege('anon',r.oid,p) ORDER BY p) AS anon_privileges,
    ARRAY(SELECT p FROM unnest(ARRAY['SELECT','INSERT','UPDATE','DELETE','TRUNCATE','REFERENCES','TRIGGER']) p WHERE pg_catalog.has_table_privilege('authenticated',r.oid,p) ORDER BY p) AS authenticated_privileges,
    ARRAY(SELECT p FROM unnest(ARRAY['SELECT','INSERT','UPDATE','DELETE','TRUNCATE','REFERENCES','TRIGGER']) p WHERE pg_catalog.has_table_privilege('service_role',r.oid,p) ORDER BY p) AS service_role_privileges,
    ARRAY(SELECT p FROM unnest(ARRAY['SELECT','INSERT','UPDATE','DELETE','TRUNCATE','REFERENCES','TRIGGER']) p WHERE pg_catalog.has_table_privilege('codex_auditor',r.oid,p) ORDER BY p) AS auditor_privileges
  FROM relation_base r
), columns_json AS (
  SELECT c.relname AS table_name, jsonb_agg(jsonb_build_object('name',a.attname,'ordinal',a.attnum,'type',pg_catalog.format_type(a.atttypid,a.atttypmod),'nullable',NOT a.attnotnull,'default',pg_catalog.pg_get_expr(d.adbin,d.adrelid,false)) ORDER BY a.attnum) value
  FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_class c ON c.oid=a.attrelid JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace JOIN pg_catalog.pg_type t ON t.oid=a.atttypid LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum
  WHERE n.nspname='public' AND c.relkind IN ('r','p','v','m') AND a.attnum > 0 AND NOT a.attisdropped GROUP BY c.relname
), constraints_json AS (
  SELECT c.conrelid, jsonb_agg(jsonb_build_object('name',c.conname,'type',c.contype,'definition',pg_catalog.pg_get_constraintdef(c.oid,false),'localColumns',coalesce(lc.names,'{}'::text[]),'referencedSchema',rn.nspname,'referencedTable',rc.relname,'referencedColumns',coalesce(fc.names,'{}'::text[]),'validated',c.convalidated,'deferrable',c.condeferrable,'updateAction',CASE c.confupdtype WHEN 'a' THEN 'NO ACTION' WHEN 'r' THEN 'RESTRICT' WHEN 'c' THEN 'CASCADE' WHEN 'n' THEN 'SET NULL' WHEN 'd' THEN 'SET DEFAULT' END,'deleteAction',CASE c.confdeltype WHEN 'a' THEN 'NO ACTION' WHEN 'r' THEN 'RESTRICT' WHEN 'c' THEN 'CASCADE' WHEN 'n' THEN 'SET NULL' WHEN 'd' THEN 'SET DEFAULT' END) ORDER BY c.conname) value
  FROM pg_catalog.pg_constraint c LEFT JOIN pg_catalog.pg_class rc ON rc.oid=c.confrelid LEFT JOIN pg_catalog.pg_namespace rn ON rn.oid=rc.relnamespace
  LEFT JOIN LATERAL (SELECT array_agg(a.attname ORDER BY k.ordinality) names FROM unnest(c.conkey) WITH ORDINALITY k(attnum,ordinality) JOIN pg_catalog.pg_attribute a ON a.attrelid=c.conrelid AND a.attnum=k.attnum) lc ON true
  LEFT JOIN LATERAL (SELECT array_agg(a.attname ORDER BY k.ordinality) names FROM unnest(c.confkey) WITH ORDINALITY k(attnum,ordinality) JOIN pg_catalog.pg_attribute a ON a.attrelid=c.confrelid AND a.attnum=k.attnum) fc ON true
  GROUP BY c.conrelid
), indexes_json AS (
  SELECT i.indrelid, jsonb_agg(jsonb_build_object('name',ci.relname,'unique',i.indisunique,'primary',i.indisprimary,'definition',pg_catalog.pg_get_indexdef(i.indexrelid)) ORDER BY ci.relname) value
  FROM pg_catalog.pg_index i JOIN pg_catalog.pg_class ci ON ci.oid=i.indexrelid GROUP BY i.indrelid
), policies AS (
  SELECT format('%s.%s',tablename,policyname) identity, tablename AS "table", policyname AS name, roles, cmd AS command, permissive, qual AS using, with_check AS "withCheck"
  FROM pg_catalog.pg_policies WHERE schemaname='public'
), functions AS (
  SELECT pg_catalog.pg_get_function_identity_arguments(p.oid) args, p.proname, format('%s(%s)',p.proname,pg_catalog.pg_get_function_identity_arguments(p.oid)) identity,
    pg_get_userbyid(p.proowner) owner, p.prosecdef AS "securityDefiner", p.provolatile AS volatility,
    coalesce(array_to_string(p.proconfig,',') LIKE '%search_path=%',false) AS "searchPathConfigured",
    has_function_privilege('public',p.oid,'EXECUTE') AS "publicExecute", has_function_privilege('anon',p.oid,'EXECUTE') AS "anonExecute",
    has_function_privilege('authenticated',p.oid,'EXECUTE') AS "authenticatedExecute", has_function_privilege('service_role',p.oid,'EXECUTE') AS "serviceRoleExecute"
  FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='public'
), default_privileges AS (
  SELECT format('%s:%s:%s:%s',pg_get_userbyid(d.defaclrole),coalesce(n.nspname,'*'),d.defaclobjtype,a.grantee) identity,
    pg_get_userbyid(d.defaclrole) owner, coalesce(n.nspname,'*') schema, d.defaclobjtype AS "objectType", a.grantee, array_agg(DISTINCT a.privilege_type ORDER BY a.privilege_type) privileges
  FROM pg_catalog.pg_default_acl d LEFT JOIN pg_catalog.pg_namespace n ON n.oid=d.defaclnamespace,
    LATERAL aclexplode(coalesce(d.defaclacl,acldefault(d.defaclobjtype,d.defaclrole))) x,
    LATERAL (SELECT CASE WHEN x.grantee=0 THEN 'PUBLIC' ELSE pg_get_userbyid(x.grantee) END grantee, x.privilege_type) a
  GROUP BY d.defaclrole,n.nspname,d.defaclobjtype,a.grantee
), migrations AS (
  SELECT version::text version, name::text name FROM supabase_migrations.schema_migrations
)
SELECT jsonb_build_object(
  'migrations',coalesce((SELECT jsonb_agg(to_jsonb(m) ORDER BY version) FROM migrations m),'[]'::jsonb),
  'relations',coalesce((SELECT jsonb_agg(jsonb_build_object('name',r.name,'objectType',r.object_type,'owner',r.owner,'rlsEnabled',r.rls_enabled,'rlsForced',r.rls_forced,'estimatedRows',r.estimated_rows,'columnCount',r.column_count,'pkCount',r.pk_count,'fkCount',r.fk_count,'uniqueCount',r.unique_count,'checkCount',r.check_count,'indexCount',r.index_count,'policyCount',r.policy_count,'triggerCount',r.trigger_count,'anonPrivileges',r.anon_privileges,'authenticatedPrivileges',r.authenticated_privileges,'serviceRolePrivileges',r.service_role_privileges,'auditorPrivileges',r.auditor_privileges,'columns',coalesce(c.value,'[]'::jsonb),'constraints',coalesce(k.value,'[]'::jsonb),'indexes',coalesce(i.value,'[]'::jsonb)) ORDER BY r.name) FROM relations r LEFT JOIN columns_json c ON c.table_name=r.name LEFT JOIN constraints_json k ON k.conrelid=r.oid LEFT JOIN indexes_json i ON i.indrelid=r.oid),'[]'::jsonb),
  'policies',coalesce((SELECT jsonb_agg(to_jsonb(p) ORDER BY identity) FROM policies p),'[]'::jsonb),
  'functions',coalesce((SELECT jsonb_agg(to_jsonb(f)-'args'-'proname' ORDER BY identity) FROM functions f),'[]'::jsonb),
  'defaultPrivileges',coalesce((SELECT jsonb_agg(to_jsonb(d) ORDER BY identity) FROM default_privileges d),'[]'::jsonb)
) AS audit_json;

COMMIT;
