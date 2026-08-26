'use strict';
const crypto=require('node:crypto');
const {SENSITIVE}=require('./e2-t8-restore-contract');
const fp=v=>crypto.createHash('sha256').update(v,'utf8').digest('hex');

// Stateful lexer: semicolons and deny words inside comments, quoted identifiers,
// strings, or dollar-quoted function bodies never become top-level SQL tokens.
function lex(sql){
  const statements=[];let raw='',code='',i=0,state='code',tag='';
  const push=()=>{if(code.trim())statements.push({raw:raw.trim(),code:code.trim()});raw='';code='';};
  while(i<sql.length){const c=sql[i],n=sql[i+1];
    if(state==='line'){raw+=c;if(c==='\n'){state='code';code+=' ';}i++;continue;}
    if(state==='block'){raw+=c;if(c==='*'&&n==='/'){raw+=n;i+=2;state='code';code+=' ';}else i++;continue;}
    if(state==='single'){raw+=c;code+=' ';if(c==="'"&&n==="'"){raw+=n;code+=' ';i+=2;}else if(c==="'"){state='code';i++;}else i++;continue;}
    if(state==='double'){raw+=c;code+=c;if(c==='"'&&n==='"'){raw+=n;code+=n;i+=2;}else if(c==='"'){state='code';i++;}else i++;continue;}
    if(state==='dollar'){if(sql.startsWith(tag,i)){raw+=tag;code+=' '.repeat(tag.length);i+=tag.length;state='code';}else{raw+=c;code+=' ';i++;}continue;}
    if(c==='-'&&n==='-'){raw+=c+n;i+=2;state='line';continue;} if(c==='/'&&n==='*'){raw+=c+n;i+=2;state='block';continue;}
    if(c==="'"){raw+=c;code+=' ';i++;state='single';continue;} if(c==='"'){raw+=c;code+=c;i++;state='double';continue;}
    if(c==='$'){const m=sql.slice(i).match(/^\$[A-Za-z_][A-Za-z0-9_]*\$|^\$\$/);if(m){tag=m[0];raw+=tag;code+=' '.repeat(tag.length);i+=tag.length;state='dollar';continue;}}
    raw+=c;code+=c;i++;if(c===';')push();
  }
  if(state!=='code'&&state!=='line')throw new Error('UNTERMINATED_SQL_TOKEN');push();return statements;
}
function normalize(sql){return lex(sql).map(s=>s.raw.replace(/^\s*--.*$/gm,'').replace(/\s+/g,' ').trim()).filter(Boolean).join('\n');}
function objectKeys(code){const keys=[];for(const m of code.matchAll(/\bCREATE\s+(?:OR\s+REPLACE\s+)?(TABLE|FUNCTION|INDEX|TRIGGER|POLICY|SEQUENCE|VIEW)\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:"?public"?\.)?"?([A-Za-z_][\w$]*)"?/gi))keys.push(`${({table:'relation',sequence:'sequence',view:'view',function:'function',index:'index',trigger:'trigger',policy:'policy'})[m[1].toLowerCase()]}:${m[2].toLowerCase()}`);return keys;}
function validateCapturedSchema({capturedSql,approvedSourceInventory,restoreScope,expectedGrantContract,captureManifest}){
  const errors=[];if(typeof capturedSql!=='string'||!capturedSql.trim())errors.push('EMPTY_ARTIFACT');let statements=[];try{statements=lex(capturedSql||'');}catch{errors.push('TOKENIZER_ERROR');}
  if(SENSITIVE.test(capturedSql||''))errors.push('SENSITIVE_PATTERN'); const seen=new Set();
  const approved=new Set((approvedSourceInventory||[]).map(x=>x.object_key)); const excluded=new Set(restoreScope?.excluded_schemas||[]); const grantees=new Set(expectedGrantContract?.grantees||[]); const privileges=new Set(expectedGrantContract?.privileges||[]);
  for(const {code} of statements){
    if(/^\s*\\/m.test(code))errors.push('PSQL_METACOMMAND');
    if(/\b(?:COPY\b[\s\S]*\bFROM|INSERT\s+INTO|UPDATE\s+|DELETE\s+FROM|MERGE\s+INTO|TRUNCATE\b)/i.test(code))errors.push('ROW_DATA_STATEMENT');
    if(/\b(?:CREATE|ALTER|DROP)\s+ROLE\b|\b(?:CREATE|ALTER)\s+DATABASE\b/i.test(code))errors.push('ROLE_OR_DATABASE_DDL');
    if(/\bOWNER\s+TO\b|\bSET\s+SESSION\s+AUTHORIZATION\b/i.test(code))errors.push('OWNER_RESTORE');
    for(const schema of excluded)if(new RegExp(`(?:^|[^A-Za-z0-9_])"?${schema}"?\\s*\\.`,'i').test(code))errors.push('MANAGED_SCHEMA_DDL');
    for(const m of code.matchAll(/\b(?:CREATE|ALTER|DROP)\s+(?:TABLE|FUNCTION|INDEX|TRIGGER|POLICY|SEQUENCE|VIEW)\s+(?:IF\s+(?:NOT\s+)?EXISTS\s+)?"?([A-Za-z_][\w$]*)"?\s*\./gi))if(m[1].toLowerCase()!=='public')errors.push('UNKNOWN_SCHEMA');
    if(/\bCREATE\s+EXTENSION\b/i.test(code))errors.push('UNSUPPORTED_EXTENSION');
    for(const key of objectKeys(code)){seen.add(key);if(!approved.has(key))errors.push('UNKNOWN_OBJECT');}
    for(const m of code.matchAll(/\b(?:ALTER|DROP)\s+TABLE\s+(?:IF\s+EXISTS\s+)?(?:"?public"?\.)?"?([A-Za-z_][\w$]*)"?/gi))if(!approved.has(`relation:${m[1].toLowerCase()}`))errors.push('UNKNOWN_OBJECT');
    for(const m of code.matchAll(/\bON\s+(?:TABLE\s+)?(?:"?public"?\.)"?([A-Za-z_][\w$]*)"?/gi))if(!approved.has(`relation:${m[1].toLowerCase()}`))errors.push('UNKNOWN_OBJECT');
    const g=code.match(/^\s*(GRANT|REVOKE)\s+([A-Z, ]+)\s+ON\s+TABLE\s+(?:"?public"?\.)?"?([\w$]+)"?\s+(?:TO|FROM)\s+"?([\w$]+)"?(\s+WITH\s+GRANT\s+OPTION)?\s*;?\s*$/i);
    if(/^\s*(?:GRANT|REVOKE)\b/i.test(code)){if(!g)errors.push('UNKNOWN_GRANT');else{const [,op,list,obj,grantee,opt]=g;const publicEvidenced=grantee!=='PUBLIC'||(approvedSourceInventory||[]).some(x=>x.object_key.startsWith(`grant:${obj}.PUBLIC.`));if(op.toUpperCase()!=='GRANT'||opt||!grantees.has(grantee)||!publicEvidenced||!approved.has(`relation:${obj.toLowerCase()}`)||list.split(',').some(x=>!privileges.has(x.trim().toUpperCase())))errors.push('UNKNOWN_GRANT');}}
    if(/\bhistorical_sql_(?:available|claim)\b|\bsupabase_migrations\s*\.\s*schema_migrations\b/i.test(code))errors.push('HISTORICAL_LEDGER_OR_CLAIM');
  }
  for(const key of approved)if(/^(?:relation|function|index|trigger|policy|sequence|view):/.test(key)&&!seen.has(key))errors.push('SOURCE_OBJECT_MISSING');
  if(captureManifest?.row_data_included!==false||captureManifest?.managed_schema_ddl_included!==false||captureManifest?.owner_restore_included!==false)errors.push('MANIFEST_RISK');
  const normalized=normalize(capturedSql||'');return {status:errors.length?'ARTIFACT_CONTRACT_FAIL':'ARTIFACT_CONTRACT_PASS',errors:[...new Set(errors)],checksum:fp(normalized),restoreSafeDecision:false};
}
module.exports={lex,normalize,validateCapturedSchema};
if(require.main===module){process.stderr.write('Library validator: provide structured inputs through the module API.\n');process.exitCode=2;}
