# A1 — Tenant, Credential ve Security Authority Denetimi

**Tarih:** 2026-10-03  
**Kapsam:** E1–E3 ve R0–R5  
**Durum:** `IN_PROGRESS / REMEDIATION_BLOCKED`  
**Yöntem:** Repository sözleşmeleri + güncel kod + canlı Supabase şema/rol/politika/advisor kanıtı + salt-okunur üretim yapılandırması kontrolü.

> Bu belge düzeltme paketi değildir. A1 tamamlanmadan ve bulgular ayrı paketlere ayrılmadan kod, migration, provider mutation veya deployment yapılmaz.

## Analist özeti

Canonical Shopify embedded yolu açısından tenant authority şu an doğru kurulmuş görünüyor: istemciden gelen `workspace_id`, `user_id` ve `shop_id` yetki kaynağı kabul edilmiyor; doğrulanmış Shopify shop kimliği aktif installation kaydına, oradan server-resolved workspace'e bağlanıyor. Canonical provider bağlantıları tekil workspace/provider anahtarıyla tutuluyor ve tokenlar şifreli.

Buna karşılık canlı Supabase `public` şemasında eski ve genel güvenlik yüzeyi temiz değil. En kritik kanıt, `expire_trials()` SECURITY DEFINER fonksiyonunun `anon` ve `authenticated` rollerine çalıştırılabilir olmasıdır. Ayrıca yeni public nesneler için varsayılan yetkiler gereğinden geniş; gelecekte eksik bir revoke/RLS adımı yeni bir açık doğurabilir. Bunlar canonical tenant modelinin doğru olmasını geçersiz kılmaz, ancak mağaza güvenliği açısından ayrı kapatma paketleri gerektirir.

## Doğrulanmış pozitif kontroller

- `src/shopify/tenant-model.js`: caller-supplied shop/workspace claim'leri reddediliyor.
- `src/shopify/embedded-auth.js`: Shopify HMAC/JWT; `HS256`, `aud`, `exp`, `nbf`, `dest`, `iss` doğrulanıyor.
- `src/shopify/runtime.js`: workspace server tarafında çözülüyor; canonical connection store kullanılıyor.
- `funnel-core/workspace-dataset-runtime.js`: caller `workspace_id`, `user_id`, `shop_id` reddediliyor; cross-workspace yazım engelleniyor.
- `workspace_provider_connections`: PK `(workspace_id, provider)`; RLS + FORCE RLS; yalnız `service_role` erişimi.
- `workspaces`, `workspace_settings`, `legacy_user_workspace_bindings`, `shopify_installations`, `shopify_workspace_provider_connections`: RLS açık; canonical server-only yüzeylerde browser policy yok.
- `performance_dataset_rows_v2`: RLS açık; incelenen roller içinde yalnız `service_role` grant'i; canonical workspace unique key mevcut.
- `shopify_installations`: `shop_id`, `shop_domain`, `workspace_id` tekil; managed reinstall mevcut `workspace_id` değerini koruyor.
- OAuth transaction ve managed install fonksiyonları service-role-only ve sabit `search_path` ile çalışıyor.
- Legacy provider write/token write ve standalone OAuth insert yolları canlı trigger'larla dondurulmuş.
- Canlı canonical provider tokenları plaintext değil; encrypted envelope olarak tutuluyor.

## Açık audit bulguları

### AF-A1-001 — Public şema default privilege yüzeyi gereğinden geniş

**Öncelik:** P1  
**Kanıt:** Canlı default privileges, `postgres` ve `supabase_admin` tarafından oluşturulan yeni table/function/sequence nesnelerine `anon`, `authenticated` ve `service_role` için geniş otomatik haklar veriyor.  
**Risk:** Yeni bir migration açık revoke/RLS adımını unutursa nesne Data API yüzeyine istemeden açılabilir. Bu sistemik bir “gelecekte sessiz açık üretme” mekanizmasıdır.  
**Karar:** Düzeltme A1 içinde yapılmayacak; ayrı güvenlik paketi açılacak.  
**Resmî dayanak:** https://supabase.com/docs/guides/api/securing-your-api

### AF-A1-002 — `expire_trials()` anon/authenticated tarafından çalıştırılabilir

**Öncelik:** P1  
**Kanıt:** Canlı fonksiyon `SECURITY DEFINER`; `PUBLIC`, `anon`, `authenticated`, `service_role` EXECUTE grant'ine sahip. Supabase security advisor bunu hem anon hem authenticated için uyarı olarak raporluyor. Fonksiyon süresi dolmuş tüm trial subscription kayıtlarını `expired` durumuna güncelliyor.  
**Risk:** Browser/API rolü tenant sınırı olmadan çapraz-tenant state mutation başlatabilir. İş kuralı sonucu normal olsa dahi authority yanlıştır.  
**Karar:** Fonksiyon çağrılmadı; canlı veri mutasyonu yapılmadı. Ayrı P1 remediation paketi gerekir.  
**Resmî dayanak:** https://supabase.com/docs/guides/database/database-linter?lint=0028_anon_security_definer_function_executable

### AF-A1-003 — `handle_new_user()` gereksiz public executable yüzeyinde

**Öncelik:** P1  
**Kanıt:** SECURITY DEFINER trigger function; sabit `search_path` var fakat `PUBLIC`, `anon`, `authenticated`, `service_role` EXECUTE grant'ine sahip. Advisor anon/authenticated uyarısı veriyor.  
**Risk:** Trigger fonksiyonunun browser rollerine açık olması gereksiz bir ayrıcalık yüzeyidir. `raw_user_meta_data.name` yalnız görüntü verisi olarak kullanılıyor; authorization kaynağı olarak kullanılmıyor.  
**Karar:** Exploit amacıyla RPC çağrısı yapılmadı. Ayrı remediation paketinde EXECUTE yüzeyi daraltılmalı ve auth trigger akışı regresyon testiyle doğrulanmalı.  
**Resmî dayanak:** https://supabase.com/docs/guides/database/database-linter?lint=0029_authenticated_security_definer_function_executable

### AF-A1-004 — Mutable search_path ve public trigger-function grant drift

**Öncelik:** P2  
**Kanıt:** `enforce_platform_account_limit_guard()` security-invoker trigger function için mutable `search_path` advisor uyarısı mevcut; public/anon/authenticated EXECUTE grant'i bulunuyor.  
**Risk:** Şu an doğrudan kanıtlanmış cross-tenant mutation yok; yine de search-path hijack ve gereksiz RPC yüzeyi açısından hardening borcu.  
**Resmî dayanak:** https://supabase.com/docs/guides/database/database-linter?lint=0011_function_search_path_mutable

### AF-A1-005 — Legacy public tablolarda geniş non-DML grant drift

**Öncelik:** P1  
**Kanıt:** 14 legacy public tabloda `anon`/`authenticated` için TRUNCATE/TRIGGER/REFERENCES dahil geniş grants görüldü. Örnekler: `dashboard_snapshots`, `fx_rates`, `performance_dataset_rows`, `platform_connections`, `snapshot_jobs`, `subscriptions`, `users`.  
**Risk:** RLS satır bazlı DML'i sınırlar; TRUNCATE gibi table-level ayrıcalıkların güvenlik modeli farklıdır. PostgREST standart table API doğrudan TRUNCATE sunmasa da grant seti least-privilege değildir ve başka SQL/RPC yüzeyleriyle birleştiğinde risk üretir.  
**Karar:** “Şu an sömürülebilir” diye varsayılmadı; ayrı privilege-matrix paketiyle her tablo/rol için kapı doğrulanacak.

### AF-A1-006 — Üretim token-encryption konfigürasyonu canlı ortamda doğrulanamadı

**Öncelik:** P1 — doğrulama kapısı  
**Kod kanıtı:** `server.js` içinde `PROVIDER_TOKEN_ENCRYPTION_ENABLED` varsayılanı false; `security/production-config.js` bu flag'i production start için zorunlu kılmıyor. Canonical embedded runtime kendi vault/keyring kapısına sahip ve canlı DB'deki canonical tokenlar şifreli.  
**Canlı ortam kanıtı:** Vercel connector proje listesini doğruladı ancak environment-variable metadata aracı sunmuyor. CLI tarafında mevcut oturum bulunmadığı için login akışı başlatılmadan durduruldu; secret değerleri okunmadı.  
**Risk:** Mevcut canlı tokenların şifreli olması olumlu fakat production fail-closed garantisi env metadata görülmeden kanıtlanmış değildir.  
**Karar:** Değişken adları ve target scope'ları yetkili Vercel metadata kanalıyla doğrulanana kadar bu madde PASS olamaz.

Kontrol edilecek adlar:
- `PROVIDER_TOKEN_ENCRYPTION_ENABLED`
- `PROVIDER_TOKEN_ACTIVE_KEY_ID`
- `PROVIDER_TOKEN_ENCRYPTION_KEYS`
- `SUPABASE_URL`
- `SUPABASE_SERVICE_ROLE_KEY`
- `SHOPIFY_EMBEDDED_PROVIDER_OAUTH_ENABLED`

Gizli değerler audit çıktısına yazılmaz.

### AF-A1-007 — Plan/contract ile canlı cardinality drift

**Öncelik:** P2  
**Kanıt:** Eski özetler Dataset V2 için 0 ve legacy token için 7 kayıt belirtirken canlı aggregate Dataset V2=5 ve legacy token=6 gösteriyor.  
**Risk:** İşlem ilerlemesi normal olabilir; fakat stale sayılar audit ve go/no-go kararlarını yanıltabilir.  
**Karar:** Snapshot tarihleri ve authoritative count kaynağı açıkça ayrılmadan contradiction sayılmayacak.

### AF-A1-008 — Leaked password protection kapalı

**Öncelik:** P2  
**Kanıt:** Canlı Supabase security advisor “leaked password protection disabled” uyarısı veriyor.  
**Risk:** Bilinen sızdırılmış parolaların yeni kullanıcı parolası olarak kabul edilmesi ihtimali. Tenant authority açığı değil, hesap güvenliği hardening borcu.  
**Resmî dayanak:** https://supabase.com/docs/guides/auth/password-security#password-strength-and-leaked-password-protection

## Canlı aggregate kanıt özeti

- workspaces: 1
- workspace_settings: 1
- canonical workspace_provider_connections: 3
- legacy_user_workspace_bindings: 1
- performance_dataset_rows_v2: 5; workspace sayısı: 1
- shopify_installations: 1
- shopify_workspace_provider_connections: 1 revoked historical row
- platform_connections: 8
- platform_connection_tokens: 6
- Canonical providerlar: Google Ads, Klaviyo, Meta — connected; encrypted access envelope mevcut.
- Legacy plaintext access token sayısı: 0
- Legacy plaintext refresh token sayısı: 0

Bu sayılar secret veya kişisel veri içermez.

## A1 geçiş kararı

**A1 şu anda PASS değildir.** Canonical tenant authority için ilk kanıtlar olumlu olsa da AF-A1-001, 002, 003, 005 ve 006 kapanmadan security authority güvence altına alınmış sayılamaz.

Bir sonraki audit adımı:

1. E1–E3 ve R0–R5 contract iddialarını bulgu matrisiyle tek tek eşleştirmek.
2. Browser/anon/authenticated/service_role privilege matrisini tamamlamak.
3. Vercel env ad/scope metadata doğrulamasını secret okumadan tamamlamak.
4. Bulguları remediation paketlerine ayırmak; ancak audit tamamlandıktan sonra uygulama sırasını kullanıcıya analist açıklamasıyla sunmak.
