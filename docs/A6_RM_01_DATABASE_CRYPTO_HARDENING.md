# A6-RM-01 — Database Least-Privilege ve Crypto Fail-Closed

**Karar tarihi:** 4 Ekim 2026  
**Durum:** Database live accepted / crypto activation pending  
**Canlı mutation:** Least-privilege migration uygulandı ve postcheck PASS

## İş çıktısı

Gereksiz public database yetkilerini kaldırırken bugün kullanılan legacy trial geçişini korumak; production provider-token runtime'ının encryption kapalı veya legacy read açık durumda sessizce başlamasını engellemek.

## Canlı bulgu özeti

- `expire_trials()` ve `handle_new_user()` postgres-owned `SECURITY DEFINER` fonksiyonlarıdır ve PUBLIC/anon/authenticated tarafından çalıştırılabilir.
- `enforce_platform_account_limit_guard()` mutable search path ve dış EXECUTE yüzeyi taşır.
- postgres ve Supabase'in iç `supabase_admin` rolü için public schema default ACL envanteri geniş grant'ler gösterir. Proje migration sınırı resmî dokümana uygun olarak yalnız postgres-owned future nesnelerdir; `supabase_admin` provider-kontrollü kalır.
- On dört legacy tabloda anon ve authenticated için TRUNCATE, TRIGGER ve REFERENCES yetkileri vardır.
- Browser doğrudan tablo/RPC tüketimi bulunmadı; oturum için Supabase Auth kullanılır, veri yolları server-side service_role ile çalışır.
- Production crypto env adları vardır; exact boolean/keyring posture ve fail-closed startup henüz kanıtlanmamıştır.

## Trial kararı

RM-01 `expire_trials()` fonksiyonunu silmez ve service_role çağrısını korur. Anon/authenticated RPC erişimi kaldırılır. `handle_new_user()` database trigger davranışı korunur.

Bu yalnız geçici legacy entitlement sürekliliğidir. Shopify merchant'ın gerçek 14 günlük trial ve ücretlendirme otoritesi E10-T7'de Shopify `appSubscriptionCreate(trialDays: 14)` olacaktır.

## Uygulama sırası

1. RM-01A: Read-only privilege ve crypto posture preflight.
2. RM-01B: Exact least-privilege migration; önce repository/CI, sonra ayrı canlı onay.
3. RM-01C: Non-secret production preflight geçtikten sonra startup guard activation.
4. RM-01D: Negative role tests, trial regression, Security Advisor ve production smoke.

## Güvenli migration sınırı

- Mevcut browser SELECT/CRUD grant'leri bu pakette körlemesine kaldırılmaz.
- Yalnız exact 14-table inventory üzerindeki TRUNCATE/TRIGGER/REFERENCES kaldırılır.
- Üç internal/trigger fonksiyonunun PUBLIC/anon/authenticated EXECUTE yetkisi kaldırılır.
- `expire_trials()` için service_role EXECUTE korunur.
- Postgres-owned future nesnelerin default privileges yapısı explicit least-privilege olur. Supabase'in iç `supabase_admin` rolüne SQL migration ile müdahale edilmez; Data API ayarı ve provider rollout'u ayrı gözlenir.
- Rollback blanket grant vermez; yalnız kanıtlanmış exact tüketici yetkisi ayrı review ile geri eklenebilir.

## Crypto kapısı

Production posture şu dört koşulu birlikte sağlamalıdır:

- encryption enabled,
- legacy reads disabled,
- active key keyring içinde geçerli,
- server-side Supabase credentials mevcut.

Diagnostic yalnız boolean, env adı ve güvenli reason code döndürür. Secret/key/token değeri, URL veya project ref döndürmez.

## Bağlayıcı contract

`contracts/a6-rm-01-database-crypto-hardening-v1.json`

## Canlı kabul sonucu — 4 Ekim 2026

- Migration history: `20261004113227 / a6_rm01_least_privilege_hardening`.
- Beş postcheck kapısının tamamı sıfır ihlal verdi: external function execute, legacy non-DML grants, mutable search path, postgres default privileges ve service-role trial continuity.
- İlk uygulama denemesi provider-internal `supabase_admin` rol sınırında transaction olarak reddedildi ve tamamen rollback oldu. PR #347 resmî Supabase modeline göre project-owned sınırı `postgres` olarak düzeltti; CI PASS sonrasında migration başarıyla uygulandı.
- Security Advisor'daki external SECURITY DEFINER ve mutable search-path bulguları kapandı.
- Kalan 10 `RLS enabled/no policy` INFO kaydı ile leaked-password protection WARN bu database mutation'ının kabulünü bozmaz; ayrı güvenlik kapsamlarında izlenir.
- Production provider-token crypto posture ve startup guard henüz aktive edilmedi; RM-01 bu runtime kapısı tamamlanana kadar bütünüyle `Done` değildir.

**Evidence:** `docs/security/evidence/A6_RM01_LIVE_ACCEPTANCE_2026-10-04.json`

## Runtime diagnostic hazırlığı

Mevcut OIDC-korumalı `/api/e10/activation-preflight` cevabına RM-01 için `provider_token_runtime` ve `rm01_crypto_ready` alanları eklenir. Çıktı yalnız boolean ve güvenli reason code taşır; secret değer veya uzunluk yayımlamaz. Bu adım startup assertion'ı bağlamaz ve production'a otomatik terfi etmez. Önce PR CI ve Vercel Preview build/route kabulü gerekir.

## Production crypto posture kabulü — 4 Ekim 2026

OIDC-korumalı production diagnostic, commit `a8e8e1c392589b658e30eacc9731c9bf6e535ab5` üzerinde RM-01 için `PASS` verdi:

- encryption flag mevcut, geçerli ve açık,
- legacy-read flag mevcut, geçerli ve kapalı,
- keyring geçerli,
- server-side Supabase credentials mevcut,
- `startup_allowed=true`, reason code boş,
- secret değer veya uzunluk yayımlanmadı.

GitHub workflow #4 genel E10 `ready` değerini de zorunlu tuttuğu için kırmızı sonuçlandı; RM-01 değil, mevcut production OAuth feature flag'inin artık kapalı olmaması bu genel E10 kapısını düşürdü. Bu ayrım fail-closed olarak korunur; RM-01 crypto sonucu PASS sayılırken E10 readiness sonucu değiştirilmez.

Startup guard bu aşamada bağlanmadı. Bir sonraki adım ayrı açık onayla `assertProductionProviderTokenPosture` çağrısını composition root'a bağlamak, CI/Preview sonrası production deployment ve smoke yapmaktır.

**Evidence:** `docs/security/evidence/A6_RM01_CRYPTO_POSTURE_LIVE_2026-10-04.json`
