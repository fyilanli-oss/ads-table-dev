# A6-RM-01 — Database Least-Privilege ve Crypto Fail-Closed

**Karar tarihi:** 4 Ekim 2026  
**Durum:** Repository prepared / live acceptance pending  
**Canlı mutation:** Yapılmadı

## İş çıktısı

Gereksiz public database yetkilerini kaldırırken bugün kullanılan legacy trial geçişini korumak; production provider-token runtime'ının encryption kapalı veya legacy read açık durumda sessizce başlamasını engellemek.

## Canlı bulgu özeti

- `expire_trials()` ve `handle_new_user()` postgres-owned `SECURITY DEFINER` fonksiyonlarıdır ve PUBLIC/anon/authenticated tarafından çalıştırılabilir.
- `enforce_platform_account_limit_guard()` mutable search path ve dış EXECUTE yüzeyi taşır.
- postgres ve supabase_admin için public schema default ACL'leri future table/sequence/function nesnelerini anon/authenticated rollerine geniş açar.
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
- Default privileges gelecekteki nesneler için explicit least-privilege olur.
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
