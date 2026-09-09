# E10-T6-B — Development App Bootstrap

**Durum:** `BLOCKED_BOOTSTRAP_ENV`
**Kontrol tarihi:** 2026-09-09
**Development onayı:** Alındı
**Shopify / Development Store / production teması:** Yapılmadı

## Sonuç

Kullanıcının “E10-T6-B’ye devam et” talimatı development-environment onayı olarak kaydedildi ve güvenli bootstrap preflight'ı çalıştırıldı. Bu yeni ortamda `SHOPIFY_API_KEY`, `SHOPIFY_API_SECRET`, `SHOPIFY_APP_URL` ve `SHOPIFY_DEV_STORE` bulunmuyor; Shopify CLI da kurulu değil. Bu nedenle app oluşturma/bağlama, configuration release veya Development Store install işlemi yapılmadı. Credential, shop adı ya da token tahmin edilmedi.

`npm run e10:t6b:preflight` yalnız değişken **adlarının varlığını**, development store domain biçimini, HTTPS app origin'ini, izin verilen development scope setini ve Shopify CLI kullanılabilirliğini kontrol eder. Değerleri veya secret'ları çıktıya yazmaz; eksikte exit code `2` ile fail-closed olur.

## Dondurulan development configuration

- App URL: `SHOPIFY_APP_URL` içindeki HTTPS origin.
- Callback: server-side olarak `<SHOPIFY_APP_URL>/auth/shopify/callback`; query/body ile değiştirilemez.
- Development Store: canonical `*.myshopify.com` domain; yalnız development ortamıdır.
- İlk scope allowlist'i yalnız `read_reports` adayıdır. Başka scope preflight'ta `SHOPIFY_SCOPE_NOT_APPROVED` ile reddedilir.
- İlk kabul: install callback → ID token validation → token exchange → doğrulanmış `shop → workspace` binding.
- T6-D öncesinde ShopifyQL query; ayrıca açık production onayı olmadan production store, billing, App Store submission, migration veya deployment yoktur.

## Engel kaldırıldığında

Gerekli environment değerleri yeni ortama secret olarak sağlanır ve resmi Shopify CLI kullanılabilir hale getirilir. Preflight `READY_FOR_MANUAL_BOOTSTRAP` vermeden hiçbir Shopify komutu çalıştırılmaz. PASS sonrasında yalnız development app/config/install ve redacted install/session binding evidence'ı tamamlanır; secret veya müşteri verisi repository'ye yazılmaz.
