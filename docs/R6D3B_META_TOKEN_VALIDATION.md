# R6-D3-B — Meta token doğrulama uygulaması

## İş çıktısı ve iş değeri

Meta OAuth onayı artık tek başına bağlantı sayılmaz. AdsTable, yetkiyi canonical workspace kaydına almadan önce uzun ömürlü token'a çevirir; doğru Meta uygulamasına ait olduğunu, `ads_read` iznini ve gelecekteki expiry'yi doğrular. Bu sayede geçersiz veya başka uygulamaya ait bir token kullanıcıya bağlıymış gibi gösterilemez.

## Amaç

R6-D3-A kararını repository runtime'ında uygulamak ve Meta account selection öncesindeki token güvenlik kapısını tamamlamak.

## Mevcut durum

R6-D3-A öncesindeki embedded Meta callback yalnız authorization code'u ilk access token'a çeviriyor, provider response expiry'sini varsa kaydediyor ve doğrudan `pending_account_selection` oluşturuyordu. Long-lived exchange ile app/scope/expiry doğrulaması yoktu.

## Planlanan durum

1. Authorization code server-side kısa ömürlü user access token'a çevrilir.
2. Kısa token server-side supported long-lived user token'a çevrilir.
3. Long-lived token debugger endpoint'inde doğrulanır.
4. `is_valid`, configured app ID, `ads_read` ve gelecekteki expiry geçmeden canonical save yapılmaz.
5. Yalnız long-lived access token encrypted envelope olarak ve debugger kaynaklı expiry/scope ile `pending_account_selection` kaydına gider.
6. Doğrulama başarısızsa kullanıcı güvenli `Reconnect Meta` durumuna döner; token veya provider response gösterilmez.

## Kapsam

- Meta short-lived → long-lived token exchange.
- Meta token debugger kontrolü.
- App, scope ve expiry fail-closed doğrulaması.
- Graph version'ın authorize, exchange ve account discovery hattında aynı environment kaynağından kullanılması.
- Güvenli hata kodları ve Shopify-native `Reconnect Meta` sunumu.
- OAuth callback sonrasında `pending_account_selection` davranışının korunması.

## Kapsam dışı

- Production deployment veya canlı Meta OAuth.
- Meta performance/Insights çağrısı.
- Dataset V2 yazımı.
- Schedule/backfill.
- Disconnect/revoke.
- E4 mapper, Time/FX, writer veya formula değişikliği.

## Bağımlılıklar

- R6-D3-A lifecycle kararı: `Done`.
- R7-A canonical store ve 1–3 account selection temeli: `PASS`.
- Meta app ID/secret ve embedded redirect ayarları: production çalıştırmadan önce ayrı preflight konusu.

## Uygulama adımları

1. Meta'ya özel üç aşamalı server-side exchange/validation zincirini eklemek.
2. Canonical store'a yalnız doğrulanmış token sonucu göndermek.
3. Doğrulama hatalarını allowlist güvenli kodlara indirgemek.
4. Reauthorization sonucunu Meta'ya bağlı `Reconnect Meta` sunumuna çevirmek.
5. Mevcut callback, Shopify authority ve 1–3 hesap seçim regresyonunu çalıştırmak.

## Kabul kriterleri

- Long-lived exchange yapılmadan canonical save oluşamaz.
- `is_valid !== true`, app mismatch, eksik `ads_read`, bilinmeyen/geçmiş expiry fail-closed durur.
- Meta refresh token üretilmez veya beklenmez.
- Browser/token logu/provider body çıktısı yoktur.
- Başarılı callback yalnız `pending_account_selection` olur.
- 1–3 hesap seçimi olmadan `Connected` olmaz.
- Meta Graph version tek environment kaynağına bağlıdır.
- Klaviyo ve Google token davranışları değişmez.

## Test planı

- Üç server-side Meta isteğinin sıra ve endpoint testi.
- Long-lived token, debugger scope ve expiry persistence şekli.
- App mismatch, scope eksik ve geçmiş expiry negatif testleri.
- Callback güvenli reauthorization redirect testi.
- Shopify Platforms `Reconnect Meta` sunum testi.
- Mevcut embedded OAuth, provider strategy ve 1–3 hesap seçim regresyonu.
- `git diff --check`.

## Rollback planı

Repository değişiklikleri geri alınır ve Meta production activation kapalı tutulur. Bu paket production'a deploy edilmediği, provider'a istek atmadığı ve canonical kayıt yazmadığı için veri rollback'i yoktur.

## Gözlemlenebilirlik

Loglar yalnız allowlist güvenli hata kodu ve provider adı taşır. Token, app secret, provider response ve account ID loglanmaz.

## Güvenlik ve veri etkisi

- Client secret yalnız server-side request body/authorization header içinde kullanılır.
- Authorization ve debug URL'lerinde client secret bulunmaz.
- Canonical store'a plaintext değil mevcut encrypted vault sınırı üzerinden veri gider.
- Production mutation, provider teması ve Dataset V2 yazısı yoktur.

## Planlanan

R6-D3-A token kararını kod ve testlerle uygulamak.

## Gerçekleşen

Long-lived exchange, token debugger doğrulaması, güvenli reauthorization ve mevcut pending-account-selection geçişi repository düzeyinde uygulandı. Production çalıştırılmadı.

## Sapmalar

Yok.

## Evidence

- `contracts/r6d3b-meta-token-validation-v1.json`
- `tests/e10-t6c2g-runtime-platforms.test.js`
- `tests/e10-t6c2c-embedded-provider-oauth-routes.test.js`
- Odaklı R6-D3-B + embedded regresyon sonucu.

## Durum

`Done — repository only / R6-D3-C account-selection acceptance gate`
