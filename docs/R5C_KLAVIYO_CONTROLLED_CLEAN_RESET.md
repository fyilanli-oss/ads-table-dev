# R5-C — Klaviyo kontrollü temiz bağlantı sıfırlaması

## İş kararı

23 Eylül 2026 tarihli canlı R5-B doğrulama isteği Klaviyo Account API'ye bir kez gönderildi ve uygulama `409` ile durdu. Supabase'deki embedded kayıt `connected`, aktif hesaplı, `USD` source currency'li ve şifreli access/refresh token zarflarına sahipti. Buna rağmen no-refresh doğrulama geçmedi. Bu sonuç eski grant'in kanonik bağlantıya taşınmasına izin vermez; yeniden yetkilendirme gerekir.

Kullanıcı temiz başlangıç kararı verdi. R5-C eski grant'i kontrollü olarak kapatmaya hazırlanır; yeni OAuth başlatmaz.

## Kullanıcı ve veri etkisi

- Klaviyo revoke ancak işlem anındaki ayrı kullanıcı onayından sonra bir kez çağrılır.
- Provider revoke başarılı olmadan Supabase bağlantı durumu değiştirilmez.
- Başarılı revoke sonrasında embedded bağlantı `revoked` olur; seçilmiş hesap, aylık maliyet, source currency, encrypted token envelopes ve tarihsel analytics silinmez.
- `workspace_provider_connections` tablosunda bu işlem nedeniyle satır oluşturulmaz.
- Human-attested legacy binding ve hesap seçimi olmayan ikinci legacy Klaviyo satırı değiştirilmez.
- Aynı işlem içinde OAuth, refresh, account selection veya Dataset V2 yazımı başlatılmaz.

## Hazırlanan güvenlik sınırı

`createKlaviyoControlledReset` yalnız exact `REVOKE_KLAVIYO_AND_START_FRESH` confirmation değeriyle çalışabilir. Shopify tarafından doğrulanmış workspace/shop authority'sine bağlı embedded refresh token'ı server tarafında okur. Provider `POST /oauth/revoke` başarılı olursa optimistic version kontrolüyle embedded satırı `revoked` yapar. Provider hatasında yerel durum korunur; eşzamanlı reconnect/değişiklik varsa `CONNECTION_CHANGED` ile fail-closed durur.

Bu modül henüz route'a veya kullanıcı düğmesine bağlanmamıştır. Repository hazırlığının merge/deploy edilmesi canlı revoke anlamına gelmez.

## Sonraki sıra

1. R5-C kod ve güvenlik testleri kabul edilir.
2. Canlı preflight yalnız bir uygun embedded Klaviyo grant'i ve boş canonical Klaviyo durumu doğrular.
3. Kullanıcı işlem anında revoke için ayrıca açık onay verir.
4. Tek revoke ve yerel `revoked` finalizasyonu çalıştırılır; postcheck yapılır.
5. R6 canonical runtime hazırlanır.
6. R7 Shopify-native Connect modalı üzerinden yeni OAuth, hesap seçimi, Klaviyo maliyeti ve Disconnect akışını açar.

## Kesin kapsam dışı

- Bu hazırlık aşamasında canlı provider çağrısı veya Supabase mutation.
- Token refresh, token silme veya plaintext log.
- Legacy tablo freeze'ini aşma.
- Yeni OAuth/reconnect.
- R6/R7 aktivasyonu.
