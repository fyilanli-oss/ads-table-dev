# R5-C — Klaviyo kontrollü temiz bağlantı sıfırlaması

## İş kararı

23 Eylül 2026 tarihli canlı R5-B doğrulama isteği Klaviyo Account API'ye bir kez gönderildi ve uygulama `409` ile durdu. Supabase'deki embedded kayıt `connected`, aktif hesaplı, `USD` source currency'li ve şifreli access/refresh token zarflarına sahipti. Buna rağmen no-refresh doğrulama geçmedi. Bu sonuç eski grant'in kanonik bağlantıya taşınmasına izin vermez; yeniden yetkilendirme gerekir.

Kullanıcı temiz başlangıç kararı verdi. R5-C eski grant'i kontrollü olarak kapatır; yeni OAuth başlatmaz. İşlem 23 Eylül 2026 tarihinde tamamlandı.

## Kullanıcı ve veri etkisi

- Klaviyo revoke ancak işlem anındaki ayrı kullanıcı onayından sonra bir kez çağrılır.
- Provider revoke başarılı olmadan Supabase bağlantı durumu değiştirilmez.
- Başarılı revoke sonrasında embedded bağlantı `revoked` olur; seçilmiş hesap, aylık maliyet, source currency, encrypted token envelopes ve tarihsel analytics silinmez.
- `workspace_provider_connections` tablosunda bu işlem nedeniyle satır oluşturulmaz.
- Human-attested legacy binding ve hesap seçimi olmayan ikinci legacy Klaviyo satırı değiştirilmez.
- Aynı işlem içinde OAuth, refresh, account selection veya Dataset V2 yazımı başlatılmaz.

## Hazırlanan güvenlik sınırı

`createKlaviyoControlledReset` yalnız exact `REVOKE_KLAVIYO_AND_START_FRESH` confirmation değeriyle çalışabilir. Shopify tarafından doğrulanmış workspace/shop authority'sine bağlı embedded refresh token'ı server tarafında okur. Provider `POST /oauth/revoke` başarılı olursa optimistic version kontrolüyle embedded satırı `revoked` yapar. Provider hatasında yerel durum korunur; eşzamanlı reconnect/değişiklik varsa `CONNECTION_CHANGED` ile fail-closed durur.

## İşlem anı onay yüzeyi

Kontrollü reset modülü Shopify session-token korumalı `POST /api/shopify/providers/klaviyo/accounts/reset` route'una bağlanmıştır. Data Sources ekranındaki **Remove old connection** düğmesi resmi Shopify `s-modal` bileşenini açar. Modalı açmak veya Cancel hiçbir provider/veritabanı işlemi yapmaz. Yalnız modal içindeki **Remove connection** eylemi exact confirmation değerini route'a gönderir.

Başarılı reset sonrasında mevcut Connect düğmesi açılmaz; ekran temiz bağlantı akışının henüz açık olmadığını belirtir. Yeni OAuth yalnız R6/R7 kabulünden sonra sunulacaktır. Kodun merge/deploy edilmesi tek başına canlı revoke anlamına gelmemiş; merchant'ın modal onayı işlem-anı onayı olmuştur.

## Canlı kapanış

- PR #233 merge commit `8f183720fe1e64975ce8adabbeb8318f55257259` production'da `READY` olarak doğrulandı.
- Merchant **Remove connection** eylemini onayladı; reset endpoint'i bir kez `200` döndü.
- Supabase postcheck `PASS`: embedded bağlantı `revoked`, canonical Klaviyo satırı `0`.
- Hesap, maliyet, currency, encrypted token zarfları ve tarihsel veriler korundu.
- Yeni OAuth, refresh, Dataset V2 yazımı ve token silme yapılmadı.

## Sonraki sıra

1. R5-C kod, güvenlik testleri, canlı revoke ve postcheck tamamlandı.
2. R6 canonical runtime ayrı kapsam ve onayla hazırlanır.
3. R7 Shopify-native Connect modalı üzerinden yeni OAuth, hesap seçimi, Klaviyo maliyeti ve Disconnect akışını açar.

## Kesin kapsam dışı

- Modal onayı verilmeden canlı provider çağrısı veya Supabase mutation.
- Token refresh, token silme veya plaintext log.
- Legacy tablo freeze'ini aşma.
- Yeni OAuth/reconnect.
- R6/R7 aktivasyonu.

