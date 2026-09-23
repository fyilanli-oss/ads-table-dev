# R4 — Workspace veri kaynağı bağlantı authority'si

## Durum

`R4-A + R4-B + R4-C complete — R5-A binding foundation complete; R5-B verification pending`.

R4-A, Meta Ads, Google Ads ve Klaviyo bağlantılarının Shopify'a değil doğrudan AdsTable workspace'ine ait olacağı ortak modeli hazırladı. R4-B, açık production onayıyla boş kanonik tabloyu canlıya ekledi. Bağlantı/token kopyalanmadı, OAuth route değiştirilmedi ve provider'a temas edilmedi.

## Analist özeti

Ticaret kanalı ile veri kaynağı farklı sorumluluklardır:

- Shopify bugün kullanıcının oturumunu ve bağlı olduğu AdsTable workspace'ini doğrulayan ticaret kanalı adapter'ıdır.
- Gelecekte WooCommerce aynı rolü ayrı bir installation adapter'ıyla üstlenebilir.
- Meta Ads, Google Ads ve Klaviyo bağlantılarının sahibi `workspace_id` ile AdsTable'dır.
- Bağlantının canonical kimliği `workspace_id + provider` olur; `shop_id` primary key veya foreign key değildir.

`last_authorized_via`, işlemin hangi doğrulanmış adapter üzerinden başlatıldığını denetim amacıyla taşır. Bu alan tenant authority veya bağlantı sahipliği vermez.

## Canlı salt okunur preflight — 23 Eylül 2026

- `workspace_provider_connections` henüz yok.
- Canonical workspace sayısı `1`.
- Shopify'a özel embedded connection sayısı `1`; Klaviyo ve `connected` durumunda.
- Eski standalone connection sayısı `8`.
- Eski encrypted token satırı `7`.
- Eski plaintext token satırı `0`.
- Shopify tablosunda duplicate `workspace_id + provider` grubu `0`.
- Hem embedded hem legacy Klaviyo kaydı bulunduğu için R4-A otomatik kopya yapmaz.

## R4-A/R4-B hedef tablo davranışı

`workspace_provider_connections`:

1. Bir workspace/provider için yalnız bir canonical kayıt tutar.
2. Yalnız ilk aktif dilimi kabul eder: Meta Ads, Google Ads ve Klaviyo.
3. TikTok ve Pinterest'i production connection yolunda reddeder.
4. Tokenları yalnız şifreli envelope olarak saklar.
5. OAuth callback sonrasında yalnız `pending_account_selection` üretir.
6. `connected` için server tarafından doğrulanmış account ID, source currency ve doğrulama zamanı ister.
7. Klaviyo `connected` için monthly plan cost ister.
8. `disconnected` ile provider tarafında grant revoke edilmesini birbirine eşitlemez.
9. Browser rollerini tamamen kapatır; yalnız server rolüne açık CRUD verir.

## Paket sınırları

### R4-A — Bu çalışma

- canlı ve repository envanteri;
- versionlı connection contract'ı;
- additive migration, preflight, postcheck ve rollback;
- commerce adapter'dan bağımsız store modülü ve negatif testler.

### R4-B — Tamamlandı

- Canlı preflight yeniden çalıştırıldı ve `PASS` verdi.
- `20260923091731_create_workspace_provider_connections` migration'ı açık onayla uygulandı.
- Kanonik tablo `0` satırla oluşturuldu; schema/constraint/index/RLS/grant postcheck `PASS` verdi.
- Mevcut sayılar değişmedi: Shopify-scoped `1`, legacy connection `8`, legacy token `7`, plaintext token `0`.
- Advisor kontrolünde yeni tablo için yalnız beklenen policiesiz RLS ve kullanılmamış indeks bilgi notları görüldü; yeni WARN oluşmadı.
- Canlı kanıt: `docs/security/evidence/R4B_WORKSPACE_PROVIDER_CONNECTION_APPLY_2026-09-23.json`.

### R4-C — Tamamlandı

- Standalone Meta, Google, Klaviyo, TikTok ve Pinterest OAuth başlangıç/callback yolları provider temasından önce kapatıldı; Shopify embedded OAuth açık kaldı.
- Legacy connection, token, ownership, snapshot schedule ve snapshot job yazıları database trigger'larıyla fail-closed donduruldu.
- `20260923093756_r4c_freeze_legacy_provider_writes` migration'ı açık onayla uygulandı; postcheck ve negatif yazma kontrolü `PASS` verdi.
- Google, Klaviyo ve parked TikTok için toplam `3` aktif schedule durduruldu. Klaviyo'nun `1` aktif schedule'ı artık inactive durumdadır.
- Dokuz gündür açık kalan `1` Google queued job ve `1` TikTok running job tarihsel satırları silinmeden `failed` durumuna kapatıldı; Klaviyo'da açık job bulunmadı.
- Mevcut `8` legacy connection ve `7` encrypted token satırı korundu; plaintext token `0`, kanonik tablo satırı `0` kaldı.
- Canlı kanıt: `docs/security/evidence/R4C_LEGACY_PROVIDER_WRITE_FREEZE_2026-09-23.json`.

### R5 — Konsolidasyon

- embedded ve legacy Klaviyo kayıtlarının açık binding ve provider doğrulamasıyla uzlaştırılması;
- doğrulanan bağlantının canonical tabloya alınması;
- erken revoke, token silme veya tahmine dayalı eşleştirme yapılmaması.
- R5-A'da explicit legacy user → workspace binding tablosu canlıya boş olarak eklendi; hiçbir eşleme otomatik üretilmedi.

## CLI sapması

Supabase migration scaffold komutu denendi ancak bu Codex ortamında `npx`, `npm` ve yerel `supabase` çalıştırıcısı bulunmadığı için komut başlatılamadı. Migration dosyası kontrollü `apply_patch` fallback'i ile repository'de oluşturuldu. Bu, canlı migration uygulaması değildir.
