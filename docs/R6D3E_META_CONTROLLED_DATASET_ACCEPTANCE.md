# R6-D3-E — Meta kontrollü Dataset V2 kabulü

## Analist özeti

Bu paket yeni Meta metriği, mapper veya analiz mantığı geliştirmez. R6-D3-D'de canlı doğrulanan workspace Meta runner'ını mevcut workspace canonical write boundary ve Supabase Dataset V2 repository ile tek kontrollü kabul işleminde birleştirir.

## Kullanıcı açısından sonuç

Normal Data Sources ekranı değişmez. Yalnız `?acceptance=r6d3-meta` operatör yüzeyindeki ayrı kritik eylem, işlemin gerçek provider satırı varsa Dataset V2'ye kalıcı yazabileceğini açıkça bildirir. İşlem schedule, backfill veya otomatik aktivasyon açmaz.

## Kontrol sırası

1. Shopify session server-side workspace authority üretir.
2. Canonical `connected` Meta bağlantısı ve merchant-selected reporting currency okunur.
3. Token expiry ve `ads_read` kapsamı fail-closed doğrulanır.
4. Seçilmiş 1–3 hesap için yakın business-date penceresinde mevcut Meta satırı aranır; varsa işlem provider temasından önce durur.
5. Meta Account ve Insights sonuçları yeniden provider'dan alınır.
6. E4 mapper, Time/FX ve ortak provider-result kontrolleri uygulanır.
7. Yalnız workspace canonical sözleşmesini geçen gerçek satırlar Dataset V2'ye UPSERT edilir.
8. `attempted != persisted` ise başarı raporlanmaz.
9. Dış sonuç yalnız aggregate sayaçları ve verified empty/non-empty durumunu taşır.

## Boş sonuç kararı

Provider doğrulanmış boş sonuç döndürürse `attempted=0`, `persisted=0` ve `empty_provider_result=true` kabul edilir. Sahte Meta satırı oluşturulmaz. Bu sonuç güvenli boş-yol kabulünü kanıtlar; gerçek non-empty fiziksel UPSERT'in henüz gözlenmediği canlı kanıtta açıkça yazılır.

## Güvenlik ve veri etkisi

- Browser workspace, hesap, tarih veya currency authority değildir.
- Token, hesap/entity kimliği ve ham metrik response'a çıkmaz.
- V1 snapshot yazılmaz.
- Schedule, backfill ve production activation yoktur.
- Normal kullanıcı yüzeyi değişmez.
- Supabase browser grant'i açılmaz; yalnız mevcut server-side repository kullanılır.

## Repository kanıtı

- Kontrollü kabul: `src/providers/meta/controlled-dataset-acceptance.js`
- Session-bound route: `src/routes/shopify-ad-account-routes.js`
- Gizli operatör yüzeyi: `src/shopify/embedded-app-home.js`
- Testler: `tests/r6d3e-meta-controlled-dataset-acceptance.test.js`

## Durum

`PASS repository only — production deployment ve açık onaylı tek kontrollü Dataset V2 kabulü bekliyor`
