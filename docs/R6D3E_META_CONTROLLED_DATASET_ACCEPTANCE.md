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

## Kanıt

- Kontrollü kabul: `src/providers/meta/controlled-dataset-acceptance.js`
- Session-bound route: `src/routes/shopify-ad-account-routes.js`
- Gizli operatör yüzeyi: `src/shopify/embedded-app-home.js`
- Testler: `tests/r6d3e-meta-controlled-dataset-acceptance.test.js`
- Canlı redacted aggregate kanıt: `docs/security/evidence/R6D3E_META_CONTROLLED_DATASET_LIVE_ACCEPTANCE_2026-09-25.json`

## Durum

`PASS production verified empty — attempted 0 / persisted 0 / synthetic 0`

PR #265 merge commit `ef0d2cd240b87b626b141111ed5712404fff816d` production'a dağıtıldı. Açık kullanıcı onayıyla tek kontrollü kabul çalıştırıldı ve `PASS — attempted: 0, persisted: 0, verified empty: true` sonucu alındı. Supabase salt-okunur postcheck canonical Meta bağlantısını `connected · 1 account`; Dataset V2 toplam/Meta, aktif Meta schedule, açık Meta job ve browser grant sayılarını `0` olarak doğruladı. Legacy Meta kaydı `1` olarak değişmeden kaldı.

Bu PASS gerçek provider sonucunun güvenli boş-yol kabulüdür. Sahte satır üretilmedi ve production activation açılmadı. Gerçek non-empty fiziksel UPSERT henüz gözlenmemiştir; ilk gerçek Meta satırı geldiğinde mevcut idempotent writer ve evidence sayaçlarıyla ayrıca izlenecektir.


## 3 Ekim 2026 historical non-empty corrective

Salt-okunur R6-D5-M1 çalışması, son kapanmış gün boş olsa bile 31 günlük bounded pencerede gerçek Meta verisinin bulunduğunu kanıtladı. Kontrollü Dataset V2 kabulü artık browser'dan tarih almaz; her seçilmiş hesabın timezone'undaki son kapanmış günden geriye en fazla 31 gün tarar, provider'ın döndürdüğü en son gerçek günü server-side seçer ve aynı günü exact-date olarak yeniden çekip mevcut E4 mapper, Time/FX ve workspace canonical write boundary üzerinden işler.

Canlı salt-okunur kanıtın hedefi: `2026-10-01`, 1 Ad satırı, 181 impression, 8 canonical link click ve 117.22 TRY spend. Meta cevabında add-to-cart, checkout ve purchase action type'ları ile action_values bulunmadığından bu conversion alanları `unknown/null` kalır; `0` üretilmez.

Koruma sınırı provider çağrısından önce tüm 31 günlük pencereyi kapsar. Aynı workspace/account için mevcut Meta satırı varsa işlem fail-closed durur. Route browser body içindeki workspace, account veya provider date alanlarını kullanmaz. Schedule, backfill ve production activation kapalı kalır. Bu repository hazırlığı Dataset V2'ye henüz yazmamıştır; tek production çalıştırma ayrıca açık işlem-anı onayı gerektirir.


## Historical non-empty canlı sonuç — 3 Ekim 2026

Açık işlem-anı onayıyla production kontrollü kabul bir kez çalıştırıldı ve `attempted 1 / persisted 1 / verified empty false` sonucu verdi. Salt-okunur Supabase postcheck `2026-10-01` tarihinde tek Meta Ad satırını, duplicate `0`, 181 impression, 8 ad click ve 117.22 TRY spend ile doğruladı. Add to cart, checkout ve purchase hem değer kolonlarında `NULL` hem de metric support'ta `unknown` kaldı. Satır sentetik değildir. Schedule, backfill ve production activation açılmadı. Redacted kanıt `docs/security/evidence/R6D5M2_META_HISTORICAL_DATASET_WRITE_LIVE_2026-10-03.json` içindedir.
