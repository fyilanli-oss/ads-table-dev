# R6-D3-C — Meta hesap seçimi canlı kabulü

## Analist özeti

R6-D3-C için yeni bir hesap seçimi altyapısı kurulmamıştır. R7-A2 kapsamında hazırlanmış canonical seçim akışı yeniden kullanılmış; gerçek Shopify merchant oturumunda OAuth, hesap seçimi, `Connected · 1 account` görünümü ve reload kalıcılığı doğrulanmıştır. Bu PASS yalnız bağlantı ve hesap seçimi içindir; Meta performans verisi veya Dataset V2 kabulü değildir.

## Neden gerekli?

OAuth izni, kullanıcının hangi Meta reklam hesaplarını AdsTable'a bağlamak istediğini tek başına göstermez. Bağlantının tamamlanması için kullanıcı Meta'nın döndürdüğü hesaplardan en az 1, en fazla 3 hesap seçmelidir. Seçim kaydedilirken hesap kimliği, adı ve currency bilgisi tarayıcıdan değil Meta'dan yeniden doğrulanmalıdır.

## Eski ve yeni yapı karşılaştırması

### Eski `server.js + dashboard.html`

- OAuth callback hesapları keşfedip legacy `platform_ad_accounts` tablosuna yazıyordu.
- Seçim endpoint'i tarayıcıdan tam hesap nesnesi alıyordu.
- Seçim sırasında ownership, schedule ve backfill yan etkileri başlatılıyordu.
- Akış standalone kullanıcı kimliğine ve legacy tablolara bağlıydı.

### Shopify embedded canonical yapı

- OAuth callback yalnız `pending_account_selection` oluşturur.
- Shopify-native modal Meta'nın döndürdüğü hesapları gösterir.
- Tarayıcı yalnız seçilen hesap ID'lerini gönderir.
- Sunucu kaydetmeden önce `/me/adaccounts` listesini yeniden alır.
- Yalnız yeniden doğrulanan 1–3 hesap canonical workspace bağlantısına yazılır.
- `active_account_id` ilk hesabı geriye uyumluluk için taşır; tam kapsam `selected_accounts` alanındadır.
- Bu adım schedule, backfill veya Dataset V2 yazısı başlatmaz.

## Mevcut uygulama kanıtı

- Account discovery: `src/shopify/ad-account-selection.js`
- Shopify session-bound route: `src/routes/shopify-ad-account-routes.js`
- Shopify-native modal davranışı: `src/shopify/ad-account-ui.js`
- Canonical persistence ve optimistic version guard: `src/providers/workspace-provider-connection-store.js`
- Database cardinality: `supabase/migrations/20260924120453_add_workspace_provider_selected_accounts.sql`

## Repository kabul kriterleri

1. Session doğrulanmadan status/list/select endpoint'leri çalışmaz.
2. Meta hesap listesi bearer access token ile server-side okunur.
3. Browser yalnız hesap ID listesi gönderebilir; ad, currency veya workspace id otorite değildir.
4. Save sırasında provider listesi yeniden okunur.
5. Sıfır, üçten fazla, duplicate veya provider listesinde olmayan seçim reddedilir.
6. Başarılı seçim canonical satırı `pending_account_selection` durumundan `connected` durumuna geçirir.
7. Aynı connection version kullanılmadan eşzamanlı/eski seçim yazılamaz.
8. Database bağlı Meta kaydında 1–3 `selected_accounts` zorunluluğunu korur.
9. Bu paket provider performans verisi, Dataset V2, schedule veya backfill başlatmaz.

## Canlı kabulde kanıtlananlar

1. Shopify Data sources içinden temiz Meta OAuth başlatılır.
2. OAuth sonrası Meta hesap seçim modalı açılır.
3. 1–3 hesap seçilebilir; dördüncü hesap kaydedilemez.
4. Save sonrasında Meta `Connected` görünür.
5. Sayfa yeniden açıldığında seçilen hesap sayısı korunur.
6. Supabase postcheck yalnız canonical Meta bağlantısının doğrulanmış hesap kapsamıyla `connected` olduğunu gösterir.
7. Dataset V2, schedule ve backfill sayıları değişmez.

## Canlı kabul sonucu

Merchant kabulü aşağıdaki hazırlanmış kontrollerle tek geçişte yürütülmüştür:

- Analist koşucusu: `docs/R6D3C_META_PRODUCTION_MERCHANT_ACCEPTANCE_RUNBOOK.md`
- Salt-okunur ön kontrol: `docs/security/sql/R6D3C_META_ACCOUNT_SELECTION_PREFLIGHT.sql`
- Salt-okunur son kontrol: `docs/security/sql/R6D3C_META_ACCOUNT_SELECTION_POSTCHECK.sql`

Ön kontrol canonical Meta kaydının temiz başlangıçta olduğunu; son kontrol ise yalnız doğrulanmış 1–3 hesabın `connected` kayda dönüştüğünü ölçer. İki sorgu da Dataset V2 Meta satırlarının, aktif Meta schedule'ın ve açık Meta job'ın `0` kaldığını kontrol eder. Legacy Meta kayıt sayısı başlangıçta kaydedilir ve son kontrolde aynı kalmak zorundadır. Token veya hesap kimliği çıktıya alınmaz; ekran görüntüsü kabul şartı değildir.

PR #261 merge edilmiş; production deployment ve `dev.adstable.app` alias eşleşmesi doğrulanmıştır. Salt-okunur preflight `PASS` sonrasında merchant akışı tamamlanmış, bir Meta hesabı seçilmiş ve sayfa yenilendikten sonra `Connected · 1 account` korunmuştur. Salt-okunur postcheck canonical Meta satırını `connected`, seçimi geçerli ve browser grant sayısını `0` olarak doğrulamıştır. Legacy Meta kayıt sayısı `1 → 1`; Dataset V2 Meta satırı, aktif legacy Meta schedule ve açık legacy Meta job sayıları `0` kalmıştır.

Redacted aggregate kanıt: `docs/security/evidence/R6D3C_META_ACCOUNT_SELECTION_LIVE_ACCEPTANCE_2026-09-25.json`

## Kapsam dışı

- Meta Insights/performance çağrısı yoktur.
- Dataset V2 yazımı yoktur.
- Schedule/backfill açılmaz.
- Disconnect/revoke yoktur.
- Google Ads kabulü ayrı R6-D4 paketidir.

## Durum

`PASS — production merchant OAuth, one verified account selection and reload persistence; Meta data runtime not accepted`
