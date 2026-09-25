# R6-D3-C — Meta hesap seçimi kabul hazırlığı

## Analist özeti

R6-D3-C için yeni bir hesap seçimi altyapısı kurulmayacaktır. R7-A2 kapsamında hazırlanmış canonical seçim akışı yeniden kullanılacaktır. Bu paket, mevcut parçaların Execution Plan'daki Meta kabul şartlarını birlikte karşıladığını kanıtlar ve canlı merchant kabulü için sınırı dondurur.

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

## Canlı kabulte kanıtlanacaklar

1. Shopify Data sources içinden temiz Meta OAuth başlatılır.
2. OAuth sonrası Meta hesap seçim modalı açılır.
3. 1–3 hesap seçilebilir; dördüncü hesap kaydedilemez.
4. Save sonrasında Meta `Connected` görünür.
5. Sayfa yeniden açıldığında seçilen hesap sayısı korunur.
6. Supabase postcheck yalnız canonical Meta bağlantısının doğrulanmış hesap kapsamıyla `connected` olduğunu gösterir.
7. Dataset V2, schedule ve backfill sayıları değişmez.

## Kapsam dışı

- Canlı Meta OAuth ve gerçek hesap seçimi bu repository preflight'ın parçası değildir.
- Meta Insights/performance çağrısı yoktur.
- Dataset V2 yazımı yoktur.
- Schedule/backfill açılmaz.
- Disconnect/revoke yoktur.
- Google Ads kabulü ayrı R6-D4 paketidir.

## Durum

`Repository preflight PASS — production merchant acceptance pending`
