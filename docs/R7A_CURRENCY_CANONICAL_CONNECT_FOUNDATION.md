# R7-A — Currency-first ve canonical Connect foundation

## Analist kararı

R7-A tek seferde canlı OAuth kabulüne açılmayacaktır. Paket iki repository alt dilimine ayrılır:

- **R7-A1:** Reporting currency seçimi, Data Sources erişim kapısı, Connect açıklama modalları, aktif/parked provider sınırı ve OAuth sonucunun canonical workspace connection store'a yazılması.
- **R7-A2:** Meta ve Google Ads için provider tarafından doğrulanmış 1–3 reklam hesabı; Klaviyo için tek doğrulanmış hesap discovery/seçim akışı. Seçimler tek OAuth grant'i altında kanonik bağlantıya atomik yazılır.

R7-A repository uygulaması tamamlanmıştır. Account-cardinality migration'ı `20260924120453` sürümüyle production Supabase'e uygulanmış ve postcheck `PASS` vermiştir. PR #242 merge commit `a94201bf74c76cc57b5a357782c06a3b48bb3f67` production'a dağıtılmış; `dev.adstable.app` aynı READY deployment'a bağlanmıştır. Merchant reporting currency seçimi ve kontrollü gerçek provider kabulü yapılmadan R6-D'ye geçilemez. R6-D tamamlanmadan Meta/Google Disconnect deneyimi açılamaz.

## Kullanıcı akışı

1. Shopify doğrulanmış oturumu AdsTable workspace'ini sunucuda çözer.
2. Kullanıcı AdsTable reporting currency seçer. Shopify store veya presentment currency okunmaz ve varsayılmaz.
3. Currency kaydı yoksa Data Sources kartları açılmaz; OAuth endpoint'i de fail-closed durur.
4. Meta, Google Ads ve Klaviyo aktiftir. TikTok ve Pinterest Parked görünür ve OAuth allowlist'inde bulunmaz.
5. Karttaki Connect yalnız açıklama modalını açar. Cancel provider teması üretmez.
6. Modal içindeki açık Continue eylemi top-level OAuth'u başlatır.
7. Callback, token'ı yalnız `workspace_provider_connections` tablosuna `pending_account_selection` olarak yazar. Eski `shopify_workspace_provider_connections` yeni OAuth hedefi değildir.
8. Meta ve Google Ads'te kullanıcı provider tarafından doğrulanmış hesaplardan en az 1, en fazla 3 hesap seçer.
9. Klaviyo'da kullanıcı tek bir doğrulanmış hesap seçer; provider source currency ile aylık plan maliyeti ayrı kaydedilir. Reporting currency bundan bağımsızdır.

## Güvenlik sınırı

Tarayıcı `workspace_id`, `shop_id`, account adı veya currency için authority değildir. Workspace Shopify session token üzerinden sunucuda çözülür. Supabase service-role yalnız backend composition root'ta kullanılır; browser doğrudan `workspace_settings` veya `workspace_provider_connections` tablolarına erişmez.

## Bu pakette yapılmayanlar

- Application production deployment tamamlanmıştır; canlı provider teması ve OAuth kabulü henüz yapılmamıştır.
- Klaviyo Disconnect/revoke güvenli zinciri repository'de hazırlanır; Meta/Google Disconnect provider semantiği doğrulanmadan işlevsiz kontrol gösterilmez.
- Dataset V2 provider runner aktivasyonu R6-D'ye aittir.
- `workspace_provider_connections.selected_accounts` additive migration'ı production'da uygulanmıştır; RLS/FORCE RLS korunmuş, browser grant sayısı `0` kalmıştır.

## R7-A2 doğrulanmış hesap seçimi

- Meta account listesi Marketing API `me/adaccounts` cevabından alınır; account ID, ad ve currency kaydetme anında yeniden doğrulanır.
- Google Ads doğrudan erişilebilir customer resource'larını listeler; `customer_client` sorgusuyla manager olmayan müşteri hesabının ad ve currency bilgisi doğrulanır.
- Browser yalnız seçilen account ID listesini gönderir. Meta/Google için benzersiz liste 1–3, Klaviyo için tek ID'dir. Account adı, source currency veya workspace kimliği browser beyanından alınmaz.
- `active_account_id` geriye uyumluluk için seçimin ilk hesabını taşır; tam bağlantı kapsamının otoritesi `selected_accounts` listesidir. Funnel'da tek aktif hesap tercihi ayrı bir ürün ayarıdır ve OAuth kapsamını daraltmaz.
- OAuth access token ve Google developer token yalnız backend isteğinde kullanılır; response'a veya HTML'e yazılmaz.
