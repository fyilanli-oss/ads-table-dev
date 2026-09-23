# R7-A — Currency-first ve canonical Connect foundation

## Analist kararı

R7-A tek seferde canlı OAuth kabulüne açılmayacaktır. Paket iki repository alt dilimine ayrılır:

- **R7-A1:** Reporting currency seçimi, Data Sources erişim kapısı, Connect açıklama modalları, aktif/parked provider sınırı ve OAuth sonucunun canonical workspace connection store'a yazılması.
- **R7-A2:** Meta ve Google Ads için provider tarafından doğrulanmış account discovery/seçim akışları ile üç aktif provider'ın repository kabulü. Repository uygulaması tamamlandı.

R7-A repository uygulaması tamamlanmıştır. Production deployment sonrasında merchant reporting currency seçimi ve kontrollü gerçek provider kabulü yapılmadan R6-D'ye geçilemez. R6-D tamamlanmadan R7-B Connected/Disconnect deneyimi açılamaz.

## Kullanıcı akışı

1. Shopify doğrulanmış oturumu AdsTable workspace'ini sunucuda çözer.
2. Kullanıcı AdsTable reporting currency seçer. Shopify store veya presentment currency okunmaz ve varsayılmaz.
3. Currency kaydı yoksa Data Sources kartları açılmaz; OAuth endpoint'i de fail-closed durur.
4. Meta, Google Ads ve Klaviyo aktiftir. TikTok ve Pinterest Parked görünür ve OAuth allowlist'inde bulunmaz.
5. Karttaki Connect yalnız açıklama modalını açar. Cancel provider teması üretmez.
6. Modal içindeki açık Continue eylemi top-level OAuth'u başlatır.
7. Callback, token'ı yalnız `workspace_provider_connections` tablosuna `pending_account_selection` olarak yazar. Eski `shopify_workspace_provider_connections` yeni OAuth hedefi değildir.
8. Klaviyo hesabı provider API'den tekrar doğrulanır; provider currency ile aylık plan maliyeti ayrı kaydedilir. Reporting currency bundan bağımsızdır.

## Güvenlik sınırı

Tarayıcı `workspace_id`, `shop_id`, account adı veya currency için authority değildir. Workspace Shopify session token üzerinden sunucuda çözülür. Supabase service-role yalnız backend composition root'ta kullanılır; browser doğrudan `workspace_settings` veya `workspace_provider_connections` tablolarına erişmez.

## Bu pakette yapılmayanlar

- Production deployment veya canlı provider teması yoktur.
- Disconnect/revoke R7-B'ye aittir.
- Dataset V2 provider runner aktivasyonu R6-D'ye aittir.
- Yeni Supabase migration gerekmez; R2 ve R4 tabloları kullanılır.

## R7-A2 doğrulanmış hesap seçimi

- Meta account listesi Marketing API `me/adaccounts` cevabından alınır; account ID, ad ve currency kaydetme anında yeniden doğrulanır.
- Google Ads doğrudan erişilebilir customer resource'larını listeler; `customer_client` sorgusuyla manager olmayan müşteri hesabının ad ve currency bilgisi doğrulanır.
- Browser yalnız seçilen account ID'yi gönderir. Account adı, source currency veya workspace kimliği browser beyanından alınmaz.
- OAuth access token ve Google developer token yalnız backend isteğinde kullanılır; response'a veya HTML'e yazılmaz.
