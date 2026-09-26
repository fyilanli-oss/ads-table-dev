# R6-D4-C1 — Google Ads hesap keşfi corrective

## Analist özeti

Merchant Google Ads OAuth iznini tamamladı; callback canonical workspace bağlantısını `pending_account_selection` durumuna getirdi. Buna rağmen hesap seçim ekranı açılamadı. Production logu OAuth callback ve status isteklerinin başarılı, `/api/shopify/providers/google_ads/accounts` isteğinin ise `503` olduğunu kanıtladı.

## Eski ve yeni yapı karşılaştırması

- Eski `server.js + dashboard.html` akışı test hesaplarında environment'taki sabit `customerId + loginCustomerId` çiftini kullanarak discovery adımını atlayabiliyordu.
- Eski gerçek discovery `ListAccessibleCustomers` ardından `googleAds:search` kullanıyordu.
- Embedded canonical akış kullanıcıya gerçek 1–3 hesap seçtirmek için `ListAccessibleCustomers` ardından `customer_client` sorgusu çalıştırır; manager hesap seçim nesnesi değildir, yalnız `login_customer_id` bağlamıdır.
- İlk production hatasında embedded route bütün provider ayrıntısını tek `PROVIDER_ACCOUNTS_UNAVAILABLE` cevabına indirgediği için Google'ın güvenli hata sınıfı ve başarısız aşama logda bulunmadı.

## Corrective kapsamı

1. `list_accessible_customers` ve `customer_client_search` aşamaları ayrılır.
2. Provider HTTP hatası yoksa token lifecycle, JSON response, customer transform ve verified-account shape aşamaları ayrıca ayrılır.
3. Yalnız HTTP `401` access-token reauthorization olarak ele alınır; `403` developer-token veya Google Ads authorization kanıtını korur ve boşuna token refresh çalıştırmaz.
4. Yalnız HTTP status, Google hata sınıfı ve request ID server loguna yazılır.
5. OAuth token, developer token, hesap/customer ID, provider mesajı ve response body loglanmaz.
6. Yeni canonical `GOOGLE_ADS_DEVELOPER_TOKEN` adı tercih edilir; eski production `GOOGLE_DEVELOPER_TOKEN` yalnız server-side fallback olarak desteklenir.
7. Mevcut pending bağlantı korunur. Yeniden OAuth, Supabase mutation, Dataset V2 write, schedule/backfill veya Disconnect yapılmaz.

## Kabul sonucu

Repository testleri geçtikten ve deployment doğrulandıktan sonra merchant aynı Data Sources sayfasında hesap listesini yeniden açar. Yeni güvenli log gerçek provider hata sınıfını kanıtlar. Hesaplar yüklenirse 1–3 verified seçimle R6-D4-C devam eder; yüklenmezse yalnız kanıtlanan Google hata sınıfına yönelik düzeltme yapılır.

## Durum

`Repository corrective — production diagnostic gate`

