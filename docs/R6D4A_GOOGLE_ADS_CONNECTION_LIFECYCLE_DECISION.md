# R6-D4-A — Google Ads bağlantı ve token yaşam döngüsü kararı

## İş çıktısı ve iş değeri

AdsTable, Shopify içinden bağlanan Google Ads hesabını workspace'e ait tek canonical bağlantı olarak yönetecek. Kullanıcı provider tarafından doğrulanmış 1–3 reklam hesabını seçmeden bağlantı tamamlanmayacak; manager hesabı yalnız erişim bağlamı olacak ve reklam hesabı gibi seçilemeyecek. Access token süresi dolduğunda kullanıcı rutin olarak yeniden OAuth yapmak zorunda kalmayacak; server encrypted refresh token ile bağlantıyı yenileyecek.

## Kanıtlı mevcut durum

- Execution Plan içindeki E5 Google Ads Standard/PMax adapter, metrik ve conversion mapping, customer metadata, Time/FX, Dataset V2 writer ve doğrulanmış boş sonuç işleri tamamlanmıştır. Yeniden geliştirilmeyecektir.
- Eski `server.js + dashboard.html` yapısı offline OAuth, refresh token yenileme, `ListAccessibleCustomers + customer_client` hesap keşfi, manager altındaki hesaplar için `loginCustomerId` ve ayrı Connect/Disconnect davranışı kullanıyordu.
- Yeni embedded yapı Shopify session-bound top-level OAuth başlatıyor, callback'i `pending_account_selection` durumunda durduruyor ve provider'dan yeniden doğrulanmış 1–3 hesabı seçebiliyor.
- Yeni embedded keşif manager hesapları seçilebilir listeden çıkarıyor ve her reklam hesabı için doğru `loginCustomerId` değerini üretiyor.
- Ancak canonical `selected_accounts` kaydı şu anda yalnız `id`, `name`, `currency` tutuyor; `loginCustomerId` kayboluyor.
- Embedded canonical bağlantı için Google refresh-token yenileme mekanizması ve Google Ads'e özel Disconnect rotası henüz yoktur.
- Google Sheets ve GA4/Organic park edilmiştir; bu paket onları yeniden açmaz.

## Planlanan durum

1. Merchant Google Ads bağlantısını yalnız Shopify embedded Data Sources yüzeyinden başlatır.
2. OAuth yalnız Google Ads `adwords` scope'u ve offline access ile çalışır.
3. Geçerli refresh token ve bilinen access-token expiry olmadan canonical connection yazılmaz.
4. Callback yalnız `pending_account_selection` üretir.
5. Server doğrudan erişilebilir kökleri ve `customer_client` hiyerarşisini tarar; manager hesapları seçimden çıkarır.
6. Kullanıcı en az 1, en fazla 3 reklam hesabı seçer. Save sırasında hesaplar provider'dan yeniden doğrulanır.
7. Her seçilmiş hesap `id`, `name`, provider source `currency` ve o hesaba erişim sağlayan doğrulanmış `login_customer_id` ile canonical kayda yazılır.
8. Süresi dolan veya dolmak üzere olan access token server-side refresh edilir. Provider yeni refresh token döndürürse rotation canonical encrypted store'a atomik yazılır.
9. Refresh grant geçersizse legacy token'a düşülmez; kullanıcı `Reconnect Google Ads` görür.
10. Disconnect açık onayla yalnız Google Ads canonical credential ve hesap bağını temizler. Ortak Google OAuth client geçmişi nedeniyle provider tarafında global revoke yapılmaz; tarihsel analytics korunur.

## Currency kararı

- Workspace reporting currency, merchant'ın daha önce seçtiği workspace ayarıdır.
- Google Ads source currency, seçilen reklam hesabının provider tarafından doğrulanmış currency'sidir.
- Shopify store veya presentment currency okunmaz ve default yapılmaz.
- Manager hesabın currency'si alt reklam hesabın source currency'si yerine kullanılamaz.

## R6-D4 alt paketleri

1. **R6-D4-A:** Bu karar, executable contract ve Execution Plan kaydı.
2. **R6-D4-B:** Canonical OAuth token doğrulama, refresh lifecycle ve hesap başına `login_customer_id` persistence uygulaması.
3. **R6-D4-C:** Canlı merchant Connect → OAuth → 1–3 verified hesap → Save → reload kabulü.
4. **R6-D4-D:** Tamamlanmış E5 runtime ile workspace-bound salt-okunur Google Ads preflight.
5. **R6-D4-E:** Tek kontrollü Dataset V2 kabulü; gerçek satır veya provider-doğrulanmış boş sonuç.
6. **R6-D4-F:** Bağımsız Disconnect modalı, non-destructive Cancel, local credential cleanup ve temiz Reconnect kabulü.

## Kabul kriterleri

- Standalone Google kaydı embedded workspace authority olarak kullanılmaz.
- OAuth response refresh token ve gelecekteki access expiry taşımıyorsa canonical save yapılmaz.
- Tokenlar yalnız encrypted envelope olarak tutulur ve browser'a çıkmaz.
- Manager hesap reklam hesabı olarak seçilemez.
- Seçilen her hesabın doğrulanmış `login_customer_id` bağlamı canonical kayıtta korunur.
- 1–3 sınırı hem server hem database sınırında korunur.
- Access token server-side yenilenebilir; invalid grant kontrollü reconnect üretir.
- Disconnect yalnız Google Ads canonical bağlantısını temizler, provider global revoke yapmaz ve tarihsel veriyi silmez.
- E5 işleri, Google Sheets ve GA4 bu pakette geliştirilmez veya aktive edilmez.

## Kapsam dışı

- Production Google OAuth veya provider çağrısı.
- Supabase mutation.
- Dataset V2 yazımı.
- Schedule/backfill aktivasyonu.
- E5 adapter, mapper, Time/FX, Dataset writer veya metric işlerinin yeniden geliştirilmesi.
- Google Sheets ya da GA4/Organic reaktivasyonu.

## Test planı

- Versionlı JSON contract parse ve karar assertion'ları.
- Offline access, refresh token, expiry ve reconnect kuralları.
- 1–3 hesap, manager rejection ve `login_customer_id` persistence kuralları.
- Source/reporting currency ayrımı.
- Local-only Disconnect ve tarihsel veri koruma kuralları.
- Kapsam dışı production/provider/database/Dataset işlemlerinin negatif assertion'ları.

## Rollback planı

Bu paket yalnız plan, karar, contract ve test değiştirir. Rollback ilgili commit'in geri alınmasıdır. Provider grant, Supabase veya Dataset V2 değişmediği için veri rollback'i gerekmez.

## Durum

`Done — contract only / R6-D4-B implementation gate`

