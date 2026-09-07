# Pinterest Paket 1 — OAuth, hesap seçimi ve provider sınırı

Pinterest'in eski Passive/Legacy kilidi kaldırılır; ancak bu paket performans verisini Dataset V2'ye yazmaz ve production primary aktivasyonu yapmaz.

## Bağlantı sözleşmesi

- OAuth başlangıcı authenticated JSON handshake ve ortak, tek kullanımlık transaction state üzerinden çalışır.
- Callback kullanıcı kimliğini yalnız tüketilen transaction kaydından alır.
- Token exchange ve refresh server tarafındadır; token browser/account-picker cevabına girmez.
- OAuth tamamlandıktan sonra connection tek başına `Connected` sayılmaz; explicit advertiser seçimi zorunludur.
- Scope varsayılanı `ads:read,user_accounts:read` olup environment ile açıkça değiştirilebilir.
- OAuth ayarı eksikse genel bir hata yerine yalnız eksik environment adları döner; hiçbir secret değeri veya provider cevabı gösterilmez.

## Advertiser keşfi

- `/v5/ad_accounts` cevabı yalnız ID, ad, currency ve timezone alanlarına indirgenir.
- Dört alanın tamamı bulunmayan veya duplicate advertiser seçime sunulmaz.
- Ham provider cevabı ve HTML hata sayfası kullanıcıya/evidence'a taşınmaz.
- Account selection ortak ownership, limit, schedule ve backfill hazırlık sınırını kullanır.

## Paket sınırı

Bu paket OAuth/account access hazırlığıdır. Analytics metric semantiği gerçek provider cevabıyla ayrıca karakterize edilmeden mevcut legacy normalizer production contract sayılmaz. Snapshot/Dataset V2, Time/FX, parity ve primary activation Paket 2–3 kapsamındadır.

## İlk canlı bağlantı düzeltmesi

İlk canlı `Connect` isteğinin environment kontrolünü geçtiği, fakat OAuth state kaydı oluşmadan durduğu doğrulandı. Kök neden provider dokümanı veya yetkilendirme URL'si değil, `oauth_transactions` tablosundaki izin listesinin Pinterest'i içermemesiydi. Pinterest aynı tek kullanımlık transaction sınırını kullandığı için veritabanı constraint'i `pinterest` provider'ını kabul edecek şekilde genişletildi. Yetkilendirme adresi ve token adresi Pinterest'in resmi V5 OpenAPI tanımıyla karşılaştırıldı; mevcut adresler değişmeden korundu.

## İlk canlı advertiser keşfi düzeltmesi

OAuth callback'i tamamlandı ve Pinterest connection kaydı oluştu; dolayısıyla önceki transaction engeli kapandı. Sonraki boş account picker'ın kök nedeni resmi V5 `AdAccount` şemasındaki IANA alanının `time_zone` olmasına rağmen normalizer'ın yalnız `timezone`/`timezone_name` okumasıydı. Normalizer artık önce resmi `time_zone` alanını okur; kimlik, ad, currency veya timezone eksikse hesabı yine fail-closed biçimde seçime sunmaz.

## ID-only list yanıtı ve detay zenginleştirmesi

Resmi V5 OpenAPI sözleşmesinde `GET /ad_accounts` içindeki `AdAccount` nesnesinde yalnız `id` zorunludur; `name`, `currency` ve `time_zone` opsiyoneldir. Bu nedenle liste yanıtını tam hesap profili varsaymak hatalıdır. Runtime artık önce erişilebilir gerçek ID'leri alır, Paket 1 hesap limiti içinde her ID için resmi `GET /ad_accounts/{ad_account_id}` detay çağrısını yapar ve yalnız detay cevabı identity/currency/timezone sözleşmesini tamamlayan hesapları seçime sunar. İsim, para birimi veya timezone uydurulmaz.

## 2026-09-07 ürün kararı — Parked

Kullanıcı kararıyla Pinterest çalışması durduruldu. Yeni OAuth başlangıcı `PINTEREST_INTEGRATION_PARKED` ile kapanır, callback token değişimi veya connection yazımı yapmaz ve dashboard Pinterest'i `Parked` olarak gösterir. Mevcut connection, encrypted token, ownership veya tarihsel snapshot kayıtları bu değişiklikle silinmez. Paket 2–3 başlatılmaz; yeniden açma ancak ayrı bir kullanıcı iş kararıyla mümkündür.
