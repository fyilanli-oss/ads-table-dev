# R6-D4-G — Google Sheets + GA4 kontrollü emeklilik ve Google Ads revoke

Google Sheets export utility ile GA4 Organic tek pakette emekli edilir. İlk Shopify embedded ürün dilimindeki aktif Google ürünü yalnız Google Ads'tir. Yeni Google projesi/client, tablo, migration, Dataset V2, metrik veya adapter işi yoktur.

- Connect, OAuth, refresh, discovery, manuel/otomatik çalışma ve veri yazımı kapalı kalır.
- Eski endpoint'ler fail-closed uyumluluk tombstone'u olarak kalır; durumları retired döner.
- Google Ads Disconnect, ortak Google grant'ini iptal eder; revoke başarısızsa local bağlantı korunur.
- Başarılı revoke sonrasında yalnız doğrulanmış workspace'in Google Ads credential ve hesap seçimi temizlenir.
- Spreadsheet metadata, inactive schedule'lar, tarihsel GA4 job'ları, Dataset V2, Meta, Klaviyo ve reporting currency korunur.

Canlı credential temizliği ve Google Cloud consent scope/API değişikliği repository paketine gömülmez. Deployment sonrasında ayrıca işlem-anı onayıyla revoke, Supabase postcheck, legacy credential cleanup, Cloud scope/API emekliliği ve temiz Google Ads reconnect yapılır. Bu kanıtlar tamamlanmadan production PASS yazılmaz.

## Production sonucu — PASS

PR #283 merge commit `0998f41124b2dade4238c9a2637f347da8568955` Security ve tam regression kontrollerinden sonra production'a dağıtıldı. Merchant onaylı Google Ads Disconnect ortak Google grant'ini revoke etti; temiz OAuth ilk-izin uyarısını yeniden gösterdi ve yalnız `adwords` scope istedi. Üç doğrulanmış hesapla reconnect tamamlandı.

Salt-okunur Supabase postcheck Google Ads'i connection version `13`, access/refresh/expiry mevcut ve üç hesapla `connected`; Meta ile Klaviyo'yu korunmuş; Google Sheets credential/connected sayısını `0/0`; GA4 tarihsel job/schedule sayısını `340/2`, aktif job/schedule sayısını `0/0`; Dataset V2'yi `0` ve reporting currency'yi `TRY` doğruladı. Google Cloud'da kullanılmayan Sheets/Analytics API'lerini kapatma non-blocking idari temizlik olarak kalır.
