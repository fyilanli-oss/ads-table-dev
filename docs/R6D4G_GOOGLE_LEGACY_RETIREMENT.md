# R6-D4-G — Google Sheets + GA4 kontrollü emeklilik ve Google Ads revoke

Google Sheets export utility ile GA4 Organic tek pakette emekli edilir. İlk Shopify embedded ürün dilimindeki aktif Google ürünü yalnız Google Ads'tir. Yeni Google projesi/client, tablo, migration, Dataset V2, metrik veya adapter işi yoktur.

- Connect, OAuth, refresh, discovery, manuel/otomatik çalışma ve veri yazımı kapalı kalır.
- Eski endpoint'ler fail-closed uyumluluk tombstone'u olarak kalır; durumları retired döner.
- Google Ads Disconnect, ortak Google grant'ini iptal eder; revoke başarısızsa local bağlantı korunur.
- Başarılı revoke sonrasında yalnız doğrulanmış workspace'in Google Ads credential ve hesap seçimi temizlenir.
- Spreadsheet metadata, inactive schedule'lar, tarihsel GA4 job'ları, Dataset V2, Meta, Klaviyo ve reporting currency korunur.

Canlı credential temizliği ve Google Cloud consent scope/API değişikliği repository paketine gömülmez. Deployment sonrasında ayrıca işlem-anı onayıyla revoke, Supabase postcheck, legacy credential cleanup, Cloud scope/API emekliliği ve temiz Google Ads reconnect yapılır. Bu kanıtlar tamamlanmadan production PASS yazılmaz.
