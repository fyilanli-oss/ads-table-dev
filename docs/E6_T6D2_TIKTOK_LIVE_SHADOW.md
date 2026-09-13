# E6-T6D2 — TikTok live shadow runtime

## Yapılan iş

TikTok legacy snapshot write otoriter kalırken aynı provider-derived Ad satırları Dataset V2'ye shadow olarak yazılır ve delivery parity hesaplanır. Runtime yalnız `TIKTOK_V2_SHADOW_ENABLED=true` ile açılır; primary TikTok read path veya dashboard otoritesi değiştirilmez.

Shadow kaynak veriyi ikinci kez tahminî bir TikTok sorgusuyla üretmez. Legacy snapshot'ın gerçek report yanıtından normalize edilmiş Ad leaf satırlarını canonical delivery mapper'a köprüler. Campaign/AdGroup toplamları V2'ye gönderilmez; synthetic fallback satırları writer öncesinde izole edilir.

## Evidence ve tamamlanma sınırı

Her refresh cevabı ve snapshot job metadata'sı redacted `tiktok_shadow_evidence` taşır. `production_activation` daima `false` kalır. Zero-row ile zero-row eşleşmesi `PASS` değildir: live evidence sayılabilmesi için iki tarafta da en az bir gerçek Ad satırı bulunmalıdır. Production API üzerinde gerçek delivery verisi oluşana kadar test/review hesabının görevi yalnız auth, advertiser erişimi, geçerli empty-result ve sentetik-write izolasyonunu doğrulamaktır.

Report seviyeleri TikTok'un 1 QPS review limitine uygun biçimde en az 1100 ms aralıkla okunur. Provider `40100` veya HTTP 429 döndürürse bounded backoff ile en çok üç deneme yapılır; limit devam ederse refresh fail olur. Başarılı boş response `rows: []` olarak korunur, sahte Campaign/AdGroup/Ad fallback entity üretilmez ve ham provider response snapshot'a yazılmaz.

Bu değişiklikten sonra yeni E6 alt paketi açılmaz. Kod akışı kapanmıştır; kalan tek operasyonel kapı, gerçek delivery verili normal advertiser üzerinde üç ardışık non-empty shadow `PASS` ve ayrı primary activation/rollback kararıdır.
