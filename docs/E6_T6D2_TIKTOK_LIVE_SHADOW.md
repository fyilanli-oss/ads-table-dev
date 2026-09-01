# E6-T6D2 — TikTok live shadow runtime

## Yapılan iş

TikTok legacy snapshot write otoriter kalırken aynı provider-derived Ad satırları Dataset V2'ye shadow olarak yazılır ve delivery parity hesaplanır. Runtime yalnız `TIKTOK_V2_SHADOW_ENABLED=true` ile açılır; primary TikTok read path veya dashboard otoritesi değiştirilmez.

Shadow kaynak veriyi ikinci kez tahminî bir TikTok sorgusuyla üretmez. Legacy snapshot'ın gerçek report yanıtından normalize edilmiş Ad leaf satırlarını canonical delivery mapper'a köprüler. Campaign/AdGroup toplamları V2'ye gönderilmez; synthetic fallback satırları writer öncesinde izole edilir.

## Evidence ve tamamlanma sınırı

Her refresh cevabı ve snapshot job metadata'sı redacted `tiktok_shadow_evidence` taşır. `production_activation` daima `false` kalır. T6D2'nin live evidence kısmı, deploy edilen runtime'da en az üç ardışık `PASS` görülmeden tamamlanmış sayılmaz.

Bu değişiklik E6'nın sondan bir önceki paketidir. Kalan tek paket E6-T6D3'tür: üç live PASS incelendikten sonra ayrı primary activation/rollback kararı.
