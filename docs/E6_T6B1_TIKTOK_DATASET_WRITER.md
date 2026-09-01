# E6-T6B1 — TikTok Dataset V2 writer boundary

## Amaç

Time/FX normalize edilmiş delivery-only TikTok satırlarını canonical write boundary'ye bağlamak; ownership, entity key, sentetik izolasyon ve write cardinality kurallarını write öncesi tekrar doğrulamak.

## Davranış

- FX oranı writer içinde advertiser source currency, target currency ve provider date ile çözülür.
- Yalnız gerçek TikTok Ad leaf canonical satırları write boundary'ye ulaşır.
- Zero-row ve synthetic-only input boş write üretir; fake measured-zero satırı üretmez.
- Write sonucu input ile aynı cardinality'de değilse işlem fail-closed durur.
- FX, adapter ve Dataset V2 hataları sırasıyla `TIKTOK_FX_LOOKUP`, `TIKTOK_DELIVERY_ADAPTER`, `TIKTOK_DATASET_V2_WRITE` safe stage'i taşır.
- Dönen operasyon evidence'ı yalnız count/boolean alanları kullanır; advertiser kimliği, metric değeri ve provider payload içermez.

Bu paket injectable canonical boundary'yi tamamlar. Gerçek refresh route composition E6-T6B2, dual-write/parity E6-T6C ve production activation E6-T6D kapsamındadır.
