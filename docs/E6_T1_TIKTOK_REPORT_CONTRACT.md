# E6-T1 — TikTok production metrics contract

## İş kararı

TikTok production performans kaynağı, Marketing API v1.3 `report/integrated/get` BASIC raporudur. Sözleşme resmi TikTok Business API raporlama referansına sabitlenmiştir ve uygulamada `tiktok-report-v1` olarak versiyonlanır.

Production canonical leaf `AUCTION_AD / ad_id` seviyesidir. `AUCTION_CAMPAIGN`, `AUCTION_ADGROUP` ve `AUCTION_AD` ayrı karakterizasyon seviyeleridir; bu seviyelerin toplamları birbirine eklenemez. Böylece aynı performansın üç kez sayılması sözleşme düzeyinde yasaklanır.

## Alan envanteri

| Provider metric | Canonical anlam | Destek |
| --- | --- | --- |
| `spend` | spend value | supported |
| `impressions` | impression | supported |
| `clicks` | ad click | supported |
| `add_to_cart` | add to cart count | tracking-dependent |
| `initiate_checkout` | checkout count | tracking-dependent |
| `complete_payment` | purchase count | tracking-dependent |
| `total_complete_payment_value` | purchase value | tracking-dependent |

Eksik tracking-dependent alan ölçülmüş sıfır değildir. `conversion`, `conversions` ve `conversion_value` generic alanları purchase count/value kanıtı olarak kullanılamaz. ATC, checkout ve purchase count/value ayrıntılı eşlemesi E6-T2/T3 kapsamındadır.

## Güvenlik ve production sınırı

Bu paket canlı TikTok isteği, token, advertiser kimliği, production write veya synthetic fallback üretmez. Fixture tamamen sentetiktir. Resmi referansın bu çalışma ortamından erişilememesi runtime sözleşmesini gevşetmez; canlı advertiser doğrulaması ileride ayrı, açık onaylı bir kabul kapısıdır.
