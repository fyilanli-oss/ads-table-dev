# E6-T4 + E6-T4A + E6-T4B — TikTok delivery-only adapter

## İş kararı

Adapter yalnız doğrulanmış `spend`, `impressions` ve `clicks` delivery alanlarını kabul eder. ATC, Checkout, Purchase ve value alanları non-empty provider kanıtına kadar `unsupported/null` kalır. Generic `conversion` alanı okunmaz ve purchase'a çevrilmez.

## Hiyerarşi ve double-count sınırı

Production fact yalnız `AUCTION_AD` leaf satırından üretilir. Campaign root ve AdGroup parent bilgileri zorunlu lineage'dır; ayrı additive Campaign/AdGroup canonical satırı üretilmez. Aynı business date ve deterministic Ad entity key tekrar ederse batch fail-closed durur.

## Canonical çıktı

Mapper yedi bloklu canonical envelope üretir: `identity`, `entity`, `raw_metrics`, `metric_support`, `currency`, `time`, `provenance`. Delivery metriği response'ta eksikse sıfır üretilmez; değer `null`, destek durumu `unknown` olur. Sentetik satır ve negatif metric reddedilir.

Bu paket runtime fetch, Dataset V2 write, FX conversion veya production flag activation yapmaz. Bunlar E6-T6 kapsamındadır.
