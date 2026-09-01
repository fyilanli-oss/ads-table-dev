# E6-T6B2 — TikTok live refresh job composition

## Amaç

TikTok advertiser metadata, yalnız `AUCTION_AD` delivery raporu, E6-T6B1 writer ve ortak refresh job boundary'yi injectable bir runtime composition içinde birleştirmek.

## Davranış

- Provider isteği yalnız `spend`, `impressions`, `clicks` ister.
- Omitted `rows` başarılı zero-row kabul edilir; present non-array `rows` reddedilir.
- Advertiser metadata identity mismatch fail-closed durur.
- Job evidence yalnız count/boolean ve sabit contract alanları taşır; advertiser, job kimliği, metric değeri veya provider payload içermez.
- Provider failure `TIKTOK_PROVIDER_REPORT` safe stage'iyle job failure boundary'ye ulaşır.
- Evidence `event_metrics_written=0` ve `synthetic_written_to_canonical=0` invariant'larını taşır.

Bu paket Express route'a veya production flag'e bağlanmaz. Server composition/dual-write parity E6-T6C, production activation E6-T6D kapsamındadır.
