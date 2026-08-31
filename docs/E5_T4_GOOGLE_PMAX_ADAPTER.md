# E5-T4 — Google Performance Max adapter

## İş çıktısı

Performance Max, Standard Campaign → AdGroup → Ad modeline zorlanmaz. PMax canonical hierarchy yalnız `Campaign → Asset Group` olur.

- `campaign_type=performance_max`
- Campaign canonical root'tur.
- Asset Group canonical leaf'tir.
- Parent alanları `null` kalır.
- Sahte AdGroup veya Ad oluşturulmaz.

PMax ve Standard aynı yedi bloklu canonical envelope, raw metric ve support sözleşmesini kullanır. Ayrım yeni bir şema ile değil `campaign_type` ve entity hierarchy değerleriyle taşınır.

## Production sınırı

Bu paket production Google API çağrısı, Dataset V2 write, runtime delegation veya feature flag açılması yapmaz.
