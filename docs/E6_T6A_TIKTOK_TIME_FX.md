# E6-T6A — TikTok Time/FX binding

## Amaç

Delivery-only TikTok canonical satırını advertiser metadata'sındaki IANA timezone ve source currency ile bağlamak; business date'i provider daily date üzerinden normalize etmek ve onaylı FX oranını supported monetary fact'lere tam bir kez uygulamak.

## Kurallar

- Advertiser kimliği istek kapsamıyla birebir eşleşir.
- Provider date `YYYY-MM-DD` olmalı; server UTC sessiz fallback değildir.
- Source/target currency üç harfli koddur.
- Same-currency rate yalnız `1` olabilir.
- Cross-currency mapping pozitif rate ve isimli provider gerektirir.
- Delivery-only kararı gereği yalnız supported `spend_value` çevrilir; event value alanları `unsupported/null` kalır.
- Synthetic isolation ve normalized duplicate guard Time/FX sonrasında da korunur.

Bu parça Dataset V2 write, dual-write, parity veya production activation yapmaz; E6-T6'nın ilk kontrollü alt paketidir.
