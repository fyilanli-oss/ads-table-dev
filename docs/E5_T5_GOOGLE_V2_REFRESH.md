# E5-T5 — Google Time/FX/V2/job/telemetry entegrasyonu

## İş çıktısı

Tek Google V2 refresh işi şu sırayı zorunlu tutar: exact customer metadata → customer business date → Standard/PMax canonical adapter → business-date FX → canonical ownership/hierarchy/key doğrulaması → ortak Dataset V2 write boundary → redacted job evidence.

Google kaynağı ile raporlama para birimi farklıysa dört parasal fact (`spend`, ATC value, checkout value, purchase value) aynı onaylı günlük kurla bir kez çevrilir. Adet metrikleri değişmez. Same-currency rate yalnız `1` olabilir.

Refresh kanıtı customer kimliği, token, ham provider satırı veya metrik değer taşımaz. Yalnız customer metadata kontrolleri, campaign type, accepted/rejected sayıları ve V2 attempted/persisted sonucunu taşır.

## Production sınırı

Bu paket production route/runtime'a bağlanmaz; Google API veya production Dataset V2 işlemi çalıştırmaz. Canlı activation ayrı PR ve açık production onayı gerektirir.
