# E5-T1 — Google conversion count/value characterization

## İş kararı

Google Ads conversion breakdown içindeki `segments.conversion_action_category` birincil sınıflandırmadır.

- `ADD_TO_CART` → `add_to_cart` count/value
- `BEGIN_CHECKOUT` → `checkout` count/value
- `PURCHASE` → `purchase` count/value

Kategori eşleşmesi yoksa yalnız kapalı listedeki **tam action adı** fallback olarak kullanılabilir. `cart`, `order` veya `sale` gibi parça eşleşmeleri yasaktır. Lead ve diğer conversion action'ları purchase/revenue olarak kullanılamaz. Generic `metrics.conversions_value`, doğrulanmış PURCHASE action yerine geçemez.

Aynı kategoriye ait birden fazla action ayrı Google conversion tanımlarıdır ve count/value birlikte toplanır. Provider alias mantığı uygulanmaz.

## Provenance

Canonical provenance ham action adı, resource name, customer kimliği veya değer taşımaz. Yalnız mapping version, kategori/name eşleşme türü, eşleşen action sayısı, kategori allowlist'i ve fallback bilgisini taşır.

## Production sınırı

Bu paket production Google API çağrısı veya Dataset V2 yazımı yapmaz. E5-T2 ve sonraki adapter paketleri bu sözleşmeyi kullanacaktır.
