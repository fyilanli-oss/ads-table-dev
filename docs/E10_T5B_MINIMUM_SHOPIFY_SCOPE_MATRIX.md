# E10-T5-B — Minimum Shopify scope matrisi

## İlk dilim kararı

İlk commerce dilimi yalnız shop currency/timezone ile son erişilebilir order penceresinden Purchase, Sales ve Refund aggregate'larını hedefler. Tek aday scope `read_orders`dır. Customer adı, e-posta, telefon, adres, note, line-item customer içeriği ve serbest metin seçilmez veya persist edilmez.

Shop metadata alanlarının ek scope gerektirmediği varsayılmaz: `scope_status=verify_no_additional_scope` kapısı gerçek API sürümüyle doğrulanmadan production query kurulamaz. `read_orders` ve seçilen money/refund alanları da Partner Dashboard talebinden önce güncel resmi Admin GraphQL schema, protected-data sınıflandırması ve review gereksinimiyle tekrar doğrulanır.

## Ekran → veri → scope

| Funnel çıktısı | Shopify kaynağı | İlk karar | PII | Saklama | Provenance |
|---|---|---|---|---|---|
| Currency | Shop currencyCode | Ek scope durumu doğrulama kapısında | Yok | Current metadata | Shopify-observed |
| Timezone | Shop ianaTimezone | Ek scope durumu doğrulama kapısında | Yok | Current metadata | Shopify-observed |
| Purchase | Order id/time/cancel state aggregate | `read_orders` adayı | Alan seçimi PII içermez | Günlük aggregate | Shopify-observed |
| Sales | Order current total shop money aggregate | `read_orders` adayı | Alan seçimi PII içermez | Günlük aggregate | Shopify-observed |
| Refund | Order refund time/value/currency aggregate | `read_orders` adayı | Alan seçimi PII içermez | Günlük aggregate | Shopify-observed |
| Revenue | Shopify resource değildir | `Sales - Spend` backend hesabı | Yok | Türetilmiş, persist edilmez | AdsTable-calculated |

## Bilinçli exclusions

- Add to Cart ve Checkout için doğrulanmış minimum Admin API sözleşmesi yoktur; ilk Shopify-observed dilimde `unsupported`, asla sahte `0` olur.
- 60 günden eski order geçmişi için `read_all_orders` talep edilmez; ayrı ürün gereksinimi, Shopify review ve insan scope onayı olmadan açılamaz.
- `read_customers`, customer write scope'ları, product write/read, customer-events ve pixel write scope'ları ilk dilimde yasaktır.
- Provider'a eşleşmeyen commerce `Unattributed`dır; Organic değildir.

## Resmi yeniden doğrulama kaynakları

- https://shopify.dev/docs/api/usage/access-scopes
- https://shopify.dev/docs/apps/launch/protected-customer-data
- https://shopify.dev/docs/api/admin-graphql/latest/objects/Shop
- https://shopify.dev/docs/api/admin-graphql/latest/objects/Order
- https://shopify.dev/docs/api/admin-graphql/latest/objects/Refund

Bu belge veya JSON contract scope talebi değildir. Exact API version, field availability, order erişim penceresi ve protected-data sonucu production/Partner Dashboard adımından önce yeniden doğrulanır.
