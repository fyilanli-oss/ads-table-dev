# AdsTable Shopify Public Embedded App — GO kararı

**Karar:** AdsTable, mevcut backend/canonical analytics omurgasını koruyarak Shopify Public Embedded App yönüne ilerler. Shopify install, doğrulanmış shop identity, embedded distribution ve Shopify-origin merchant billing kanalıdır; AdsTable provider adapter, Dataset V2, Formula Engine ve Funnel API business logic'ini korur.

## Dondurulan başlangıç sınırları

- İlk sürümde bir Shopify shop bir AdsTable workspace'e bağlanır; agency/multi-store daha sonra eklenir.
- Shopify-observed order/revenue ile Meta/Google/TikTok/Klaviyo provider-reported conversion aynı fact değildir ve ayrı provenance taşır.
- Minimum scope ve mümkün olduğunca PII'siz ilk dilim hedeflenir.
- Browser tarafından taşınan shop/user/workspace identity authoritative değildir.
- Shopify-origin kullanıcı için embedded Shopify billing öncelikli değerlendirilir; bağımsız kanal ayrı capability'dir.
- App Store review hazırlığı son görev değil, requirements freeze ile başlayan paralel workstream'dir.
- E11 Funnel API, Shopify shop/workspace authority sözleşmesi tamamlanana kadar başlamaz.

## Doğrulama kapısı

Implementation başlamadan önce Public App, embedded auth/App Bridge, billing, privacy/protected data, mandatory lifecycle webhooks ve App Store review kuralları güncel resmi Shopify dokümantasyonundan linkli decision log ile doğrulanır. Erişilemeyen veya doğrulanmayan bir internet bilgisi teknik sözleşme kabul edilmez.

Production credential, Partner Dashboard değişikliği, scope talebi, billing aktivasyonu, migration, webhook registration veya App Store submission ayrı açık production onayı gerektirir.
