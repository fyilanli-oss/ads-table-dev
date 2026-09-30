# AdsTable Shopify Public Embedded App — GO kararı

**Karar:** AdsTable, mevcut backend/canonical analytics omurgasını koruyarak Shopify Public Embedded App yönüne ilerler. Shopify install, doğrulanmış shop identity, embedded distribution ve Shopify-origin merchant billing kanalıdır; AdsTable provider adapter, Dataset V2, Formula Engine ve Funnel API business logic'ini korur.

## Dondurulan başlangıç sınırları

- İlk sürümde bir Shopify shop bir AdsTable workspace'e bağlanır; agency/multi-store daha sonra eklenir.
- Shopify-reported platform Purchase Count/Sales Value ile Meta/Google/TikTok/Klaviyo provider-reported conversion aynı fact değildir ve ayrı provenance taşır; Shopify total commerce verisi ilk intake'e alınmaz.
- Minimum scope ve mümkün olduğunca PII'siz ilk dilim hedeflenir.
- Browser tarafından taşınan shop/user/workspace identity authoritative değildir.
- Shopify-origin kullanıcı için embedded Shopify billing öncelikli değerlendirilir; bağımsız kanal ayrı capability'dir.
- App Store review hazırlığı son görev değil, requirements freeze ile başlayan paralel workstream'dir.
- E11 Funnel API, Shopify shop/workspace authority sözleşmesi tamamlanana kadar başlamaz.

## Doğrulama kapısı

Implementation başlamadan önce Public App, embedded auth/App Bridge, billing, privacy/protected data, mandatory lifecycle webhooks ve App Store review kuralları güncel resmi Shopify dokümantasyonundan linkli decision log ile doğrulanır. Erişilemeyen veya doğrulanmayan bir internet bilgisi teknik sözleşme kabul edilmez.

Production credential, Partner Dashboard değişikliği, scope talebi, billing aktivasyonu, migration, webhook registration veya App Store submission ayrı açık production onayı gerektirir.

## 30 Eylül 2026 — Embedded UI bağlayıcı standardı

Bu GO kararının bütün merchant-facing UI uygulamaları `docs/SHOPIFY_EMBEDDED_UI_CONSTITUTION.md` ve `contracts/shopify/shopify-embedded-ui-constitution-v1.json` ile yönetilir. App Bridge + güncel stabil App Home Polaris web componentleri dışındaki button/card/badge/modal/form/navigation taklitleri; inline CSS, literal renk ve yalnız desktop kabulü yasaktır. Desktop + gerçek mobil kanıt ve açık ürün sahibi kabulü olmadan UI işi tamamlanmış sayılmaz.