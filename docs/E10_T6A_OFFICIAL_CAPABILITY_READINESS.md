# E10-T6-A — Official capability ve development-readiness

**Durum:** `PASS — READY_FOR_EXPLICIT_DEVELOPMENT_APPROVAL`
**Kontrol tarihi:** 2026-09-09
**Shopify/Development Store/production teması:** Yok

## İş çıktısı

Önceki `BLOCKED_OFFICIAL_DOCS_ACCESS` ağ engeli kaldırıldı. `shopify.dev` resmi Markdown/reference sayfaları ve resmi `@shopify/dev-mcp` 1.15.0 docs/validation servisleri proxy üzerinden erişilebilir oldu. Daha önce blokeli bırakılan UI, navigation, ShopifyQL, scope, protected-data, API version ve development checklist satırları fail-closed yeniden doğrulandı. E10-T6-A sonucu **PASS**tır.

Bu PASS Shopify'da app oluşturmaz, scope istemez, Development Store'a bağlanmaz ve canlı sorgu çalıştırmaz. Yalnız **E10-T6-B için insan development onayı isteme kapısını** açar.

## Dondurulan resmi baseline

| Alan | Resmi kaynak | Karar |
|---|---|---|
| Shopify Dev MCP | [`@shopify/dev-mcp` 1.15.0](https://www.npmjs.com/package/@shopify/dev-mcp) | Docs search ve component validator erişimi PASS. |
| Admin GraphQL API | [`2026-07`](https://shopify.dev/docs/api/admin-graphql/2026-07) | T6-B/T6-D geliştirme baseline'ı; live smoke öncesi drift yeniden kontrol edilir. |
| App Home / Polaris | [App Home v1.0](https://shopify.dev/docs/api/app-home/v1.0) | Stable Polaris 1 channel, global `s-*` web components; eski Polaris React kullanılmaz. |
| App Bridge | [App Bridge web components](https://shopify.dev/docs/api/app-home/v1.0/app-bridge-web-components) | Polaris'ten ayrı ve unversioned; latest CDN/runtime contract'ı kullanılır. |
| Başlangıç template'i | [`shopify-app-template-react-router`](https://github.com/Shopify/shopify-app-template-react-router) | Public/embedded, feature-rich uygulama için T6-B adayı seçildi; T6-A CLI kurmadı. |
| Eski Polaris React | [`polaris-react-archive`](https://github.com/Shopify/polaris-react-archive) | REJECTED; dependency veya yeni UI temeli olamaz. |

## Shopify-native component doğrulaması

T5-C1–C7'de dondurulan component yönünü temsil eden tek App Home artifact'i resmi Dev MCP `validate_component_codeblocks` aracıyla doğrulandı.

- Artifact: `e10-t6a-component-matrix`, revision `2`.
- API: `polaris-app-home` (version parametresi verilmez; App Home validator bunu versioned API olarak kabul etmez).
- Sonuç: **VALID / PASS**.
- Doğrulanan temel aile: page/banner/section/layout, search/date/choice/checkbox, chip/button/menu/popover/modal/tooltip, badge/spinner ve table header/body/row/cell.
- Validator düzeltmesi: `s-checkbox` label property kullanır; `s-tooltip` bir `content` property alan wrapper değildir, `interestFor` ile hedeflenen sibling elementtir.

Bu kanıt component isim/property baseline'ını doğrular. Gerçek E12 ekranları üretildiğinde her yeni Shopify component kodu ayrıca aynı resmi validator'dan geçer.

## Embedded navigation, callback ve güvenlik

- [Navigation API](https://shopify.dev/docs/api/app-home/v1.0/apis/user-interface-and-interactions/navigation-api) dış URL'ye mevcut top-level context'te çıkış için `open(url, '_top')` / `target="_top"` contract'ını doğrular. Provider consent için `_blank`, nested iframe ve browser-authoritative dönüş URL'si kullanılmaz.
- App config'in `auth.redirect_urls` alanında en az bir geçerli callback URL gerekir. AdsTable exact callback'i server-side sabit allowlist'ten üretir; request query/body callback authority olamaz.
- Embedded response CSP `frame-ancestors https://{shop}.myshopify.com https://admin.shopify.com` taşır. `{shop}` yalnız doğrulanmış server-side shop identity'den çözülür; embedded olmayan yüzey framing'i reddeder.
- Browser App Bridge'in kısa ömürlü **ID token**'ını taşır. Backend claim'leri doğrular ve Shopify access token'ını server-side token exchange ile alır; ID/access token browser URL'sine veya loga yazılmaz.

## ShopifyQL attribution feasibility contract'ı

Resmi ShopifyQL `2026-07` sales schema ve syntax aşağıdaki exact adayları doğrular:

| AdsTable alanı | ShopifyQL contract'ı |
|---|---|
| Platform | `referring_platform` |
| Platform-attributed Purchase Count | `orders__last_click` |
| Platform-attributed Sales Value | `total_sales__last_click` |
| Attribution modeli | `WITH LAST_CLICK_ATTRIBUTION` |
| Para birimi | Varsayılan shop currency; yalnız açık `WITH CURRENCY '<ISO>'` ile dönüşüm |
| GraphQL wrapper | Admin API `shopifyqlQuery` |

Read-only aday query:

```text
FROM sales
  SHOW orders, total_sales
  GROUP BY referring_platform WITH LAST_CLICK_ATTRIBUTION
  SINCE <completed_start> UNTIL <completed_end>
  ORDER BY total_sales__last_click DESC
```

Önemli sınırlar:

1. Bu query **resmi schema/syntax capability kanıtıdır**, canlı mağaza sonucu değildir. Exact store availability, parse sonucu, platform value taxonomy, null/unattributed satırı ve toplamlar yalnız açık development onayından sonra E10-T6-D read-only smoke ile kanıtlanır.
2. `shopifyqlQuery`, resmi Admin GraphQL referansında `read_reports` scope'una ek olarak ad, adres, telefon ve e-posta dahil **Level 2 protected customer data access** şartı bildirir. Query PII kolonu seçmese bile bu platform gereksinimi yok sayılamaz.
3. İlk slice hiçbir Order/Customer satırı veya doğrudan tanımlayıcı ingest etmez. Protected-data başvurusu ve `read_reports` talebi T6-A'da yapılmadı; T6-B development onayı içinde görünür ve minimum olarak değerlendirilir.
4. Requirement ürün değerine göre ağır bulunursa otomatik `read_orders` fallback'i, Order ingestion veya provider fact overwrite yapılmaz; T6-D `BLOCKED` olur ve iş kararı istenir.

## Webhook ve Development Store checklist'i

- Public App Store dağıtımı için `customers/data_request`, `customers/redact` ve `shop/redact` mandatory compliance webhook'ları gerekir. AdsTable access-revocation contract'ı ayrıca `app/uninstalled` lifecycle'ını korur.
- İlk sync/order webhook'u T6-A gereksinimi değildir; T6-E'ye kadar açılmaz.
- Resmi bootstrap sırası: public app için resmi CLI/template → development config/App URL/callback/scope → released app configuration version → Development Store install → install callback → ID token doğrulama/token exchange → `shop → workspace` binding smoke.
- T6-A Shopify CLI kurmadı ve bu adımların hiçbirini çalıştırmadı.

## PASS ve kalan insan kapısı

- E10-T6-A: **PASS / Done**.
- E10-T6-B: **Ready — explicit development approval required**.
- Development onayı scope veya production onayı değildir. T6-B yalnız development app/store install ve install/session binding smoke kapsamındadır.
- Billing activation, App Store submission, production credential/store/data, migration ve deployment ayrıca açık production onayı olmadan yasaktır.

## Evidence sınırı

Repository evidence'ı secret, shop adı, token, credential veya müşteri verisi içermez. Resmi docs search/component validation geçici tooling ile çalıştırıldı; application dependency'si veya Shopify CLI kurulumu repository'ye eklenmedi.
