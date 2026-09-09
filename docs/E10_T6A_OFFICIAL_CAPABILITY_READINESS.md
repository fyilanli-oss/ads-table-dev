# E10-T6-A — Official capability ve development-readiness

**Durum:** `Verification — BLOCKED_OFFICIAL_DOCS_ACCESS`  
**Kontrol tarihi:** 2026-09-09  
**Shopify/Development Store/production teması:** Yok

## İş çıktısı

Güncel resmi Shopify package/repository kaynaklarından doğrulanabilen foundation bileşenleri PASS; yalnız `shopify.dev` doküman aramasıyla doğrulanabilen kritik UI, navigation ve ShopifyQL attribution ayrıntıları blokelidir. Sonuç **BLOCKED**dır; E10-T6-B açılamaz.

## Resmi kaynak envanteri

| Alan | Resmi kaynak | Sonuç |
|---|---|---|
| Shopify Dev MCP | [`@shopify/dev-mcp`](https://www.npmjs.com/package/@shopify/dev-mcp) 1.15.0 | PASS; Shopify'ın local, auth gerektirmeyen docs/schema doğrulama aracıdır. |
| App Bridge | [`Shopify/shopify-app-bridge`](https://github.com/Shopify/shopify-app-bridge) | PASS; aktif resmi repository, embedded Admin/Mobile/POS SDK yönünü doğrular. |
| JS API ve app tools | [`Shopify/shopify-app-js`](https://github.com/Shopify/shopify-app-js) | PASS; Admin API client, Shopify API, Express ve session-storage resmi paket ailelerini doğrular. |
| CLI | [`Shopify/cli`](https://github.com/Shopify/cli) ve `@shopify/cli` 4.8.0 | PASS yalnız mevcut resmi araç ailesi için; proje template/API sürümü seçimi değildir. |
| Resmi app templates | [`node`](https://github.com/Shopify/shopify-app-template-node), [`react-router`](https://github.com/Shopify/shopify-app-template-react-router), [`remix`](https://github.com/Shopify/shopify-app-template-remix) | PASS yalnız aday envanteri; AdsTable framework kararı henüz verilmedi. |
| Eski Polaris React | [`Shopify/polaris-react-archive`](https://github.com/Shopify/polaris-react-archive) | REJECTED; resmi repository deprecated/archive yönündedir. |

Official Dev MCP 1.15.0'ın bundled App Home talimatı Polaris App Home'u `unversioned`, App Bridge API + `s-` prefix'li global web components olarak tanımlar; `@shopify/polaris`/`@shopify/polaris-react` importunu yasaklar ve generated component code için resmi validation tool'unu zorunlu tutar. Bu, C1–C7'nin genel `s-*` yönünü destekler; exact property veya implementation PASS'i değildir.

## Kritik blokaj

Ortamın HTTPS proxy'si hem `shopify.dev` root hem doküman URL'lerinde origin'e ulaşmadan `403 CONNECT tunnel failed` üretir. Web search aracı ayrı olarak `401 Unauthorized` verir. DNS ve GitHub erişimi çalışır; dolayısıyla bu Shopify servis kesintisi kanıtı değildir.

Official Dev MCP'nin ShopifyQL talimatı schema/grammar'ın bundled instruction içinde olmadığını; metric, dimension ve schema adlarının developer documentation'da aranmasını ve asla tahmin edilmemesini açıkça şart koşar. Bundled Admin GraphQL introspection `shopifyqlQuery` wrapper'ını doğrulayabilir, fakat ShopifyQL içindeki platform attribution dimension veya Purchase/Sales field uyumluluğunu kanıtlamaz.

Bu nedenle aşağıdakiler UNVERIFIED/BLOCKED kalır:

- exact ShopifyQL platform attribution dimension;
- exact Purchase metric;
- Sales metric + platform dimension birlikteliği ve currency semantiği;
- minimum scope ve protected-data sonucu;
- exact App Home component property'leri ve code validation;
- exact App Bridge navigation/top-level OAuth exit;
- seçilecek Admin API sürümü ve development-store kabul adımları.

## Fail-closed karar

- `first_slice_scopes=[]` korunur; `read_reports`, `read_orders` veya başka scope talep edilmez.
- Field, component, API veya scope isim benzerliğinden uydurulmaz.
- Unofficial mirror, blog, search cache veya eski Polaris React contract kaynağı olmaz.
- E10-T6-A PASS değildir; E10-T6-B development bootstrap, Partner Dashboard, store install veya API query çalıştırılamaz.
- Erişim düzeldiğinde yalnız blocked satırlar güncel resmi docs + Dev MCP validation ile yeniden değerlendirilir; PASS satırları da sürüm drift'i için kontrol edilir.

## Acceptance için kalanlar

1. `shopify.dev` official docs search erişimi;
2. App Home/navigation/modal component örneklerinin Dev MCP validator PASS'i;
3. ShopifyQL schema docs'tan exact attribution dimension + metric kanıtı veya açık `UNAVAILABLE` kararı;
4. minimum scope/protected-data matrisi;
5. seçilen template/API version ve callback/CSP/development checklist;
6. bütün satırlar PASS/NOT_REQUIRED olmadan overall PASS verilmemesi.

Bu repository paketi readiness gözlemini kaydeder; ağ politikasını değiştirmez, Shopify'a bağlanmaz ve kullanıcıdan teknik işlem istemez.
