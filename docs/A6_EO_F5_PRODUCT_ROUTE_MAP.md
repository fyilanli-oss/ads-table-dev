# A6 EO-F5 — Embedded-only ürün ve route haritası

**Tarih:** 4 Ekim 2026  
**Durum:** EO-F5 tamamlandı; EO-F6 bekleniyor  
**Etkisi:** Salt-okunur ürün, route ve consumer sözleşmesi. Kod, repository/project, Shopify configuration, provider, veritabanı veya deployment değiştirmez.

## Analist sonucu

Hedef ürünün Shopify embedded yolculuğu ve production route sınırı donduruldu.

Yeni uygulama mevcut `/shopify/app/*` ağacını, standalone login/dashboard yüzeyini veya operator tanı route'larını taşımayacaktır. Shopify Admin içindeki uygulama kökü `/` doğrudan **Funnel** ürün yüzeyidir. İlk kurulum eksikse aynı kök server-authoritative guard ile kullanıcıyı yalnız gerekli adıma — Currency veya Platforms — götürür. Uygulama hazırsa her açılışta Funnel'a döner.

Bu kararın iş değeri şudur: merchant'ın ilk günden review'a kadar gördüğü yol tek ve açıklanabilir olur; eksik kurulum, reauthorization, veri gecikmesi, billing veya deletion durumu sahte başarı/boş sıfır olarak gizlenmez. Tarayıcı hiçbir zaman workspace, entitlement veya job authority'si olmaz.

## Resmî Shopify kontrolü

4 Ekim 2026 tarihinde güncel resmî kaynaklar yeniden kontrol edildi:

- iframe tabanlı public App Home, App Bridge ve Polaris web components kullanır;
- App Nav'da uygulama adı zaten home route'a gider; ayrı, yinelenen home link'i eklenmez;
- App Nav desktop'ta sidebar, mobile'da dropdown olarak Shopify tarafından sunulur;
- mandatory privacy topic'leri `customers/data_request`, `customers/redact` ve `shop/redact`tir; HMAC doğrulanamayan istek 401 ile reddedilir;
- app-specific webhook subscriptions app configuration üzerinden yönetilmelidir;
- Polaris 2 hâlâ release-candidate niteliğindedir; mevcut UI Constitution'daki dev-store revalidation kapısı korunur.

Kaynaklar:

- https://shopify.dev/docs/api/app-home/latest
- https://shopify.dev/docs/api/app-home/latest/app-bridge-web-components/app-nav
- https://shopify.dev/docs/apps/build/app-home/polaris2
- https://shopify.dev/docs/apps/build/compliance/privacy-law-compliance
- https://shopify.dev/docs/apps/build/webhooks/subscribe
- https://shopify.dev/docs/api/webhooks/2025-10

## Product shell ve navigation kararı

### Canonical UI route'ları

| Route | Ürün yüzeyi | Authority/consumer | Review durumu |
|---|---|---|---|
| `/` | Funnel ve App Home | Query/Formula + Dataset V2; guard tamam değilse onboarding yönlendirmesi | Review-critical |
| `/dashboard` | Dashboard | Canonical aggregate/read model | Review-critical |
| `/ad-analysis` | Ad Analysis | Analytical leaf/query contract | Review-critical |
| `/attribution-differences` | Attribution Differences | Read-only comparison; Shopify report capability yoksa açık unavailable state | Capability-gated |
| `/platforms` | Provider connect/account/cost setup | Canonical provider connections | Review-critical |
| `/settings` | Currency, plan/trial, privacy ve Delete My Data | Workspace settings, entitlement, privacy deletion | Review-critical |

`/analysis`, `/shopify/app`, `/shopify/app/settings`, `/shopify/app/platforms`, `/shopify/app/funnel` ve `/shopify/app/analysis` hedef route değildir. Eski runtime cutover'a kadar bunların sahibi olmaya devam eder; yeni runtime'da kalıcı alias veya ikinci ürün ağacı kurulmaz.

### App Nav davranışı

Mantıksal ürün sırası:

1. Funnel
2. Dashboard
3. Ad Analysis
4. Attribution Differences
5. Platforms
6. Settings

Shopify'ın resmî home davranışı nedeniyle Funnel, uygulama adı ve `/` üzerinden home'dur; App Nav içine yinelenen “Funnel” satırı eklenmez. Render edilen navigation öğeleri Dashboard, Ad Analysis, Attribution Differences, Platforms ve Settings olur. Böylece dondurulmuş ürün sırası korunur, fakat Shopify'ın yinelenen home link'i eklememe kuralına uyulur.

Funnel'daki Table görünümü ayrı global route değildir; aynı `/` yüzeyindeki görünüm seçimidir.

## Giriş ve onboarding resolver

Her UI isteği server-side doğrulanmış Shopify session'dan `shop`, installation generation ve `workspace_id` türetir. Query/body içindeki workspace veya shop authority değildir.

Resolver sırası:

1. Shopify session yok/geçersiz → fail-closed embedded authentication response; standalone login yok.
2. Installation bootstrap eksik/geçersiz → managed install/bootstrap; başarılı olmadan ürün verisi yok.
3. Billing entitlement yok/expired → `/settings` içindeki plan/trial state.
4. Reporting currency yok → `/settings` currency setup.
5. Aktif, doğrulanmış provider hesabı yok → `/platforms`.
6. Klaviyo seçili ve gerekli Email Monthly Plan Cost eksik → `/platforms` içindeki cost setup.
7. Hazır → `/` Funnel.

Deep link doğrudan bir rapor route'una gelse de aynı guard çalışır. Guard tamamlanınca güvenli return target allowlist ile korunur; dış URL, secret, PII veya caller-supplied tenant taşınmaz.

## Authenticated product API sınırı

UI yalnız aynı-origin server BFF route'larını tüketir; browser database client veya provider token görmez.

| Method ve route | İşlev | Temel schema |
|---|---|---|
| `GET /api/app/context` | Session, onboarding, entitlement, currency ve provider readiness özeti | `app`, `shopify`, `billing`, `integrations` |
| `GET /api/workspace/settings` | Reporting currency ve kullanıcıya gösterilebilir ayarlar | `app` |
| `PATCH /api/workspace/settings/reporting-currency` | Merchant seçimi; allowlist + audit | `app` |
| `GET /api/providers` | Meta/Google Ads/Klaviyo bağlantı özeti | `integrations` |
| `GET /api/providers/:provider/accounts` | Verified account discovery | `integrations` + provider client |
| `PUT /api/providers/:provider/active-account` | Canonical reporting account seçimi | `integrations` |
| `POST /api/providers/:provider/disconnect` | Local authority değişimi; revoke ayrı doğrulanmış davranış | `integrations` |
| `GET /api/providers/klaviyo/email-spend-history` | Versionlı maliyet geçmişi | `analytics` |
| `POST /api/providers/klaviyo/email-spend-history` | Effective-date maliyet kaydı | `analytics` |
| `GET /api/reports/funnel` | Funnel/Table read model | `analytics`, `operations` |
| `GET /api/reports/dashboard` | Dashboard aggregate read model | `analytics`, `operations` |
| `GET /api/reports/ad-analysis` | En alt kanıtlı analytical leaf | `analytics`, `operations` |
| `GET /api/reports/attribution-differences` | Salt-okunur provider/Shopify karşılaştırması | `analytics`, capability boundary |
| `GET /api/refresh/status` | Son run, coverage, freshness/finality; refresh başlatmaz | `operations` |
| `GET /api/billing/entitlement` | Trial/subscription truth | `billing` |
| `POST /api/billing/subscribe` | Shopify billing onay akışını başlatır | `billing`, Shopify API |
| `POST /api/privacy/deletion-requests` | Delete My Data talebini idempotent açar | `privacy` |
| `GET /api/privacy/deletion-requests/:requestId` | Yalnız aynı workspace için durum | `privacy` |

Exact request/response schema'ları ilgili EO-02–EO-07 paketlerinde versionlanır. Bu harita isim ve authority sınırını dondurur; implementation değildir.

## OAuth, callback ve webhook route'ları

### OAuth

- `POST /api/providers/:provider/oauth/start`
- `GET /api/providers/:provider/oauth/callback`

`:provider` allowlist'i yalnız `meta|google-ads|klaviyo`dur. Start authenticated Shopify session ve workspace-owned transaction ister. Callback state, PKCE/nonce ve transaction TTL doğrulaması yapmadan token exchange veya connection write yapmaz. Başarılı/iptal/hata dönüş hedefi yalnız `/platforms` allowlist'idir.

TikTok ve Pinterest eski contract'larda görünse bile hedef provider allowlist'ine girmez; bu güncel EO kararı E10-T5C4'teki eski aktif-provider gösterimini geçersiz kılar.

### Shopify webhooks

Tek canonical ingress: `POST /webhooks/shopify`.

Raw body korunur; HMAC ve topic allowlist doğrulaması JSON işleme ve DB yazısından önce yapılır. Desteklenen topic sınıfları:

- `customers/data_request`
- `customers/redact`
- `shop/redact`
- `app/uninstalled`
- billing entitlement değişimini taşıyan resmî topic — exact API name EO-02'de güncel resmî dokümanla dondurulur

Her teslim idempotent webhook claim üretir. Unknown topic fail-closed olur. Privacy topic'leri UI session veya aktif installation gerektirmez; HMAC + shop/install generation binding kullanır.

## Internal runtime girişleri

| Route | Caller | Kural |
|---|---|---|
| `POST /internal/jobs/hourly-refresh` | Yalnız Vercel Cron/internal signed caller | Merchant/UI çağıramaz; shard + lease + idempotent upsert |
| `GET /healthz` | Platform health probe | Secret, provider/tenant detail veya DB row döndürmez |

Hourly refresh yalnız AdsTable-owned scheduler tarafından başlatılır. Sayfa açılması, browser reload veya birden fazla Shopify kullanıcısı refresh üretmez. İlk bootstrap yalnız yesterday + today; rolling reconciliation/finality EO-05 sözleşmesine tabidir.

## Production yüzeyinden yasaklanan route sınıfları

Hedef build aşağıdakileri route olarak içeremez:

- `/api/e10/*` acceptance/preflight/probe yolları
- `*/runtime/preflight`, `*/runtime/acceptance`
- historical inventory ve manual historical fetch
- controlled reset, journey diagnostic, flow-event inventory
- raw provider response/metric discovery/operator selection route'ları
- test, debug, synthetic-data ve secret inspection yolları
- standalone `/login`, `/signup`, `/auth/*`, legacy `/dashboard`
- browser-triggered manual refresh
- generic proxy veya caller-supplied URL/tenant route'u

Gerekli tanılama ayrı operator aracı/CI evidence olarak çalışır; production product router'a bağlanmaz.

## Journey, state ve evidence kapıları

| Journey | Başarılı sonuç | Zorunlu görünür state | Done evidence |
|---|---|---|---|
| Install/session | Workspace-bound installation generation | loading, auth failure, forbidden | desktop/mobile embedded session + tamper negative |
| Trial/billing | 14 günlük trial ve Shopify-authoritative entitlement | trial, active, expired, billing_error | billing webhook/API reconciliation; EO-02 |
| Currency | Merchant-selected reporting currency | required, saved, invalid | server read-back + audit |
| Connect/reconnect | Canonical encrypted provider grant | loading, cancel, error, reauthorization_required | provider identity/read probe; plaintext zero |
| Account selection | Tek verified reporting account | empty, partial, forbidden, error | read-back + ownership |
| Klaviyo cost | Effective-date email cost history | required, active, validation_error | versioned history + formula allocation |
| Hourly refresh | Lease-owned idempotent run | queued/running/partial/stale/final | run/checkpoint/reconciliation evidence |
| Funnel/Dashboard | Truthful supported/unsupported/unknown facts | loading, empty, partial, stale, reauth, error | API/UI parity, desktop/mobile |
| Ad Analysis | Kanıtlı leaf grain | same states + unavailable dimensions | provider raw/normalized parity |
| Attribution Differences | Read-only comparison | capability_unavailable dahil | scope/API capability + non-mutating proof |
| Delete My Data | Durable request and terminal manifest | confirm, queued, running, complete, failed | idempotency + deletion manifest |
| Uninstall | Access/schedule stop; deletion değil | UI erişimi yok | webhook claim + stopped leases |
| Clean reinstall | Yeni generation; eski deletion job canlanmaz | onboarding yeniden başlar | generation isolation + 48h race tests |

Eksik veri sıfıra çevrilmez. `unsupported`, `unknown`, `partial`, `stale`, `reauthorization_required` ve `capability_unavailable` ayrı product truth state'leridir.

## UI Constitution eşlemesi

Bu paket UI kodu yazmaz. EO-07 uygulamasından önce her ekran için analist brief, state/copy matrisi ve exact Shopify component/property/variant eşlemesi hazırlanacaktır.

Dondurulan shell yönü:

- App navigation: resmî App Bridge/Polaris `s-app-nav` / `s-link`
- Page shell: `s-page`
- Birincil/ikincil eylemler: `s-button`
- Form alanları: resmî Polaris web components
- Banner/notice/badge/table/empty state: yalnız güncel resmî karşılık doğrulanırsa
- raw HTML action control, `s-clickable` button taklidi, inline CSS, literal renk, custom card/badge/modal yasak

Mevcut development contract'ındaki `polaris-2.0-rc.js` kabulü production için otomatik değildir. EO-07 başladığında güncel stabil/RC durumu resmî dokümandan yeniden doğrulanır. Desktop ve gerçek Shopify Admin mobile 320px kanıtı ile açık ürün sahibi kabulü olmadan hiçbir ekran Done/merge olamaz.

## DB consumer map

| Product/runtime consumer | Okuma | Yazma |
|---|---|---|
| Session/onboarding guard | `shopify.installations`, `app.*`, `billing.*`, `integrations.provider_connections` | bootstrap transaction dışında yok |
| Settings | `app.workspace_settings`, `billing.*`, `privacy.*` | currency, billing intent, deletion request |
| Platforms | `integrations.*`, `analytics.email_spend_history` | OAuth transaction, connection/account binding, spend history |
| Reports | `analytics.*`, `operations.reconciliation_ledger` | yok |
| Hourly refresh | `integrations.*`, `app.workspace_settings` | `analytics.*`, `operations.*` |
| Webhooks/lifecycle | `shopify.installations` | `privacy.*`, `billing.*`, installation state |
| Migration/cutover | restricted source manifests | allowlisted EO-F4 carry only |

UI, browser ve App Bridge hiçbir schema'ya doğrudan bağlanmaz. BFF caller-supplied workspace kabul etmez; workspace Shopify session'dan server-side türetilir.

## Önceki contract'larla çatışma çözümü

- E10-T5C7'deki mantıksal sıra korunur; Shopify'ın resmî home davranışı nedeniyle Funnel root/app-name home olur ve App Nav'da yinelenmez.
- E10-T5C4'te TikTok active görünümü geçersizdir; güncel hedef provider listesi Meta, Google Ads ve Klaviyo'dur.
- Mevcut App Home'daki Dashboard/Funnel/Analysis/Settings nav'ı hedef değildir; Attribution Differences, Platforms ve doğru Ad Analysis ayrımı eklenir.
- `/analysis` hedefte `/ad-analysis` olur.
- Attribution Differences route'u capability kanıtı yoksa boş/zero göstermez; açık `capability_unavailable` state'i verir.
- Billing ayrı global nav değildir; Settings içindeki plan/trial alanıdır.
- Manual Refresh yoktur; kullanıcı yalnız freshness/run status görür.

## Kabul sonucu

EO-F5 **PASS**:

- canonical embedded UI route: 6
- canonical report surface: 4
- active provider: 3
- standalone UI/auth route: 0
- production debug/operator/acceptance route: 0
- browser-triggered refresh route: 0
- direct browser DB access: 0
- implementation/production/provider/DB/deployment mutation: 0
- sıradaki kapı: **EO-F6 — Repair vs re-establishment effort and risk comparison**

Bu paket EO-F7 GO vermez, yeni repository/project oluşturmaz, Shopify app URL'sini değiştirmez, webhook kaydetmez ve mevcut runtime'ı kapatmaz.
