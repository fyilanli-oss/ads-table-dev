# A4 — Formula, API ve UI Doğruluk Auditi

**Tarih:** 3 Ekim 2026  
**Kapsam:** E11, E12 ve müşteriye görünen legacy dashboard; formula, support, currency, cost, aggregation, freshness ve sunum  
**Yöntem:** Salt-okunur; Execution Plan → contract → Formula/Query runtime → testler → public UI binding  
**Yetki sınırı:** Kod, deployment, veri, route, feature flag veya UI değiştirilmedi.

## Analist özeti

Canonical ve Formula çekirdeğinde önemli doğru davranışlar vardır:

- gerçek ölçülmüş 0 korunur;
- unsupported metric null olmak zorundadır;
- mixed support additive toplam üretmez;
- oranlar aggregate-first hesaplanır;
- denominator 0 ise oran null olur;
- cross-currency satırlar tek target currency olmadan toplanmaz;
- synthetic satırlar production canonical validation'dan geçmez.

Yerel Phase 1 testinde 53/53, Phase 2 testinde 33/33 PASS alınmıştır.

Buna rağmen müşteriye kadar uzanan zincir güvenli değildir. En ağır bulgu aktif public /dashboard yüzeyindedir: snapshot yoksa kod gerçek bir empty/unknown state üretmek yerine bütün KPI ve journey alanlarını sayısal 0 yapar. API başarısız olduğunda da ekrandaki başlangıç 0 değerleri kalır. Böylece “veri yok”, “henüz çekilmedi”, “unknown” ve “ölçülmüş sıfır” aynı görünür sonuca dönüşebilir.

Yeni E11 ve E12 henüz başlamadığı için bu problem onların başarısız uygulaması değildir. Fakat E11/E12 başlamadan önce kapatılması gereken mevcut P0 sunum borcudur.

## Doğrulanmış pozitif kontroller

### Canonical/support

- supported + finite number kabul edilir.
- gerçek zero supported olarak korunur.
- unsupported veya unknown değer null kalır.
- mixed supported/unknown/unsupported aggregate null + unknown olur; kısmi toplam uydurulmaz.

### Formula

Plan ile uyumlu aritmetik:

- sales = purchase_value
- abandoned = max(checkout - purchase, 0)
- abandoned_value = max(checkout_value - purchase_value, 0)
- ctr = click / impression × 100
- cpc = spend / click
- roas = sales / spend
- cps = spend / purchase
- revenue hesabının aritmetiği mevcut kodda profit adıyla sales - spend olarak vardır.

### Currency

Query Service aynı aggregate içinde birden fazla target currency gördüğünde fail-closed durur. FX, raw monetary alanlara Formula Engine'den önce uygulanır.

## Bulgular

### AF-A4-001 — P0 — Empty/unknown/error legacy dashboard'da ölçülmüş 0'a dönüşüyor

Müşteri etkisi: Müşteri provider Ads Manager ile AdsTable'ı karşılaştırdığında veri henüz gelmediği veya istek başarısız olduğu halde 0 görebilir. Bu, güven kaybı ve yanlış karar doğurur.

Kanıt:

- public /dashboard route'u dashboard.html dosyasını sunar.
- İlk KPI değerleri 0, 0.00 ve 0.00% olarak statik gelir.
- buildEmptySnapshotForCurrentDateScope bütün KPI, journey ve click alanlarını sayısal 0 yapar.
- snapshot:null durumunda bu yapay zero snapshot render edilir.
- access token yoksa, HTTP başarısızsa, payload geçersizse veya exception olursa ayrı error/empty state render edilmez; mevcut 0 değerler ekranda kalır.

Çelişki: Execution Plan “unknown/unsupported hiçbir noktada gerçek 0'a dönüşmez” der. E12 loading, empty, partial, stale ve error state'lerini ayrı ister.

### AF-A4-002 — P1 — Formula output adları frozen sözleşmeyle uyumsuz

Müşteri etkisi: API/UI Revenue beklerken Query Service profit; Revenue Margin beklerken margin üretir. Aynı aritmetik iki farklı iş anlamıyla sunulabilir veya mapping sırasında kaybolabilir.

Kanıt: formula-engine.js primary output olarak profit ve margin döndürüyor; revenue ve revenue_margin yok. Phase testleri de eski isimleri bekliyor.

Plan: canonical ad revenue ve revenue_margin; profit/margin ancak açık versionlı compatibility alias olabilir. Ad Analysis contract margin kolonunu kaldırır.

### AF-A4-003 — P1 — Derived metric support/reason semantiği kayboluyor

Müşteri etkisi: Bir null sonucun nedeni unknown input, unsupported capability, denominator=0 veya empty dataset olabilir. API yalnız null döndürürse UI doğru olarak “—”, “unsupported” veya “not comparable/new” ayrımını yapamaz.

Kanıt:

- Formula Engine ctr/cpc/roas/cps/revenue/margin/rate değerleriyle birlikte derived support veya null_reason döndürmüyor.
- Query meta.metric_support yalnız additive total support map'idir.
- calculateIntentMetrics abandoned null olduğunda doğrudan unknown kabul eder; alttaki unsupported veya empty nedeni korunmaz.

### AF-A4-004 — P1 — Query output freshness, partial, stale ve reconciliation durumunu taşımıyor

Müşteri etkisi: A2'de lineage/finality eksik olan satırlar API'ye bağlandığında UI bunları güncel veya tam sanabilir.

Kanıt: WorkspaceFunnelQueryService meta alanı workspace, scope, formula version, contract version, currency ve raw metric support ile sınırlıdır. source_job_id, data-through, observed-at, provisional/final, partial, stale veya warnings yoktur.

Plan E11-T7 freshness, partial ve warnings metadata'sını; E12-T6 bunların ayrı sunumunu zorunlu tutar.

### AF-A4-005 — P1 — Compare contract uygulanmamış

Müşteri etkisi: Ad Analysis percent-change ranking veya Funnel compare yanlış yerde/frontend'de tekrar hesaplanabilir; previous=0 sonsuz büyüme gibi gösterilebilir.

Kanıt: funnel-core içinde versionlı Compare Engine veya current/previous/absolute/percent output'u yoktur. Query Service tek period döndürür. UI contract compare kolonlarını ve backend hesaplamasını frozen kabul eder.

### AF-A4-006 — P2 — Mevcut Query Service E11 security/API boundary'si değildir

Müşteri etkisi: Yanlışlıkla doğrudan route'a bağlanırsa caller-supplied workspace/account/scope değerleri authority gibi kullanılabilir; query bounds ve pagination yoktur.

Kanıt:

- WorkspaceFunnelQueryService workspace_id parametresi alır; verified Shopify session resolver wrapper'ı değildir.
- platform_account_id ve analysis_scope doğrudan input'tur.
- maksimum tarih aralığı, pagination ve performance budget uygulanmaz.
- /api/funnel/data route'u yoktur.

Bu nedenle Execution Plan'daki E11 Blocked durumu doğrudur. Bu bulgu mevcut bir açık endpoint zafiyeti değil, yanlış reuse'u engelleyen implementation kapısıdır.

### AF-A4-007 — P1 — Shopify sunumu için Paid/Organic/Blend sızıntı riski

Müşteri etkisi: Parked Organic capability veya backend iç analiz scope'u kullanıcıya ürün segmenti olarak açılabilir ve onaylı Shopify ürün sözleşmesi bozulabilir.

Kanıt: Generic Query Service paid, organic ve blend kabul edip meta.analysis_scope döndürür. Shopify E11 contract'ı bu segmentlerin UI response'u olarak üretilmesini yasaklar.

Sınır: Generic capability silinmemelidir; Shopify API adapter'ı yalnız onaylı response sözleşmesini üretmelidir.

## Paket durum gerçeği

| Paket / yüzey | A4 sınıflaması | Gerekçe |
|---|---|---|
| Canonical + aggregate core | partially_verified | zero/null/support, aggregate-first ve currency guard testli |
| Formula Engine v1 | partially_verified | aritmetik doğru; naming ve derived support eksik |
| Workspace Query Service | partially_verified_as_internal_artifact | tenant result check var; API security, compare, freshness, bounds yok |
| E11 Funnel API | verified_as_blocked | route yok; başlamamış olması planla uyumlu |
| E12 Embedded Funnel UI | verified_as_not_started | implementation yok; freeze contract'ları mevcut |
| Legacy /dashboard | contradicted | empty/error/unknown sayısal 0 olarak görünebilir |

## Güvenli remediation bağımlılık sırası önerisi

Bu audit implementation izni vermez. A6 için önerilen sıra:

1. Legacy /dashboard'da default zero yerine loading; snapshot:null için empty; fetch failure için error; stale/partial için ayrı state.
2. Formula v2 veya versionlı compatibility layer: revenue/revenue_margin canonical adları.
3. Her derived metric için support ve null_reason contract'ı.
4. Provider/run/finality freshness metadata'sını A2 provenance tasarımından API'ye taşıma.
5. Versionlı Compare Engine ve previous=0 not-comparable semantiği.
6. Shopify-session-derived workspace/account authority, query bounds ve pagination.
7. Shopify-specific presentation DTO; Paid/Organic/Blend iç capability'sini kullanıcı yüzeyinden ayırma.
8. Aynı golden fixture'ın Formula → API → Compare → Export → UI parity testi.

## Sonuç

Business math çekirdeği tamamen yanlış değildir; aksine birçok önemli guard çalışmaktadır. Ölüm fermanı riski, bu doğru çekirdeğin müşteriye ulaşmadan önce empty/error bilgisini 0'a çeviren legacy sunum ve derived sonuçların anlamını taşıyamayan API taslağıdır.

A4 bir P0, beş P1 ve bir P2 bulgu kaydetmiştir. Hiçbir remediation uygulanmamıştır.
