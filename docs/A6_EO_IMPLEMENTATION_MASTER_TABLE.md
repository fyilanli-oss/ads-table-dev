# A6 EO — Tek Hat Yürütme Kuralı ve Bütünleşik İş Paketleri Ana Tablosu

**Tarih:** 4 Ekim 2026  
**Durum:** EO ana yürütme baseline'ı  
**Otorite:** Execution Plan + EO executable contract  
**Amaç:** Dallanıp budaklanan paralel paket ağını engellemek ve bütün yeni uygulama işlerini tek EO hattında yürütmek.

## Analist sonucu

Bundan sonra AdsTable geliştirmesinde tek aktif ürün hattı **A6-EO** olacaktır.

E, R ve A6-RM paketleri yeniden çalıştırılacak paralel backlog değildir. Bunlar tarihsel karar, gereksinim, risk ve evidence referans havuzudur. Yeni uygulama maddeleri eski paketlerin kopyası olarak değil, temiz embedded ürünün kendi mimarisine ve müşteri yolculuğuna göre EO paketlerinde tanımlanır.

Bu ana tablo merge edilmeden EO-01 teknik kurulumu başlamaz.

## Değiştirilemez yürütme kuralları

1. Aynı anda yalnız bir EO parent paket `In progress` olabilir.
2. Parent sıra `EO-01 → EO-10`dur; downstream paket upstream kabulü olmadan başlayamaz.
3. E, R veya A6-RM altında yeni implementation paketi açılmaz.
4. Eski paketler yalnız ilgili EO satırının `references` alanında kullanılır.
5. Bir EO paketi eski bir yükümlülüğü ancak exact acceptance evidence ve açık closure kaydıyla kapatabilir; “kapsama aldık” Done değildir.
6. Production'ı korumak için zorunlu acil containment gerekirse EO dışı geliştirme sayılmaz; ayrı, minimum, geri alınabilir ve açık insan kararıyla yapılır.
7. Parent altındaki stable child ID'ler en fazla tek seviyedir: `EO-04-A`, `EO-04-B` gibi. Varsayılan olarak `EO-04-B-1-C` türü ikinci/üçüncü seviye paket açılmaz.
8. Child içindeki teknik adımlar checklist/test olarak tutulur; yeni package kodu verilmez.
9. Yeni bulgu önce mevcut aktif EO parent risk kaydına yazılır. Doğası başka parent'a aitse o parent'ın `pending_findings` alanına aktarılır; yeni paralel harf dizisi açılmaz.
10. Her parent başlamadan önce analist brief verilir: iş çıktısı, iş değeri, kapsam, kapsam dışı, bağımlılık, canlı etki, kabul ve rollback.
11. Her parent bittiğinde dört sonuç birlikte yazılır:
    - üretilen gerçek çıktı;
    - geçen acceptance/evidence;
    - reference paketlerden kapanan/açık kalan yükümlülükler;
    - sıradaki tek parent.
12. Kod yazılması, PR merge'i veya deployment tek başına Done değildir.
13. Merge her zaman açık kullanıcı onayı ister.
14. Production, provider, data carry, cutover ve destructive retirement kendi exact insan kapılarını korur.

## Durum sözlüğü

- **Ready:** Upstream ve governance kapıları tamam; başlanabilir.
- **Not started:** Sırası gelmedi.
- **In progress:** Tek aktif parent.
- **Verification:** Implementation hazır; evidence/insan kabulü bekleniyor.
- **Done:** Contract, CI, canlı/merchant evidence ve gerekli insan kabulü tamam.
- **Blocked:** Belgelenmiş dış bağımlılık olmadan ilerlenemiyor.
- **Post-review:** Review tesliminden sonra yürütülür; erken başlatılmaz.

## Ana iş paketleri tablosu

| Parent | Doğal amacı | Faz | Başlangıç bağımlılığı | Stable child sayısı | Durum |
|---|---|---|---|---:|---|
| EO-01 | Temiz runtime/repository/CI sınırı | Review-critical | EO-F7 + bu master | 3 | Ready |
| EO-02 | Workspace/install/billing/privacy foundation | Review-critical | EO-01 | 4 | Not started |
| EO-03 | OAuth/connection/token authority | Review-critical | EO-02 | 4 | Not started |
| EO-04 | Üç provider adapterı | Review-critical | EO-03 | 5 | Not started |
| EO-05 | Scheduler/Dataset/finality | Review-critical | EO-04 | 5 | Not started |
| EO-06 | Formula/Query/BFF API | Review-critical | EO-05 | 5 | Not started |
| EO-07 | Üç yüzeyli Shopify UI | Review-critical | EO-06 | 6 | Not started |
| EO-08 | Carry/parity/canary/rollback | Review-critical | EO-07 | 4 | Not started |
| EO-09 | Cutover/stabilization/consumer-zero | Post-review/cutover | EO-08 + ayrı cutover GO | 3 | Not started |
| EO-10 | Archive/retention/retirement | Post-consumer-zero | EO-09 + destructive approval | 4 | Not started |

Toplam: **10 parent, 43 stable child**. Bu sayı hedef plan baseline'ıdır; teknik checklist'ler paket sayısını artırmaz.

## EO-01 — Clean runtime shell, CI and dependency boundary

- **A — Repository and physical boundary:** Ayrı clean repository; fork/toplu kopya yok.
- **B — Official stack, manifest and CI:** Güncel resmî Shopify stack, temiz dependency manifesti, forbidden import/route/table/env testleri.
- **C — Three-route truthful preview shell:** `/`, `/ad-analysis`, `/settings`; preview-only ve veri iddiası yok.

Çıktı: clean repository + preview shell.  
Referans: A6-RM-01, E3 mimari dersleri, Shopify Embedded UI Constitution.

## EO-02 — Workspace, installation, billing/trial and privacy foundations

- **A — Private schemas, roles and migrations**
- **B — Workspace, installation and generation authority**
- **C — Fourteen-day trial, subscription and entitlement**
- **D — Privacy, uninstall, deletion and clean reinstall**

Çıktı: clean data foundation ve tam merchant lifecycle.  
Referans: A6-RM-03/04/05, E10-T4/T7, R2.

## EO-03 — Canonical OAuth, connection and token vault

- **A — OAuth transaction boundary**
- **B — Token envelope and startup guard**
- **C — Connection and reporting-account authority**
- **D — Reconnect, disconnect and renewal lifecycle**

Çıktı: tek provider authority ve güvenli token lifecycle.  
Referans: A6-RM-01, R0/R5/R6, E7.

## EO-04 — Meta, Google Ads and Klaviyo adapters

- **A — Common adapter contract**
- **B — Meta adapter**
- **C — Google Ads adapter**
- **D — Klaviyo adapter ve 15 Ekim resmî revalidation**
- **E — Integrated three-provider acceptance**

Çıktı: raw evidence + normalize edilmiş truthful provider facts.  
Referans: R6/R7/R7-B5 ve deepest-grain discovery girdileri.

## EO-05 — Scheduler, Dataset V2, FX, maturity and reconciliation

- **A — Hourly scheduler, shard, lease and checkpoint**
- **B — Dataset V2, FX and provenance**
- **C — Maturity, attribution windows and finality**
- **D — Yesterday+today bootstrap, idempotent upsert and reconciliation**
- **E — Operational observability**

Çıktı: AdsTable-owned saatlik ve düzeltilebilir canonical data plane.  
Referans: A6-RM-06/07, E9-T8, R3/R4/R7.

## EO-06 — Formula, Query and same-origin BFF/API

- **A — Formula engine**
- **B — Query, filters and comparison semantics**
- **C — BFF routes and DTO envelope**
- **D — Support/null/freshness semantics**
- **E — Contract and security acceptance**

Çıktı: missing/unknown'u sıfır yapmayan truthful API.  
Referans: A6-RM-08/09, E11 ve Dataset V2 sözleşmeleri.

## EO-07 — Shopify-native three-surface UI

- **A — Settings**
- **B — Funnel App Home**
- **C — Ad Analysis**
- **D — Cross-platform deepest-grain discovery**
- **E — Attribution Differences nested view; yalnız D geçerse**
- **F — Desktop, gerçek mobile ve accessibility acceptance**

Çıktı: Funnel, Ad Analysis ve Settings. Dashboard grafikleri bağlamsal; Platforms Settings içinde.  
Referans: A6-RM-09, E10-T5, E12 ve UI Constitution.

## EO-08 — Carry rehearsal, parity, canary and rollback

- **A — Restricted data/sealed-token carry rehearsal**
- **B — Provider, Dataset, API ve UI parity**
- **C — Failure injection and rollback rehearsal**
- **D — Review evidence and explicit review-ready decision**

Çıktı: Shopify review'a sunulabilir olduğumuzu kanıtlayan paket.  
Referans: A6-RM-10, R8/R9, E13.

## EO-09 — Authority cutover stabilization and consumer-zero

- **A — Production cutover plan and freeze**
- **B — Canary authority switch**
- **C — Stabilization and consumer-zero observation**

Çıktı: ayrı GO ile kontrollü production authority geçişi.  
Referans: A6-RM-10/11, R9, E13.

## EO-10 — Legacy archive, retention and controlled retirement

- **A — Archive and retention manifest**
- **B — Legacy route/deployment/environment retirement**
- **C — Legacy database retirement**
- **D — Final restore and closure evidence**

Çıktı: geri alınabilir archive ve kanıtlı legacy retirement.  
Referans: A6-RM-11, E14, R10.

## Referans yükümlülüklerinin kullanımı

Eski paketler için üç durum vardır:

- **Referenced:** EO tasarımının girdisidir; henüz kapanmamıştır.
- **Satisfied by EO evidence:** Exact EO acceptance eski yükümlülüğü karşılamıştır.
- **Superseded for implementation, retained historically:** Eski implementation yolu kullanılmaz; karar/evidence geçmişi korunur.

“EO başladı, eski paketler otomatik kapandı” ifadesi yasaktır.

## Açık legacy yükümlülük köprüsü

Bu iki Klaviyo maddesi EO başladı diye kapanmaz ve yalnız referans verilerek arafta bırakılamaz:

| Açık yükümlülük | Bugünkü kesin durum | EO sahipliği | Nihai kapanış sahibi |
|---|---|---|---|
| R6-D5-K3 — Flow/journey count-value ve persistence canlı kabulü | **Açık.** Beş günlük attribution/data-maturity beklemesi kabul kanıtı değildir. Non-fabricated Flow satırı, truthful `Added to Cart` / `Started Checkout` / `Placed Order` count-value support durumu ve Dataset V2 persistence/read-back kanıtı eksiktir. | EO-04-D provider facts/support; EO-05-D idempotent persistence/read-back | EO-05-D |
| R7-B5 — Estimated 30-Day Email Spend runtime/migration/live acceptance | **Açık.** Repository C1/C2/C3 hazırlığı vardır; production migration, gerçek runtime dağıtımı, no-double-count/currency provenance, merchant Settings ve canlı Dataset/API/UI parity kabulü tamamlanmamıştır. | EO-02-A schema/migration; EO-04-D input semantics; EO-05-B Dataset/provenance; EO-06-A formula; EO-07-A Settings; EO-08-B live parity | EO-08-B |

Kapanış kuralları:

- K3, yalnız Flow verisinin görülmesiyle kapanmaz; provider gerçekliği ile kontrollü Dataset V2 persistence/read-back birlikte geçmelidir.
- Beş günlük pencerenin dolması, boş/zero sonuç veya zaman geçmesi acceptance değildir.
- R7-B5, migration ya da UI tek başına geçince kapanmaz; bütün maliyet zinciri ve canlı parity birlikte kanıtlanmalıdır.
- Her iki kayıt da exact evidence ve açık closure kaydı oluşana kadar **Open** kalır.

## Yeni bulgu yönlendirme kuralı

| Yeni bulgu doğası | Gideceği EO parent |
|---|---|
| Repository, dependency, build, Shopify stack | EO-01 |
| Workspace, install, billing, privacy, deletion | EO-02 |
| OAuth, token, account ownership | EO-03 |
| Provider request/normalization/hierarchy | EO-04 |
| Scheduler, Dataset, FX, finality | EO-05 |
| Formula, query, API semantics | EO-06 |
| UX, component, mobile, three-surface behavior | EO-07 |
| Migration, parity, canary, rollback | EO-08 |
| Cutover ve consumer-zero | EO-09 |
| Archive/retirement | EO-10 |

Bulgu ilgili parent'ın sırası gelene kadar backlog satırı olarak kalır; yeni paralel paket ailesi oluşturmaz.

## Review sınırı

EO-01–EO-08 review-critical hattır. EO-08-D sonunda ayrıca açık insan review-ready kararı gerekir.

EO-09 ve EO-10 review sonrasında yürütülür. Bununla birlikte review sonrasında yapılmaları, erken legacy deletion yetkisi vermez; consumer-zero ve destructive approval korunur.

## İlk aktif iş

Bu master merge edildikten sonra tek aktif parent **EO-01** olacaktır.

EO-01 başlamadan önce verilecek analist brief:

- hangi resmî Shopify stack ve neden;
- repository/Vercel provisioning yolu;
- dependency allowlist;
- CI/negative test seti;
- üç route'un yalnız shell kapsamı;
- production/Shopify configuration mutation sayısının neden sıfır kaldığı;
- rollback.

Başka hiçbir EO parent paralel başlatılmaz.
