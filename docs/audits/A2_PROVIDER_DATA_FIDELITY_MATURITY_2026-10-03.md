# A2 — Provider Veri Doğruluğu ve Olgunluk Auditi

**Tarih:** 3 Ekim 2026  
**Kapsam:** E4, E5, E6, E7, E8, E9, R6 ve R8  
**Yöntem:** Salt-okunur; Execution Plan → contract → runtime → canlı şema/veri → resmî provider semantiği  
**Yetki sınırı:** Bu belge kod değişikliği, migration, deploy, scheduler aktivasyonu, provider mutation, veri silme veya remediation yetkisi vermez.

## Analist özeti

Mevcut provider çalışmaları tamamen boşa gitmiş değildir:

- Meta için gerçek ve non-empty bir Ad satırı Dataset V2'ye yazılmıştır.
- Klaviyo için Campaign Message ve Flow Message seviyelerinde dört gerçek satır vardır.
- Google Ads canlı ham cevabı seçili structure alanlarını kabul etmiş, fakat results döndürmemiştir; Dataset V2'ye sahte satır yazılmamıştır.
- Unsupported veya unknown dönüşüm alanları sahte 0 yapılmamıştır.

Ancak bu satırlar sürdürülebilir üretim hattının kanıtı değildir. Canlı Dataset V2'deki beş satırın tamamında source_job_id boştur. Canonical workspace-scoped saatlik scheduler/job zinciri ve fiziksel checkpoint henüz yoktur. Dataset V2 ayrıca satırın provisional/final durumunu veya hangi provider düzeltmesiyle değiştiğini açıkça taşımamaktadır.

A2 sonucu: **tekil provider kabul kanıtları vardır; fakat müşteriye sürekli, izlenebilir ve provider düzeltmelerine uyumlu veri üretme kabiliyeti henüz doğrulanmamıştır.**

## Canlı bulguların özeti

### Dataset V2

Canlı tabloda toplam beş satır vardır:

| Provider | Tarih / grain | Adet | Canlı anlam |
|---|---|---:|---|
| Klaviyo | 2026-09-26 Campaign Message | 1 | gerçek kabul satırı |
| Klaviyo | 2026-09-28 Flow Message | 1 | gerçek kabul satırı |
| Klaviyo | 2026-10-02 Campaign Message | 1 | provisional kabul satırı |
| Klaviyo | 2026-10-02 Flow Message | 1 | provisional kabul satırı |
| Meta | 2026-10-01 Ad | 1 | gerçek non-empty hierarchy/metrik satırı |

Beş satırın ortak durumu:

- synthetic=false
- source_job_id boş
- metric_support dolu
- ham referans yalnız sınırlı özet alanları taşıyor; provider request/response kimliği, attribution ayarı, payload hash'i, gözlem zamanı ve revision/finality kimliği yok.

### Schedule, job ve checkpoint

Canlı legacy snapshot_schedules kayıtları kullanıcı-scoped, active=false ve interval_minutes=240 durumundadır. Bu durum R4-C ile eski hattın bilinçli şekilde dondurulmasıyla uyumludur; bu kayıtların yeniden açılması çözüm değildir.

Asıl boşluk:

- server-controlled, workspace-scoped, saatlik canonical schedule/job zinciri canlıda bulunmuyor;
- mevcut snapshot_jobs legacy user-scoped yapıdır;
- backfill_checkpoints fiziksel olarak bulunmuyor;
- kabul satırları bir otomasyon job'una bağlanamıyor.

### Runtime politikası

Repository'deki onboarding scope yalnız yesterday=finalized ve today=provisional ayrımı yapar. Legacy runtime ise dört saatlik platform saatleri, her çalışmada today ve günde bir kez last_7d recovery kullanır; ayrıca provider ayrımı olmaksızın üç saatlik maturity varsayımı taşır.

Bu legacy politika yeni saatlik workspace-first kararı değildir ve provider'ların farklı düzeltme/attribution davranışlarını temsil etmez.

## Provider bazında sonuç

### Meta

Doğrulanan:

- actions içinde landing page view, link click ve engagement türleri gerçek değerlerle geldi.
- action_values boş dizi geldi.
- add-to-cart, checkout ve purchase action türleri response içinde hiç bulunmadı.
- Canonical satırda bu dönüşümler unknown/null bırakıldı; sahte 0 üretilmedi.

Eksik:

- kullanılan attribution/report-time/unified-attribution ayarının satır veya run provenance'ında kalıcı kanıtı yok;
- ileride geç gelen dönüşüm veya attribution düzeltmesinin hangi tarihleri yeniden uzlaştıracağına ilişkin canlı canonical job politikası yok.

Meta'nın resmî Marketing API örnekleri Insights isteğinde actions, action_values, attribution window ve unified attribution parametrelerini ayrı parametreler olarak ele alır. Response yokluğu ile “provider bu metriği desteklemiyor” sonucu aynı şey değildir; mevcut unknown davranışı doğrudur.

Resmî kaynak: [Meta Marketing API — Attribution Setting örneği](https://www.postman.com/meta/facebook-marketing-api/request/5wdl62t/attributionsetting)

### Google Ads

Doğrulanan:

- Üç hesapta Standard structure sorgusu canlı SearchStream tarafından kabul edildi.
- fieldMask, adGroupAd.ad.name dahil seçilen alanları doğruladı.
- response body'lerde results bulunmadı.
- Dataset V2'ye sıfır satır yazıldı; sahte entity/metrik oluşturulmadı.

Eksik:

- non-empty metric acceptance yok;
- Google'ın gecikmeli conversion ve conversion adjustment davranışına uygun provider-specific reconciliation planı canlı runtime'da yok.

Google Ads resmî dokümantasyonu conversion verisinin anlık olmadığını, tüm metrikleri sıfır olan satırların dönmeyebileceğini ve conversion adjustment'ların sonradan retract/restatement yapabildiğini belirtir. Lag segmentleri çok daha uzun süreleri modelleyebilir; bu nedenle evrensel üç saat veya yalnız yedi günlük tekrar çekim kuralı Google finality sözleşmesi olamaz.

Resmî kaynaklar:

- [Google Ads — Conversion reporting](https://developers.google.com/google-ads/api/docs/conversions/reporting)
- [Google Ads — Conversion adjustment upload](https://developers.google.com/google-ads/api/docs/conversions/upload-adjustments)
- [Google Ads — Segment fields](https://developers.google.com/google-ads/api/fields/v25/segments)

### Klaviyo

Doğrulanan:

- Campaign Message ve Flow Message leaf seviyesinde gerçek Dataset V2 satırları vardır.
- 2 Ekim Campaign/Flow ve provider-attributed journey count/value kanıtı provisional olarak korunmuştur.
- R6-D5'in final reconciliation beklemesi doğrudur; PASS tamamlanmış sayılmamıştır.

Eksik:

- satırlar bir source job/run kimliğine bağlı değildir;
- configured attribution penceresi, kullanılan conversion metric ve reporting request kimliği canonical provenance'da yoktur;
- final reconciliation'ı otomatik çalıştıracak saatlik workspace job zinciri yoktur.

Klaviyo resmî Reporting API, Campaign/Flow UI ile 1:1 eşleşme için kullanılması gereken yüzeydir. Query Metric Aggregates event-time'a göre gruplandığı için UI send-date raporuyla birebir eşleşmez. Campaign/Flow value sorguları conversion metric bağlamını ayrıca gerektirir. Bu ayrımın run provenance'ında kanıtlanması gerekir.

Resmî kaynaklar:

- [Klaviyo — Reporting API overview](https://developers.klaviyo.com/en/reference/reporting_api_overview)
- [Klaviyo — Query campaign values](https://developers.klaviyo.com/en/reference/query_campaign_values)
- [Klaviyo — Campaigns API overview](https://developers.klaviyo.com/en/reference/campaigns_api_overview)

## Bulgular

### AF-A2-001 — P1 — Canonical saatlik refresh zinciri canlıda yok

Müşteri etkisi: Tekil kabul satırları var olsa bile sonraki saatlerde yeni veri veya provider düzeltmesi otomatik gelmeyebilir; ekran sessizce eski kalabilir.

Kanıt: Legacy schedule'lar pasif/240 dakika/user-scoped; yeni workspace-scoped saatlik scheduler/job kaydı yok.

Sınır: Legacy schedule'ları yeniden açmak çözüm değildir. Ayrı remediation paketi server-controlled workspace scheduler, lease, job lineage, retry ve gözlemlenebilirlik tasarlamalıdır.

### AF-A2-002 — P1, A4'te P0 adayı — Canonical satırlarda run lineage yok

Müşteri etkisi: Bir satırın hangi provider isteğiyle üretildiği, tekrar çekimde neden değiştiği ve hatalı run'dan etkilenip etkilenmediği güvenilir biçimde ispatlanamaz.

Kanıt: Beş canlı satırın tamamında source_job_id boş; raw referans provider request/response/revision kimliği taşımıyor.

Yükseltme kuralı: A4'te UI bu satırları güncel/final diye sunuyorsa müşteri yanlış yönlendirme riski nedeniyle P0'a yükselir.

### AF-A2-003 — P1 — Provider-specific maturity/finality modeli yok

Müşteri etkisi: Klaviyo attribution, Meta attribution ve Google conversion adjustment davranışları aynı kısa pencereye sıkıştırılırsa geçmiş günler eksik veya yanlış kalabilir.

Kanıt: Dataset V2'de explicit maturity/finality alanı yok; legacy runtime universal üç saat varsayıyor.

### AF-A2-004 — P1 — E9 checkpoint/control artefaktı canlı operasyon kontrolü değildir

Müşteri etkisi: Backfill veya reconciliation kesilirse nereden güvenli devam edileceği ve aynı aralığın duplicate üretmeden nasıl tekrar çalışacağı canlıda kanıtlanamaz.

Kanıt: Workspace checkpoint/idempotent code artefaktları repository'de bulunmasına rağmen fiziksel backfill_checkpoints tablosu yoktur. R8 zaten Blocked durumundadır.

### AF-A2-005 — P1 — Tek tip lookback penceresi provider düzeltmelerini kapsamaz

Müşteri etkisi: Provider daha sonra conversion eklediğinde veya düzelttiğinde AdsTable geçmiş tarihi tekrar çekmez ve Ads Manager ile ayrışır.

Kanıt: Legacy today + günlük last_7d yaklaşımı; Google resmî correction/lag semantiği yedi günden uzun olabilir. Meta ve Klaviyo da kendi attribution/reporting bağlamlarına göre ayrı sözleşme ister.

### AF-A2-006 — P2 — Paket durumları tekil acceptance ile sürdürülebilir runtime'ı ayırmıyor

Müşteri etkisi: PASS ifadesi connection/lifecycle, tekil ham response, Dataset write ve sürekli refresh kabiliyetini aynı sanmaya yol açabilir.

Gerekli ayrım: Her provider için en az dört ayrı kapı tutulmalıdır: capability/raw response, hierarchy/mapping, first canonical write, recurring reconciliation/finality.

## Paket durum gerçeği

| Paket | A2 sınıflaması | Gerekçe |
|---|---|---|
| E4 Meta | partially_verified | gerçek non-empty Ad satırı var; recurring reconciliation ve conversion finality yok |
| E5 Google | partially_verified | capability + verified-empty doğru; non-empty metric acceptance yok |
| E6 TikTok | verified_as_parked | yeni activation yok; korunmuş artefaktlar Parked kararına uygun |
| E7 Klaviyo | partially_verified | gerçek Campaign/Flow satırları var; run lineage ve final reconciliation açık |
| E8 Provider access | partially_verified | OAuth/account access uygulanmış; canlı provider acceptance açık |
| E9 Backfill | partially_verified | repository implementation artefaktları var; canlı checkpoint/control ve activation yok |
| R6 Runtime→V2 | partially_verified | provider bazında kabul kanıtları var; reporting completeness ve recurring runtime açık |
| R8 Historical backfill | verified_as_blocked | doğrudan V1 copy yapılmamış; R3/R6 bağımlılıkları ve checkpoint tasarımı açık |

## Güvenli karar sınırı

Bu audit şu anda bir schema tasarımı veya implementation seçmemektedir. A6'da deduplicate edilecek remediation önerisi şu bileşenleri birlikte ele almalıdır:

1. workspace-scoped saatlik scheduler ve job authority;
2. run/request/response/revision provenance;
3. provider-specific lookback, attribution ve finality sözleşmesi;
4. resumable/idempotent checkpoint ve lease;
5. UI/API'nin provisional, stale, reconciled ve final durumlarını dürüst göstermesi;
6. mevcut beş satırın lineage olmadan sessizce final ilan edilmemesi.

## Sonuç

A2'nin ölüm fermanı niteliğindeki risk, provider adapter'ların tamamen yanlış olması değildir. Risk, doğru görünen tekil satırların üretim hattı ve düzeltme geçmişi kanıtlanmadan müşteriye sürekli ve final gerçek gibi sunulabilmesidir.

A2 bu riski kayda almıştır; hiçbir remediation uygulanmamıştır.
