# A6-EO-00 — Embedded-only Yeniden Kuruluş Kararı ve Fizibilite Kapısı

**Tarih:** 4 Ekim 2026  
**Durum:** Stratejik yön donduruldu; uygulama GO kararı fizibilite kapılarına bağlı  
**Yetki sınırı:** Bu paket production, provider, veritabanı, deployment, Shopify configuration veya mevcut runtime'ı değiştirmez.

## İş çıktısı

AdsTable'ın Shopify embedded ürünü, mevcut monoliti onarmaya devam etmek yerine temiz ve ayrı bir deployable runtime sınırında yeniden kurulacaktır. Çalışan embedded yetenekler, canlı veriler ve kabul edilmiş sözleşmeler körlemesine kopyalanmayacak; yalnız kanıtlı bir allowlist ile taşınacaktır.

**İş değeri:** GA4, Google Sheets, TikTok, Pinterest, standalone kullanıcı/OAuth, legacy dashboard ve kök monolit gibi emekli edilecek yüzeylerin yeni üründe gizli bağımlılık veya tekrar etkinleşme riski ortadan kaldırılır. Bunun karşılığında yeniden kuruluşun OAuth, veri, billing, lifecycle ve cutover riskleri parity ve rollback kapılarıyla kontrol altında tutulur.

## Bağlayıcı karar

1. Hedef ürün yalnız Shopify embedded olacaktır.
2. Hedef provider dilimi yalnız Meta, Google Ads ve Klaviyo'dur.
3. Hedef uygulama, mevcut monolitten ayrı bir deployable runtime sınırına sahip olacaktır.
4. Yeni runtime'a kod toplu olarak kopyalanmaz. Her taşınan modül, şema ve sözleşme allowlist girdisi, sahiplik gerekçesi ve kabul kanıtı taşır.
5. Mevcut production runtime, yeni runtime review-ready ve cutover-ready olana kadar containment/rollback hattıdır; yeni ürün geliştirme zemini değildir.
6. Big-bang cutover, erken veri silme ve kanıtsız legacy retirement yasaktır.
7. Yeni repository mi yoksa aynı repository içinde bağımsız deploy root mu kullanılacağı EO-F2 kanıtı ile karara bağlanır. Her iki halde build ve dependency sınırı fail-closed olacaktır.
8. Uygulama GO kararı bu belgenin fizibilite kapıları tamamlanmadan verilemez.

## Execution Plan anayasa değişikliği

V4 §1 madde 1'deki “Proje baştan yazılmayacaktır” hükmü genel bir big-bang rewrite yasağı olarak korunur; ancak Shopify embedded hedef runtime'ın **allowlist tabanlı kontrollü yeniden kuruluşu** bu hükmün versionlı istisnasıdır.

V4 §1 madde 2 ve §1.2'nin parity/rollback ilkeleri aynen korunur:

- mevcut hat erken kapatılmaz;
- davranış characterization ve contract testleriyle taşınır;
- canlı veri körlemesine kopyalanmaz;
- parity ve rollback kanıtlanmadan authority cutover yapılmaz;
- consumer-zero kanıtlanmadan legacy silinmez.

Bu karar audit bulgularını kapatmaz. Bulgular yeni package map'e taşınır ve kabul kapısı olarak yaşamaya devam eder.

## Hedef mimari zinciri

```text
Shopify installation/session
  -> workspace + entitlement + reporting currency
  -> canonical provider connection/token vault
  -> Meta | Google Ads | Klaviyo adapters
  -> AdsTable-owned hourly scheduler
  -> Dataset V2 + provenance + maturity/finality
  -> Formula / Query / Funnel API
  -> Shopify App Bridge + Polaris web components UI
```

## Taşıma allowlist'i

Aşağıdaki varlıklar otomatik olarak değil, EO-F1–EO-F6 kanıtlarından sonra taşınabilir:

- Onaylı Execution Plan kararları, executable contracts ve acceptance evidence
- Shopify installation, workspace, entitlement ve workspace settings authority'si
- Kullanıcının seçtiği workspace reporting currency
- Workspace-scoped canonical provider connections ve şifreli token vault
- Meta, Google Ads ve Klaviyo'nun kanıtlı OAuth/token lifecycle, client, mapper ve Dataset V2 writer davranışları
- Dataset V2 workspace authority, schema invariants ve provider provenance
- Kanıtlı Formula/Query kuralları: gerçek 0, unknown/unsupported, aggregate-first oranlar, currency ve null-reason semantiği
- Shopify Embedded UI Constitution ve yalnız App Bridge/Polaris web component yaklaşımı
- Billing/trial, privacy, deletion, scheduler, reconciliation, cutover ve rollback sözleşmeleri; yalnız uygulaması yeniden doğrulanarak

## Yeni runtime'a taşınmayacaklar

- Standalone kullanıcı authority'si ve standalone login/signup/dashboard ürünü
- `platform_connections` / `platform_connection_tokens` üzerinden yeni OAuth, reconnect veya refresh authority'si
- Kök `server.js` monoliti
- Legacy dashboard/snapshot V1 write ve read authority'si
- GA4, Google Sheets, TikTok, Pinterest ve Organic provider runtime'ları
- Debug, test, operator ve acceptance route'larının production yüzeyi
- Raw HTML ile Shopify kontrolü taklit eden UI, inline CSS ve Shopify görünümünü kopyalayan özel stil
- Tarihsel/synthetic/ambiguous verinin kanıtsız Dataset V2 backfill'i
- Eski dependency ağacının veya public klasörünün toplu kopyası

## Canlı veri ve geçmiş veri kuralı

Yeni runtime ile yeni repository kavramı, yeni veya boş veritabanı anlamına gelmez.

- Canlı Shopify installation/workspace, billing ve canonical provider bağlantıları için versionlı carry map hazırlanır.
- Şema yerinde kullanılacaksa yeni runtime yalnız allowlist tablolarına erişir.
- Veri migrate edilecekse her tablo için source, target, tenant key, dönüşüm, row-count/hash, provenance ve rollback kanıtı gerekir.
- V1/history verisi yalnız doğrulanmış binding + provider re-fetch veya canonical validation ile taşınır.
- Eski runtime başlangıçta read-only/containment durumunda kalır.
- Silme ancak restore point + read-disable gözlem penceresi + consumer-zero + insan onayı sonrası yapılabilir.

## RM, R ve E paketlerine etkisi

| Mevcut risk/paket | Yeni programdaki hüküm |
|---|---|
| A6-RM-00 containment | Aynen yürür; EO programı HOLD kapılarını kaldırmaz. |
| A6-RM-01 DB/crypto | Canlı güvenlik kazanımları korunur; startup fail-closed guard yeni runtime'ın ilk kapısıdır. |
| A6-RM-02 truthful legacy adapter | Eski runtime için geçici containment olarak küçülür; yeni runtime'da false-zero baştan yasaktır. |
| A6-RM-03/04/05 lifecycle, deletion, clean reinstall | Aynen zorunlu; yeni foundation içinde uygulanır. |
| A6-RM-06 scheduler/control plane | Aynen zorunlu; saatlik SnapshotJob'un tek authority'si olur. |
| A6-RM-07 maturity/reconciliation/finality | Aynen zorunlu; provider verisi final olmadan müşteriye final gösterilmez. |
| A6-RM-08 formula semantics | Aynen zorunlu; API ve UI'dan önce tamamlanır. |
| A6-RM-09 secure truthful API/UI | Aynen zorunlu; Shopify-native UI bunun üzerine kurulur. |
| A6-RM-10 cutover/rollback | Aynen zorunlu; EO parity/canary/cutover kapısıdır. |
| A6-RM-11 consumer-zero/retirement | EO cutover sonrasında legacy archive ve söküm kapısıdır. |
| A6-RM-12 adapter readiness | Shopify review kritik yolunda değildir; deferred kalır. |
| R3/R4/R6/R7 | Kanıtlı workspace/Dataset/provider/UI kararları carry allowlist girdileridir; uygulama yeniden doğrulanır. |
| R8/R9/R10 | EO parity, cutover ve retirement paketleriyle birleşir; kendiliğinden Done sayılmaz. |
| E10-T4/T7 | Lifecycle/privacy ve Shopify billing/trial foundation'a taşınır. |
| E11/E12/E13/E14 | Funnel API, UI, production cutover ve legacy retirement olarak EO sırasına uyarlanır. |
| Legacy refactor/E3 monolit çıkarma işleri | Yeni runtime tarafından gereksiz kılınan kısmı yapılmaz; yalnız containment veya carry keşfi için gereken minimum iş kalır. |

## Fizibilite kapıları

### EO-F1 — Authority ve canlı veri envanteri

Her tenant, installation, billing, provider grant, token, Dataset V2 ve job authority için source-of-truth ile owner belirlenir. Duplicate authority ve restore riski açık kalamaz.

### EO-F2 — Fiziksel proje sınırı kararı

Ayrı GitHub repository ile aynı repository içinde bağımsız deploy root seçenekleri; build isolation, secret scope, CI, Vercel linkage, rollback, history ve yanlış import riski açısından puanlanır. Tavsiye edilen varsayılan **ayrı repository**dir; nihai karar kanıt tablosu ve insan onayı ister.

### EO-F3 — Kod/dependency carry allowlist

Her aday modül için carry-as-is, extract-and-rewrite, contract-only veya retire kararı verilir. Transitive import taraması ve build manifesti yasaklı provider/legacy dependency'yi sıfır göstermelidir.

### EO-F4 — Şema ve veri carry map

Her tablo için keep-in-place, migrate, read-only history, anonymize veya delete-later kararı; tenant key, RLS/grant, migration, reconciliation ve rollback kuralıyla yazılır.

### EO-F5 — Ürün ve route haritası

Review-critical embedded journey'ler: install, 14 günlük trial/billing, settings/currency, connect/disconnect/reconnect, account selection, hourly refresh, funnel, ad analysis, delete my data, uninstall ve clean reinstall. Her journey'nin API, UI, state ve evidence kapısı tanımlanır.

### EO-F6 — Karşılaştırmalı süre/risk hesabı

“Mevcut monoliti iyileştirme” ve “embedded-only yeniden kuruluş” seçenekleri aynı kapsamla karşılaştırılır: paket sayısı değil kritik yol, P0/P1 kapanışı, cutover riski, geri dönüş, bakım maliyeti ve Shopify review evidence'i ölçülür.

### EO-F7 — GO/NO-GO kararı

F1–F6 tamamlanınca versionlı karar verilir. GO olmadan yeni repository/proje oluşturulmaz, production değiştirilmez ve legacy remediation topluca iptal edilmez.

## Makro uygulama sırası

Bu başlıklar erken mikro-task patlamasını önlemek için kasıtlı olarak makro seviyededir:

1. **A6-EO-00:** Karar ve fizibilite kapısı — bu paket
2. **A6-EO-01:** Temiz runtime shell, CI ve dependency allowlist
3. **A6-EO-02:** Workspace, installation, billing/trial ve privacy lifecycle foundation
4. **A6-EO-03:** Canonical OAuth, connection ve token vault boundary
5. **A6-EO-04:** Meta, Google Ads ve Klaviyo provider adapters
6. **A6-EO-05:** Scheduler, Dataset V2, provenance, maturity ve reconciliation
7. **A6-EO-06:** Formula, Query ve Funnel API
8. **A6-EO-07:** Shopify-native Settings, Funnel ve Ad Analysis UI
9. **A6-EO-08:** Side-by-side parity, canary ve rollback
10. **A6-EO-09:** Authority cutover ve consumer-zero
11. **A6-EO-10:** Legacy archive, veri retention ve kontrollü retirement

Alt paketler yalnız ilgili analist brief'i ve dependency kanıtı oluşunca açılır.

## Başlangıç repository ölçümü

4 Ekim 2026 `main` ağacı üzerinde salt-okunur ölçüm:

- 819 dosya
- 305,537 byte kök `server.js`
- 1,530,539 byte `public/`
- 120 dosya / 561,776 byte `src/`
- 203 test dosyası / 1,018,092 byte `tests/`
- Aktif Vercel manifesti legacy HTML route'larını, kök monoliti, Shopify embedded route'larını ve saatlik cron'u aynı deploy içinde birleştiriyor.
- Provider ağacında hedef üçlü yanında Google Sheets, Organic, Pinterest ve TikTok kodları bulunuyor.

Bu ölçüm yeniden kuruluş lehine bir sinyaldir; tek başına GO kanıtı değildir.

## Stop kuralları

- Provider veya live DB mutation gerekirse ilgili package ve açık insan onayı olmadan durulur.
- Shopify resmi component/property doğrulanamazsa UI kodu yazılmaz.
- Carry adayı hidden import ile yasaklı legacy yüzeye bağlıysa taşınmaz; contract üzerinden temiz implementasyon seçilir.
- OAuth/session, billing, privacy veya data parity kanıtı başarısızsa cutover durur.
- Rollback restore point'i ve consumer-zero yoksa legacy silinmez.
- Yeni yaklaşım beklenen süre/risk avantajını F6'da göstermiyorsa NO-GO verilir.

## Bu pakette yapılmayanlar

- Yeni repository veya Vercel project oluşturulmadı.
- Kod, migration, provider, Supabase, Shopify veya production değiştirilmedi.
- Mevcut runtime kapatılmadı.
- Audit bulguları kapatılmadı.
- Legacy dosya silinmedi.
- Review-ready veya production GO verilmedi.
