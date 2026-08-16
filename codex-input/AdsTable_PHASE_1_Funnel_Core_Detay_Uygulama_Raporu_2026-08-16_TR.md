# AdsTable — PHASE 1 Detay Uygulama Raporu
## Funnel Core İskeleti / Canonical Contract / Time / FX / Entity / Scope / Formula / Repository / Query Service

**Tarih:** 16 Ağustos 2026  
**Durum:** PHASE 1 uygulama haritası — ana Implementation V3 raporunun detay eki  
**Ana referans:** `AdsTable_Funnel_Core_Audit_Blueprint_Implementation_Plan_FINAL_V3_2026-08-15_TR.md`  
**Destekleyici referans:** `AdsTable_Audit_3_Formula_Engine_Backend_Matematik_TR_V2_Blend_Duzeltildi.md`  
**Kural:** Ana V3 rapor ile eski audit arasında çelişki çıkarsa **V3 nihai karar önceliklidir.** Audit 3 yalnız formül/evidence bağlamı için kullanılır.

---

# 0. Bu belge neden var?

Ana Implementation V3 raporu **hangi sırayla ne yapılacağını** söylüyor. Bu belge ise yalnız **PHASE 1'in içinde gerçekten ne kurulacağını** tarif eder.

Amaç şu soruları uygulama sırasında tekrar sormamaktır:

- `canonical-contract oluştur` tam olarak ne demek?
- Formula Engine hangi metrikleri hesaplayacak?
- Time Service provider API'ye mi gidecek, yoksa yalnız normalize mi edecek?
- FX Service neyi çevirecek, neyi çevirmeyecek?
- Campaign → AdGroup → Ad ilişkisi nerede korunacak?
- PMax ve Klaviyo aynı contract'a nasıl oturacak?
- Paid / Organic / Blend seçimi hangi modülün sorumluluğu olacak?
- Dataset Repository Phase 1'de Supabase'e gerçekten yazacak mı?
- Funnel Query Service Phase 1'de endpoint mi açacak, yoksa yalnız servis sınırını mı kuracak?
- Phase 1 ne zaman gerçekten tamamlanmış sayılacak?

Bu nedenle bu belge **kod yazma sırasını, sorumluluk sınırlarını, input/output contract'larını, kabul kriterlerini ve dokunulmayacak alanları** tek yerde freeze eder.

---

# 1. PHASE 1'in tek cümlelik amacı

> **Platformlardan bağımsız, gerçek provider verisi bağlanmadan da deterministic olarak çalışabilen yeni Funnel Core omurgasını kurmak; daha sonraki Meta/Google/TikTok/Klaviyo/GA4 adapter'larının aynı canonical contract'a bağlanabileceği temiz sınırları hazırlamak.**

PHASE 1 sonunda henüz gerçek yeni Dataset tablosu production'a bağlanmış olmayacak ve Funnel mock kaldırılmayacaktır.

PHASE 1'in ürünü **gerçek veri entegrasyonu değil, gerçek veri entegrasyonunun değişmez omurgasıdır.**

---

# 2. PHASE 1'in sistem içindeki yeri

Final hedef akış:

```text
Platform / GA4 Provider Data
        ↓
Adapter
        ↓
Canonical Raw Contract
        ↓
Time + FX Normalization
        ↓
Dataset V2 — Raw Fact Source-of-Truth
        ↓
Scope-aware Aggregation
        ↓
Formula Engine
        ↓
Funnel Query / Compare / Intent
        ↓
Funnel API
        ↓
Funnel UI
```

PHASE 1 bu hattın provider ve database bağımsız çekirdeğini kurar:

```text
Fixture Raw Input
        ↓
Canonical Contract Validation
        ↓
Time Service
        ↓
FX Service
        ↓
Entity / Hierarchy Validation
        ↓
Analysis Scope
        ↓
Formula Engine
        ↓
Repository Interface
        ↓
Funnel Query Service
        ↓
Deterministic Test Output
```

Buradaki `Fixture Raw Input`, Meta/Google vb. gerçek provider çağrısı yerine kontrollü test datasıdır.

---

# 3. PHASE 1 boyunca korunacak çalışan sistem

PHASE 1 sırasında aşağıdaki çalışan yapılar **değiştirilmeyecek / emekli edilmeyecek**:

- `dashboard.html` operational shell
- login / signup / sign out
- auth / Supabase user lifecycle
- My Account
- account currency ayarları
- Connect / Disconnect
- platform account discovery / selection
- OAuth / token lifecycle
- global Refresh
- mevcut Refresh status / job lifecycle
- `snapshot_jobs`
- `snapshot_schedules`
- mevcut Snapshot capture path
- mevcut Dashboard'ın snapshot'tan beslenen analytics path'i
- Funnel'ın mevcut mock `SOURCE_ROWS` yapısı

Bunların hiçbirinin Phase 1'i tamamlamak için sökülmesi gerekmez.

---

# 4. PHASE 1 sırasında özellikle yapılmayacaklar

Aşağıdakiler **PHASE 1 kapsamı değildir**:

- Meta API mapping'ini production'a bağlamak
- Google conversion mapping'ini değiştirmek
- TikTok metric request'lerini değiştirmek
- Klaviyo Campaign/Flow API implementasyonu yapmak
- GA4 Organic classifier'ı production'a bağlamak
- Google PMax API/reporting implementasyonu yapmak
- Supabase `performance_dataset_rows_v2` migration'ını production'a geçirmek
- mevcut snapshot tablosunu değiştirmek veya silmek
- Funnel HTML içindeki mock veriyi kaldırmak
- Dashboard ile Funnel'ı bind etmek
- Dashboard cleanup yapmak
- Compare Engine'i production'a bağlamak
- Top Selling / Ranking geliştirmek
- 62 günlük backfill yapmak
- ek kullanıcı/workspace modelini geliştirmek
- genel `server.js` refactor'u yapmak
- eski çalışan endpointleri yeni core'a zorla geçirmek

PHASE 1'in temel güvenlik kuralı:

> **Yeni omurga kurulurken mevcut çalışan ürün davranışı değişmemelidir.**

---

# 5. Mevcut repo gerçekliği ve teknik çalışma stili

Güncel proje Node.js + Express kullanmaktadır ve `server.js` CommonJS stilindedir:

```text
require(...)
module.exports / exports
```

Bu nedenle PHASE 1'de yeni Funnel Core dosyaları, gereksiz bir module-system migration başlatmadan **mevcut CommonJS yapısıyla uyumlu** kurulmalıdır.

Mevcut proje `package.json` içinde ayrıca bir test framework dependency'si taşımamaktadır. PHASE 1'in deterministic contract testleri için yeni büyük bir test altyapısı zorunlu değildir; Node'un built-in assertion imkanlarıyla hafif fixture testleri kurulabilir. Daha sonra project-wide test standardı ayrıca geliştirilebilir.

---

# 6. PHASE 1 hedef klasör yapısı

Ana V3 rapordaki hedef:

```text
/funnel-core
  /adapters
    meta.js
    google.js
    tiktok.js
    klaviyo.js
    ga4.js

  canonical-contract.js
  entity-hierarchy.js
  analysis-scope.js
  time-service.js
  fx-service.js
  formula-engine.js
  dataset-repository.js
  funnel-query-service.js
```

PHASE 1'de provider adapter dosyalarının gerçek API implementasyonu **yapılmaz**.

PHASE 1 için zorunlu aktif çekirdek:

```text
/funnel-core
  canonical-contract.js
  entity-hierarchy.js
  analysis-scope.js
  time-service.js
  fx-service.js
  formula-engine.js
  dataset-repository.js
  funnel-query-service.js
```

Test/fixture için ayrıca örneğin:

```text
/funnel-core/fixtures/
/funnel-core/tests/
```

alanı oluşturulabilir.

Bu test/fixture klasör adları uygulama detayıdır; esas zorunluluk fixture'ların ve deterministic testlerin bulunmasıdır.

---

# 7. İş 1 — `/funnel-core` modül alanını oluştur

## Amaç

Yeni Funnel analytics motorunu mevcut 325KB civarındaki `server.js` içine yeni patchler halinde gömmemek.

`server.js` mevcut operational orchestration görevini sürdürür; yeni analytics logic ayrı modüllerde yaşar.

## Yapılacak

- `/funnel-core` klasörü oluşturulur.
- PHASE 1 çekirdek dosyaları açılır.
- Her dosyanın tek sorumluluğu belirlenir.
- Modüller birbirlerini açık dependency yönüyle çağırır.
- Circular dependency oluşmaması sağlanır.
- `server.js` içine mevcut davranışı değiştiren business logic taşınmaz.

## Sorumluluk yönü

Tercih edilen dependency akışı:

```text
canonical-contract
      ↑
entity-hierarchy
      ↑
time-service / fx-service
      ↑
analysis-scope
      ↑
formula-engine
      ↑
dataset-repository interface
      ↑
funnel-query-service
```

Bu şema katı import sırası değildir; temel prensip şudur:

> Alt seviye contract modülleri HTTP route, UI veya provider API bilmemelidir.

## Yapılmayacak

- mevcut Meta/Google/TikTok/Klaviyo/GA4 fetch fonksiyonlarını hemen yeni klasöre taşımak
- `server.js`i parçalamaya çalışmak
- auth ve Refresh route'larını taşımak
- eski snapshot logic'i temizlemek

## Çıkış ürünü

Yeni core modülleri tek başına `require()` edilebilir ve syntax/runtime import hatası vermeden yüklenebilir olmalıdır.

---

# 8. İş 2 — `canonical-contract.js` oluştur

## Amaç

Bütün provider adapter'larının sonunda aynı dili konuşmasını sağlamak.

Meta, Google, TikTok, Klaviyo ve GA4 farklı payloadlar döndürür. PHASE 1'den sonra core katmanı provider-specific alan isimleriyle çalışmamalıdır.

## Canonical identity contract

```text
user_id
platform
traffic_type
source_system
channel
platform_account_id
date
```

Allowed semantics:

```text
platform     = meta | google | tiktok | klaviyo
traffic_type = paid | organic
source_system = meta_ads | google_ads | tiktok_ads | klaviyo | ga4
channel      = email | sms | null
```

Örnekler:

```text
Meta Paid
platform=meta
traffic_type=paid
source_system=meta_ads
channel=null

Meta Organic
platform=meta
traffic_type=organic
source_system=ga4
channel=null

Klaviyo Email
platform=klaviyo
traffic_type=paid
source_system=klaviyo
channel=email

Klaviyo SMS
platform=klaviyo
traffic_type=paid
source_system=klaviyo
channel=sms
```

## Raw metric contract

PHASE 1 canonical raw fact seti:

```text
impression
ad_click
session
spend_value
add_to_cart
add_to_cart_value
checkout
checkout_value
purchase
purchase_value
```

Her raw metric:

```text
number | null
```

olabilir.

### Çok kritik semantic

```text
0    = provider gerçekten ölçtü, sonuç sıfır
null = ölçülemiyor / desteklenmiyor / bilinmiyor
```

`null` hiçbir yerde otomatik olarak `0`a çevrilmez.

## Metric Support contract

Her metric için status:

```text
supported
unsupported
unknown
```

taşınır.

Örnek:

```text
purchase = 0
metric_support.purchase = supported
```

şu anlama gelir:

> Purchase ölçüldü ve gerçekten 0 çıktı.

Buna karşılık:

```text
checkout_value = null
metric_support.checkout_value = unsupported
```

şu anlama gelir:

> Provider/hesap/report bu metriği sağlamıyor; 0 satış değeri değildir.

## Currency contract

Canonical row şu metadata'yı taşıyabilmelidir:

```text
source_currency
target_currency
fx_rate
fx_rate_date
fx_provider
fx_engine_version
```

## Time contract

```text
source_timezone
business_date
time_engine_version
```

## Provenance contract

En az:

```text
source_system
adapter_version
source_confidence
synthetic
ga4_property_id
raw_reference
```

`synthetic=true` production performance olarak Funnel'a giremez.

## Canonical validation kuralları

PHASE 1'de validator şu hataları yakalamalıdır:

- bilinmeyen `platform`
- bilinmeyen `traffic_type`
- platform/source_system uyumsuzluğu
- Klaviyo dışında anlamsız `channel=email|sms` kullanımı
- numeric metric yerine geçersiz string/object
- `unsupported` metric'e gerçek numeric değer verilmesi gibi semantic çelişkiler
- gerekli identity alanlarının eksik olması
- synthetic production fact'in yanlışlıkla normal fact gibi işlenmesi
- Organic row'da GA4'ün platform gibi yazılması

## Organic account identity kuralı

Organic row:

```text
source_system = ga4
```

olmasına rağmen:

```text
platform_account_id
```

GA4 Property ID değildir.

Bu alan deterministic olarak eşleşmiş AdsTable platform account ID'dir.

GA4 Property ayrıca:

```text
ga4_property_id
```

provenance içinde tutulur.

Deterministic account match yoksa production canonical Organic row oluşturulmaz.

## PHASE 1 çıktısı

Canonical Contract modülü:

- allowed enumları,
- raw metric listesini,
- metric support statuslarını,
- canonical validation kurallarını,
- row shape/normalization helper'larını

tek yerde tanımlamalıdır.

Provider adapter'ları ileride bu contract'a göre yazılacaktır.

---

# 9. İş 3 — `time-service.js` sınırını oluştur

## Amaç

Bir günlük performans satırının hangi güne ait olduğunu server saatine göre değil **kaynağın business timezone'una göre** belirlemek.

## PHASE 1 Time Service ne yapacak?

Time Service provider API'ye gidip timezone aramayacaktır.

Input olarak kendisine verilen:

```text
source_timezone
provider date/time context
```

üzerinden canonical:

```text
business_date
```

üretme/validate etme sorumluluğu taşır.

## Kaynak prensibi

Daha sonraki adapter fazlarında:

```text
Meta / Google / TikTok / Klaviyo Paid → provider account timezone
GA4 Organic                           → GA4 Property timezone
```

Time Service'e input olacaktır.

PHASE 1 yalnız bu contract'ı hazırlar.

## Zorunlu kurallar

- Server UTC zamanı business date yerine sessiz fallback olamaz.
- IANA timezone açık olmalıdır.
- Geçersiz timezone hata olarak görülmelidir.
- Aynı fixture + aynı timezone daima aynı business_date üretmelidir.
- `date` ve `business_date` semantic olarak karışmamalıdır.
- Time normalization version taşır:

```text
time_engine_version = v1
```

## Edge-case testleri

En az şu durumlar fixture ile test edilmelidir:

- UTC'de bir gün, Istanbul'da sonraki güne düşen timestamp
- US timezone'da önceki güne düşen timestamp
- timezone geçersiz
- timezone eksik
- zaten provider tarafından günlük date olarak gelen satır

## Yapılmayacak

- provider account metadata fetch
- GA4 Property metadata fetch
- production snapshot date rewrite

Bunlar adapter fazlarının işidir.

---

# 10. İş 4 — `fx-service.js` sınırını oluştur

## Amaç

Bütün monetary raw metriclerin Formula Engine'e **aynı reporting currency semantics'iyle** girmesini sağlamak.

## FX uygulanacak raw money alanları

```text
spend_value
add_to_cart_value
checkout_value
purchase_value
```

Counts çevrilmez:

```text
impression
ad_click
session
add_to_cart
checkout
purchase
```

## Zorunlu sıra

```text
Raw provider money
→ source currency doğrulama
→ FX normalize
→ aggregate
→ Formula Engine
```

Formula Engine önce hesaplayıp sonra derived metriği çevirmemelidir.

Örneğin CPC:

```text
converted spend / click
```

üzerinden hesaplanır.

## PHASE 1 input/output

Input:

```text
source_currency
target_currency
fx_rate
fx_rate_date
monetary raw fields
```

Output:

```text
normalized monetary raw fields
+ FX provenance
```

PHASE 1'de canlı FX provider entegrasyonu zorunlu değildir. Deterministic fixture rate kullanılabilir.

## Zorunlu kurallar

- Source ve target currency aynıysa rate semantic olarak `1` olabilir.
- Farklı currency'de gerekli FX bilgisi yoksa sistem sessizce `1` kullanamaz.
- `null` monetary metric, FX sırasında `0`a dönüşmez.
- unsupported metric, FX sonrası da unsupported kalır.
- aynı row'un farklı monetary alanları final aggregate'e farklı currency semantics'iyle giremez.
- FX provenance korunur.

## Version

```text
fx_engine_version = v1
```

## Test fixture'ları

- USD → TRY normal conversion
- TRY → TRY rate=1
- null `checkout_value`
- gerçek `0` spend
- farklı currency ama rate eksik
- negative/invalid rate input validation

---

# 11. İş 5 — `entity-hierarchy.js` contract oluştur

## Amaç

Campaign / AdGroup / Ad ilişkisini kaybetmeden, PMax ve Klaviyo geldiğinde olmayan seviyeleri uydurmadan bütün provider hierarchy'lerini ortak sistemde taşıyabilmek.

## Final hierarchy capability map

```text
Meta
Campaign → AdSet → Ad

Google Standard
Campaign → AdGroup → Ad

Google Performance Max
Campaign → Asset Group

TikTok
Campaign → AdGroup → Ad

Klaviyo
Campaign → Campaign Message

Klaviyo
Flow → Flow Message

Organic
Platform-level Organic identity
```

## Canonical lineage alanları

```text
campaign_type
root_entity_type
root_entity_id
root_entity_name
parent_entity_type
parent_entity_id
parent_entity_name
entity_type
entity_id
entity_name
```

## Bağ nasıl korunur?

Google Standard Ad örneği:

```text
root    = Campaign
parent  = AdGroup
entity  = Ad
```

Meta Ad örneği:

```text
root    = Campaign
parent  = AdSet
entity  = Ad
```

PMax örneği:

```text
root    = Campaign
entity  = Asset Group
```

Burada olmayan AdGroup/Ad yaratılmaz.

Klaviyo Campaign Message:

```text
root    = Campaign
entity  = Campaign Message
```

Klaviyo Flow Message:

```text
root    = Flow
entity  = Flow Message
```

Flow, Campaign'ın altına sokulmaz.

## PHASE 1 hierarchy validator ne yapmalı?

- Meta Ad için Campaign + AdSet lineage'ını kabul etmeli.
- Google Standard Ad için Campaign + AdGroup lineage'ını kabul etmeli.
- PMax Asset Group için AdGroup/Ad zorunlu tutmamalı.
- PMax altında fake `Ad` kabul etmemeli.
- Klaviyo Campaign Message ile Flow Message branch'lerini ayırmalı.
- Klaviyo'da fake `Email Flow` ara seviyesi üretmemeli.
- Organic için paid Campaign/Ad identity istememeli.
- `entity_type` ile `campaign_type` çelişkilerini yakalayabilmeli.

## Entity Key

Phase 2 unique key `entity_key` kullanacağı için PHASE 1'de **deterministic entity key üretme prensibi** freeze edilmelidir.

Format implementation detayı olabilir; ancak şu şartlar zorunludur:

- aynı gerçek provider entity her zaman aynı key'i üretmeli,
- farklı provider/entity tipleri çakışmamalı,
- Campaign Message ile Flow Message aynı ID string'ine sahip olsa bile collision oluşmamalı,
- PMax Asset Group ile Standard Ad aynı namespace'e yanlışlıkla düşmemeli,
- Organic identity paid Ad ID uydurmamalı.

Örnek conceptual key:

```text
<platform>:<account>:<entity_type>:<entity_id>
```

Bu örnek zorunlu string formatı değildir; collision-safe determinism zorunluluktur.

## Double-count korumasının temeli

Hierarchy contract ayrıca şu kuralı desteklemelidir:

> Campaign + AdGroup + Ad metrikleri aynı total içine üst üste toplanmaz.

PHASE 1 gerçek TikTok provider datasını düzeltmez; ancak query/aggregation katmanının hangi gerçek leaf/grain'i aggregate ettiğini bilebilmesi için entity capability metadata sağlar.

---

# 12. İş 6 — `analysis-scope.js` oluştur

## Amaç

Kullanıcının Funnel'daki Paid / Organic seçimlerinin backend'de tek ve deterministic anlamı olmasını sağlamak.

## Üç scope

```text
PAID
ORGANIC
PAID_ORGANIC_BLEND
```

API presentation adı daha sonra:

```text
paid | organic | blend
```

olabilir.

## Scope'un ana görevi

Canonical raw rows arasından seçili analize ait satırları belirlemek ve Formula Engine için additive aggregate input hazırlamak.

Scope module business math formüllerini hesaplamaz; **hangi raw factlerin hesaplamaya gireceğini** belirler.

## Click normalization

Ana Funnel'ın bilinçli ürün contract'ı:

```text
Paid    → funnel_click kaynağı = ad_click
Organic → funnel_click kaynağı = session
```

Blend:

```text
funnel_click = paid.ad_click + organic.session
```

Bu bir hata değil, ürün contract'ıdır.

## PAID scope

Seçili Paid satırlar kullanılır:

```text
click     = ad_click
spend     = paid spend
purchase  = paid purchase
sales     = paid purchase_value
```

## ORGANIC scope

Deterministic olarak eşleşmiş Organic satırlar kullanılır:

```text
click     = session
spend     = organic spend
purchase  = organic purchase
sales     = organic purchase_value
```

Organic spend çoğu provider/site durumunda `0` veya destek durumuna göre başka semantic taşıyabilir; scope module bunu uydurmaz.

## PAID_ORGANIC_BLEND

Önce additive facts birleşir:

```text
funnel_click    = paid ad_click + organic session
funnel_spend    = paid spend + organic spend
funnel_purchase = paid purchase + organic purchase
funnel_sales    = paid purchase_value + organic purchase_value
```

ATC, Checkout ve value metrics de aynı seçili scope aggregate prensibiyle toplanır.

## Çok kritik kural

Blend şu değildir:

```text
(Paid CTR + Organic CTR) / 2
```

Doğru:

```text
önce raw facts SUM
sonra Formula Engine derived KPI
```

## Platform selection ile Organic ilişkisi

Organic yalnız deterministic olarak ilgili AdsTable platform/account kimliğine bağlı row'dan gelebilir.

Örneğin Meta seçiliyse Meta Organic, Google seçiliyse Google Organic o platform scope'una katılabilir. GA4 ayrı platform olarak gösterilmez.

## Intent istisnası

Intent Analysis:

```text
Paid-only
```

kalır.

Ana Funnel Blend seçili olsa bile Intent'e Organic session/purchase karıştırılmaz.

PHASE 1'de Intent API bağlanmaz; ancak scope helper'ı `paid-only intent scope` üretmeye hazır olmalıdır.

## NULL propagation

Scope aggregate sırasında:

- unsupported değer `0`a dönüştürülmez,
- metric support state kaybolmaz,
- derived KPI için gerekli input unknown ise Formula Engine bunun hesaplanamaz olduğunu anlayabilmelidir.

Bu noktada her metric için aggregate support semantics'i deterministik olmalıdır.

Örnek:

```text
iki row da supported ve [0, 5] → aggregate 5, supported
rowlardan biri unsupported ve metric gerçekten hesap için zorunluysa → unsupported/partial semantic kaybolmaz
```

Exact support-merging helper'ı PHASE 1'de test edilmelidir.

---

# 13. İş 7 — `formula-engine.js` oluştur

## Amaç

AdsTable business mathematics'in tek sahibi olmak.

Final sistemde şu katmanlar aynı formülü tekrar hesaplamayacak:

- platform normalizer
- snapshot builder
- Dataset spread
- Funnel frontend

## Formula Engine'e girecek veri

Formula Engine provider payload okumaz.

Input yalnız canonical, time/FX normalize edilmiş ve seçili scope için aggregate edilmiş facts'tir.

Örnek input semantic:

```text
funnel_impression
funnel_click
funnel_spend
funnel_add_to_cart
funnel_add_to_cart_value
funnel_checkout
funnel_checkout_value
funnel_purchase
funnel_sales
metric_support
analysis_scope
```

## PHASE 1'de zorunlu Core Derived Output'lar

### 1. Sales

```text
sales = purchase_value
```

Sales raw source-of-truth olarak ayrı provider bağımsız gerçek değildir; canonical purchase value semantic'inin Funnel output adıdır.

### 2. Abandoned

```text
abandoned = max(checkout - purchase, 0)
```

Input unknown/unsupported ise result `null` olur.

### 3. Abandoned Value

V3 blueprint kararı:

```text
abandoned_value = max(checkout_value - purchase_value, 0)
```

Not: Audit 3 daha önce negatif clamp için source evidence'in explicit freeze gerektirdiğini not etmişti. V3 nihai blueprint bu kuralı `max(...,0)` olarak freeze ettiği için PHASE 1 V3 kararını uygular.

### 4. Profit

```text
profit = funnel_sales - funnel_spend
```

Canonical isim `profit`tir.

Funnel prototipindeki `revenue = sales - spend` isimlendirmesi canonical backend'e taşınmaz.

### 5. Margin

```text
margin = profit / funnel_sales × 100
```

Sales denominator 0/unknown ise:

```text
null
```

### 6. CTR

```text
ctr = funnel_click / funnel_impression × 100
```

CTR output percentage-point semantic'indedir.

Heuristic:

```text
value > 1 ise percent
value <= 1 ise ratio
```

**yasaktır.**

CTR Formula Engine tarafından raw click/impression'dan yeniden hesaplanır.

### 7. CPC

```text
cpc = funnel_spend / funnel_click
```

### 8. ROAS

```text
roas = funnel_sales / funnel_spend
```

### 9. CPS

```text
cps = funnel_spend / funnel_purchase
```

## Intent Analysis formula primitives

PHASE 1 Formula Engine aşağıdaki business formulas'ı tek yerde tanımlamalıdır; production Intent binding daha sonra yapılır.

### Add to Cart Rate

```text
add_to_cart_rate = paid_add_to_cart / paid_ad_click × 100
```

### Checkout Rate

```text
checkout_rate = paid_checkout / paid_add_to_cart × 100
```

### Abandoned Rate

```text
abandoned_rate = paid_abandoned / paid_checkout × 100
```

### Purchase Rate

```text
purchase_rate = paid_purchase / paid_checkout × 100
```

Intent scope **Paid-only**dır.

## Zorunlu NULL / 0 policy

Raw fact:

```text
ölçülmüş sıfır = 0
unknown / unsupported = null
```

Derived metric:

```text
payda 0 veya gerekli input hesaplanamaz → null
```

Örnek:

```text
Spend=100
Purchase=0
CPS=null
```

Frontend bunun yerine `0` yazamaz.

## Oran aggregate kuralı

Yanlış:

```text
Campaign CTR = AVG(Ad CTR)
```

Doğru:

```text
Campaign CTR = SUM(Click) / SUM(Impression) × 100
```

Aynı prensip:

- CPC
- ROAS
- CPS
- Margin
- Intent rates

icin geçerlidir.

## Phase 1'de zorunlu olmayan hesaplar

Funnel prototype `sumRows()` içinde ayrıca:

```text
CPM
ACOS
CVR
AOV
```

hesaplanmaktadır.

Ancak Audit 3 bunların ana 17 Funnel metric contract'ında olmadığını ve **V1 zorunlu Formula Engine contract'ına dahil edilmemesi gerektiğini** belirlemiştir.

Bu nedenle PHASE 1'de:

> CPM / ACOS / CVR / AOV geliştirmek zorunlu değildir.

Gelecekte ürün ihtiyacı olursa ayrı eklenir.

## Compare Formula durumu

Compare matematiği backend business logic'tir; ancak ana V3 implementation planında gerçek Compare binding **PHASE 8** kapsamındadır.

PHASE 1 Formula Engine'in core scope'unu Compare'e bağlamak zorunlu değildir.

Compare için ileride kullanılacak freeze edilmiş temel:

```text
change_pct = (current - previous) / abs(previous) × 100
previous = 0 → null
```

Farklı period uzunluğu normalization policy'sinin production wiring'i Compare fazında yapılır.

## Formula version

PHASE 1 çıktısı:

```text
formula_engine_version = v1
```

metadata'sını üretmelidir.

Her Formula Engine result hangi version ile üretildiğini açıklayabilmelidir.

## Formula Engine yapmayacak

- OAuth
- provider API fetch
- GA4 source classification
- FX rate fetch
- database query
- HTML formatting
- currency symbol formatlama
- arrow / renk / chart
- checkbox state
- CSV/XLSX presentation

---

# 14. İş 8 — `dataset-repository.js` interface oluştur

## Amaç

Formula/query katmanının Supabase SQL detayına bağımlı olmamasını sağlamak.

PHASE 1'de henüz Dataset V2 production migration yapılmayacağı için bu dosyanın ana işi **DB contract sınırı** oluşturmaktır.

## Repository neyi temsil edecek?

İleride:

```text
performance_dataset_rows_v2
```

ile konuşacak persistence katmanının tek giriş noktası.

Query Service doğrudan:

```text
supabase.from(...)
```

yazmamalıdır.

## PHASE 1 repository interface sorumlulukları

Conceptual olarak en az şu operasyon sınırları tanımlanmalıdır:

```text
write/upsert canonical raw facts
read canonical raw facts by user/account/date/platform/scope
read entity-scoped raw facts
return raw facts + provenance + metric_support
```

Exact function isimleri repo coding convention'ına göre seçilebilir; business responsibility değişmez.

## PHASE 1'de gerçek Supabase write zorunlu mu?

Hayır.

PHASE 1'de repository'nin:

- interface'i,
- input/output shape'i,
- mock/in-memory fixture implementation'ı

ile Query Service test edilebilir.

Gerçek `performance_dataset_rows_v2` schema ve production repository implementation **PHASE 2** işidir.

## Repository kesinlikle ne yapmayacak?

- CTR/CPC/ROAS hesaplamayacak
- raw metric'i 0/null arasında dönüştürmeyecek
- fake hierarchy üretmeyecek
- FX hesaplamayacak
- analysis scope belirlemeyecek
- frontend response formatı üretmeyecek

Repository'nin görevi:

> **raw canonical gerçeği doğru saklamak/okumak.**

## Phase 2'ye hazırlaması gereken contract

Repository interface şu unique identity semantics'ine hazır olmalıdır:

```text
user_id
+ platform
+ platform_account_id
+ business_date
+ traffic_type
+ entity_key
```

`channel` ve provenance satırda korunur.

Unique DB constraint'in gerçek migration'ı PHASE 2'de kurulur.

---

# 15. İş 9 — `funnel-query-service.js` oluştur

## Amaç

Funnel'ın ihtiyaç duyduğu analizi üretirken bütün core modülleri tek orchestration noktasında birleştirmek.

Bu servis finalde HTTP endpoint'in arkasındaki application service olacaktır.

PHASE 1'de HTTP endpoint açmak zorunlu değildir.

## PHASE 1 query flow

```text
Query Request
   ↓
Repository'den raw rows al
   ↓
Entity/filter scope uygula
   ↓
Analysis Scope uygula
   ↓
Additive facts aggregate et
   ↓
Formula Engine çalıştır
   ↓
Funnel-ready core result dön
```

## Input contract

PHASE 1 fixture query en az şu semantikleri desteklemelidir:

```text
user_id
from
to
platform/account filters
entity filter
analysis_scope = paid | organic | blend
```

Compare period PHASE 1 zorunlu değildir.

## Output contract

PHASE 1 minimum core response:

```text
period
rows
totals
meta
```

Meta en az:

```text
analysis_scope
formula_engine_version
currency
metric_support
```

bilgisini taşıyabilmelidir.

Final HTTP API shape PHASE 8'de freeze/bağlanır; PHASE 1 service bu API'yi beslemeye uygun olmalıdır.

## Query Service ne yapmayacak?

- provider API çağrısı
- OAuth
- raw formula implementation kopyası
- direct UI formatting
- HTML DOM logic
- legacy snapshot JSON parsing'i production source olarak kullanmak

## En önemli sınır

Funnel Query Service business formülü kendi içine kopyalamaz.

Örneğin:

```text
roas = sales/spend
```

sadece `formula-engine.js` içinde bulunmalıdır.

---

# 16. PHASE 1 modüller arası kesin sorumluluk tablosu

| Modül | Sahibi olduğu şey | Sahibi olmadığı şey |
|---|---|---|
| `canonical-contract` | row shape, enums, metric support, validation | provider fetch, formula |
| `time-service` | business date normalization | provider timezone fetch |
| `fx-service` | monetary raw normalization | live rate acquisition policy |
| `entity-hierarchy` | real hierarchy capability + lineage validation | UI tree rendering |
| `analysis-scope` | Paid/Organic/Blend row selection + additive scope facts | derived KPI math |
| `formula-engine` | derived business mathematics + NULL policy | DB/API/UI |
| `dataset-repository` | canonical raw persistence/read boundary | formulas |
| `funnel-query-service` | orchestration | formula duplication, provider fetch |

Bu tablo PHASE 1'in en önemli anti-spaghetti kuralıdır.

---

# 17. PHASE 1 fixture paketi

PHASE 1 gerçek provider entegrasyonu olmadan doğrulanmalıdır.

Minimum fixture seti:

## Fixture A — Meta Paid

```text
Campaign → AdSet → Ad
```

Gerçek sayılar:

- impression
- ad_click
- spend
- ATC/value
- checkout/value
- purchase/value

Amaç:

- canonical validation
- Paid scope
- core formulas

## Fixture B — Google Standard Paid

```text
Campaign → AdGroup → Ad
```

Amaç:

- lineage standard hierarchy
- same formulas, provider bağımsızlık

## Fixture C — Google PMax shape

```text
Campaign → Asset Group
```

Gerçek PMax API entegrasyonu yapılmaz.

Amaç:

- AdGroup/Ad zorunlu olmadığını kanıtlamak
- fake Ad üretilmediğini kanıtlamak

## Fixture D — Klaviyo Campaign Email

```text
Campaign → Campaign Message
channel=email
```

Amaç:

- channel contract
- message entity

## Fixture E — Klaviyo Flow SMS

```text
Flow → Flow Message
channel=sms
```

Amaç:

- Flow'un Campaign altına zorlanmadığını
- SMS channel'ın canonical taşındığını
- spend supported/null semantic'inin korunabildiğini

kanıtlamak.

## Fixture F — Organic

Örnek Meta Organic:

```text
platform=meta
traffic_type=organic
source_system=ga4
platform_account_id=<matched Meta account>
ga4_property_id=<real property>
entity_type=organic
```

Amaç:

- GA4 platform değil source olduğunu
- Organic click basis'in session olduğunu
- account identity'nin platform account'a bağlı olduğunu

kanıtlamak.

## Fixture G — Paid + Organic Blend

Aynı platform için Paid + Organic rows.

Amaç:

```text
funnel_click = paid ad_click + organic session
```

ve derived KPI'ın aggregate raw totals üzerinden üretildiğini kanıtlamak.

## Fixture H — NULL / unsupported

Örneğin:

```text
checkout_value=null
metric_support.checkout_value=unsupported
```

Amaç:

- `null → 0` dönüşmediğini
- abandoned_value hesaplanamıyorsa null kaldığını

kanıtlamak.

## Fixture I — gerçek zero

```text
purchase=0
metric_support.purchase=supported
```

Amaç:

- raw zero'nun korunması
- denominator-dependent CPS'in `null` olması

## Fixture J — FX + timezone

Aynı raw row için source currency ve timezone verilerek deterministic normalized output doğrulanır.

---

# 18. PHASE 1 zorunlu test matrisi

## Contract tests

- Allowed platform kabul edilir.
- Bilinmeyen platform reddedilir.
- GA4 `platform=ga4` olarak kabul edilmez.
- Klaviyo Email/SMS channel kabul edilir.
- Meta `channel=email` gibi anlamsız kombinasyon reddedilir/validation error üretir.
- Raw metric number/null dışı payload reddedilir.
- unsupported ile numeric value semantic conflict yakalanır.

## Hierarchy tests

- Meta Campaign→AdSet→Ad
- Google Campaign→AdGroup→Ad
- PMax Campaign→AssetGroup
- Klaviyo Campaign→Message
- Klaviyo Flow→Message
- Organic platform-level
- fake PMax Ad reddedilir
- fake Klaviyo AdGroup gerektirilmez

## Scope tests

- Paid-only
- Organic-only
- Blend
- Organic click=session
- Paid click=ad_click
- Intent helper Paid-only

## Formula tests

En az:

```text
Abandoned
Abandoned Value
Profit
Margin
CTR
CPC
ROAS
CPS
Add to Cart Rate
Checkout Rate
Abandoned Rate
Purchase Rate
```

Her biri için:

- normal sayı
- denominator=0
- required input=null
- gerçek raw zero

senaryosu test edilir.

## FX tests

- same currency
- cross currency
- missing rate
- null money
- zero money

## Time tests

- timezone day boundary
- invalid timezone
- missing timezone

## Query Service integration test

Tek fixture paketi:

```text
Repository fixture
→ Query Service
→ analysis scope
→ formula engine
→ Funnel-ready output
```

zincirinde aynı input için her çalıştırmada aynı output üretmelidir.

---

# 19. Determinism kuralı

PHASE 1'in ana acceptance cümlesi:

> **Aynı canonical fixture + aynı timezone + aynı FX rate + aynı analysis scope, her çalıştırmada byte-level semantic olarak aynı business result üretmelidir.**

Current time, server timezone, random ID, UI state veya provider network response sonucu değiştirememelidir.

---

# 20. PHASE 1'de server.js'e dokunma sınırı

PHASE 1 yeni core dosyalarını oluştururken `server.js` için kural:

## İzin verilen

- gerekirse yeni core modülünü import eden minimal wiring
- fixture/dev test için güvenli orchestration hook'u
- syntax/import verification

## İzin verilmeyen

- mevcut Refresh route davranışını değiştirmek
- legacy snapshot writer'ı kaldırmak
- platform normalizer'larını topluca taşımak
- auth middleware'i refactor etmek
- unrelated security/cleanup patch yapmak
- dashboard endpointlerini değiştirmek

Yeni core henüz production source-of-truth değildir.

---

# 21. PHASE 1'de Supabase'e dokunma sınırı

## Yapılacak

Repository interface Dataset V2'nin ihtiyaçlarını tanıyacak.

## Yapılmayacak

- production table create/drop
- existing table alter
- RLS migration
- index migration
- V1 data migration
- snapshot rewrite

Bunların tamamı PHASE 2'dir.

---

# 22. PHASE 1'de Funnel HTML'e dokunma sınırı

PHASE 1 sırasında Funnel:

```text
mock çalışan referans ürün
```

olarak korunur.

Yapılmayacak:

- `SOURCE_ROWS` kaldırmak
- `sumRows()` silmek
- backend endpoint bağlamak
- UI metricleri değiştirmek
- hierarchy UI redesign yapmak

Funnel mock bu aşamada **parity oracle / ürün referansı** olarak kalır.

Frontend Formula logic ancak gerçek backend parity doğrulandığında PHASE 9'da kaldırılır.

---

# 23. PHASE 1 kararlarının gelecekte nasıl izleneceği

Her core engine version taşımalıdır:

```text
formula_engine_version = v1
fx_engine_version      = v1
time_engine_version    = v1
adapter_version         = daha sonra provider adapter fazında
```

Canonical Contract da mümkünse kendi contract version bilgisini taşımalıdır:

```text
canonical_contract_version = v1
```

Bu alan V3'te explicit zorunlu DB kolonu olarak freeze edilmemiştir; ancak PHASE 1'in test/output metadata'sında contract revision'ın izlenebilir olması önerilir. Production schema kararı PHASE 2'de kesinleştirilebilir.

---

# 24. PHASE 1 sırasında açık kalan ve burada uydurulmayacak konular

Bu detay raporu her şeyi çözmüş gibi davranmamalıdır.

Aşağıdakiler PHASE 1'de tahminle kapatılmaz:

1. **Provider-specific exact metric field isimleri**  
   Meta/Google/TikTok/Klaviyo/GA4 adapter fazlarında gerçek provider response üzerinden doğrulanır.

2. **Klaviyo Email automatic Spend**  
   Ayrı `AdsTable_Klaviyo_Spend_Model_Research_Note_2026-08-15_TR.md` içinde park edilmiştir.

3. **Google PMax gerçek API/reporting implementation**  
   Entity contract hazırdır; provider implementation core sonrası.

4. **GA4 deterministic Organic classifier'ın exact source/medium rules'i**  
   GA4 Phase 7'de gerçek provider evidence ile kurulacaktır.

5. **Compare unequal-period exact production wiring**  
   Compare Engine fazında freeze/implement edilir; Formula core'dan bağımsız ayrı business policy'dir.

6. **Dataset V2 physical PostgreSQL schema types/indexleri**  
   PHASE 2 migration raporunun konusudur.

Açık konu görünür biçimde açık kalır; varsayımla kod içine gömülmez.

---

# 25. PHASE 1 uygulama sırası

Önerilen exact sıra:

## Step 1 — Freeze / baseline

- güncel repo commit/hash not edilir
- `server.js` baseline korunur
- mevcut testsiz çalışan routes'a dokunulmaz

## Step 2 — Funnel Core klasörü

- klasör ve boş module boundaries
- import sanity check

## Step 3 — Canonical Contract

- identity
- entity
- raw metrics
- metric support
- provenance
- validators

Bu diğer bütün modüllerin temelidir.

## Step 4 — Entity Hierarchy

- provider capability map
- lineage validation
- entity key principle

## Step 5 — Time Service

- deterministic business date
- timezone validation

## Step 6 — FX Service

- monetary raw conversion boundary
- null/support propagation

## Step 7 — Analysis Scope

- Paid
- Organic
- Blend
- Intent paid-only scope helper

## Step 8 — Formula Engine

- core derived metrics
- Intent rate formulas
- null/zero policy
- version metadata

## Step 9 — Repository Interface

- in-memory/mock repository contract
- future V2 persistence boundary

## Step 10 — Funnel Query Service

- modules orchestration
- fixture query → Funnel-ready result

## Step 11 — Full fixture matrix

- Meta
- Google Standard
- PMax shape
- TikTok shape
- Klaviyo Email
- Klaviyo SMS
- Organic
- Blend
- null/unsupported
- zero
- FX/time

## Step 12 — Phase acceptance

- tests
- no legacy regression
- no production schema change
- deterministic output

---

# 26. PHASE 1 kabul kriterleri — detaylı

PHASE 1 ancak aşağıdakilerin **tamamı** doğruysa kapanır.

## A. Structure

- `/funnel-core` mevcut.
- 8 çekirdek module mevcut.
- Import/syntax hatası yok.
- General server.js refactor yapılmamış.

## B. Canonical Contract

- platform/source/traffic/channel enums freeze.
- raw metric list freeze.
- `0 != null != unsupported` korunuyor.
- provenance taşıyor.
- Organic account mapping shape destekleniyor.

## C. Hierarchy

- Meta gerçek lineage.
- Google Standard gerçek lineage.
- PMax Campaign→Asset Group.
- TikTok Campaign→AdGroup→Ad contract.
- Klaviyo Campaign→Message ve Flow→Message.
- Organic platform-level.
- fake level yok.

## D. Time

- timezone input ile deterministic business_date.
- silent UTC fallback yok.

## E. FX

- dört monetary raw field aynı normalization contract'ında.
- missing cross-currency rate sessizce 1 yapılmıyor.
- null/unsupported korunuyor.

## F. Scope

- Paid çalışıyor.
- Organic çalışıyor.
- Blend çalışıyor.
- Paid click=ad_click.
- Organic click=session.
- Blend yüzdelerin ortalaması değil raw aggregate.
- Intent scope Paid-only.

## G. Formula

Zorunlu outputlar doğru:

```text
abandoned
abandoned_value
profit
margin
ctr
cpc
roas
cps
add_to_cart_rate
checkout_rate
abandoned_rate
purchase_rate
```

- denominator 0 → derived null.
- required input unsupported/null → derived null.
- gerçek raw 0 korunuyor.
- Formula Engine version dönüyor.
- CPM/ACOS/CVR/AOV zorunlu core'a eklenmemiş.

## H. Repository

- Formula logic repository içinde yok.
- in-memory/fixture raw rows okunabiliyor.
- future Dataset V2 raw shape ile uyumlu.

## I. Query Service

- fixture rows üzerinden selected scope result üretebiliyor.
- formulas yalnız Formula Engine'den geliyor.
- output Funnel-ready core shape'e sahip.

## J. Regression boundary

- mevcut Dashboard çalışmaya devam ediyor.
- mevcut Refresh davranışı değişmedi.
- legacy Snapshot path çalışmaya devam ediyor.
- Funnel mock değişmedi.
- production Supabase schema değişmedi.

---

# 27. PHASE 1 kapanışında hazırlanacak kısa evidence paketi

Phase tamamlandığında yalnız “kod yazıldı” denmemelidir.

Kapanış paketi en az şunları göstermelidir:

1. Oluşturulan dosya listesi.
2. Her module'ün tek cümle sorumluluğu.
3. Canonical Contract örnek row.
4. PMax hierarchy fixture sonucu.
5. Klaviyo Campaign/Flow fixture sonucu.
6. Paid-only fixture sonucu.
7. Organic-only fixture sonucu.
8. Paid+Organic Blend sonucu.
9. `0` vs `null/unsupported` test sonucu.
10. FX conversion fixture sonucu.
11. timezone business-date fixture sonucu.
12. Formula Engine metric test sonucu.
13. Query Service end-to-end fixture sonucu.
14. Eski production path'e dokunulmadığının diff özeti.

Bu evidence paketi PHASE 2'ye geçiş iznidir.

---

# 28. PHASE 1 başarısız sayılacağı durumlar

Aşağıdakilerden biri varsa PHASE 1 kapanmış sayılmaz:

- Formula birden fazla module kopyalanmışsa
- `null` metric 0'a çevriliyorsa
- GA4 platform olarak modelleniyorsa
- Organic platform account yerine GA4 Property ID kullanıyorsa
- PMax'a fake AdGroup/Ad yaratılıyorsa
- Klaviyo Flow Campaign altına zorlanıyorsa
- Repository derived KPI source-of-truth tutuyorsa
- FX Formula Engine'den sonra uygulanıyorsa
- server UTC business date olarak sessiz fallback ise
- Paid+Organic Blend iki oranı ortalayarak hesaplanıyorsa
- Campaign/AdGroup/Ad aynı aggregate içinde double-count edilebiliyorsa
- existing Refresh/Snapshot behavior Phase 1 yüzünden değişmişse
- production Supabase schema Phase 2 öncesi değiştirilmişse

---

# 29. PHASE 1 sonunda elimizde ne olacak?

Kullanıcı açısından henüz ekranda yeni gerçek data görünmeyebilir.

Ama teknik olarak şu omurga hazır olacaktır:

```text
HER PLATFORM
     ↓
aynı Canonical Raw diline çevrilebilir
     ↓
aynı Time contract
     ↓
aynı FX contract
     ↓
gerçek hierarchy korunur
     ↓
Paid / Organic / Blend scope belirlenir
     ↓
tek Formula Engine hesaplar
     ↓
tek Repository sınırı
     ↓
tek Funnel Query Service
```

Bundan sonra PHASE 2'nin görevi:

> **Bu çalışan core contract'ının fiziksel Supabase Dataset V2 karşılığını kurmak.**

PHASE 3'ten itibaren ise provider'lar tek tek bu hatta bağlanır.

---

# 30. PHASE 1 — Yönetici özeti / uygulama emri

PHASE 1'in amacı yeni feature üretmek değildir.

Amaç:

> **AdsTable analytics sisteminin bir daha platforma, snapshot'a veya frontend patch'ine göre farklı matematik konuşamayacağı ortak çekirdeği kurmaktır.**

Uygulama sırası:

```text
1. /funnel-core
2. canonical-contract
3. entity-hierarchy
4. time-service
5. fx-service
6. analysis-scope
7. formula-engine
8. dataset-repository interface
9. funnel-query-service
10. fixture + deterministic acceptance tests
```

PHASE 1 tamamlanmadan:

```text
Supabase Dataset V2 migration
```

başlatılmamalıdır; çünkü PHASE 2'nin schema'sı PHASE 1'de freeze edilen contract'ın fiziksel karşılığı olacaktır.

---

# 31. Bundan sonraki çalışma standardı

Bu projede bundan sonra her PHASE için iki belge birlikte kullanılacaktır:

```text
ANA IMPLEMENTATION RAPORU
→ tüm savaş planı / faz sırası / mimari yön

PHASE DETAY RAPORU
→ yalnız başlanacak fazın exact görev haritası
```

Her yeni PHASE başlamadan önce o faz için:

- amaç,
- gerçek mevcut durum,
- yapılacaklar,
- sorumluluk sınırları,
- input/output,
- test/evidence,
- kabul kriteri,
- dokunulmayacaklar,
- bir sonraki faza geçiş şartı

ayrı detay raporunda sabitlenecektir.

Bu yöntem ana raporu şişirmeden implementation sırasında bağlam kaybını önler.
