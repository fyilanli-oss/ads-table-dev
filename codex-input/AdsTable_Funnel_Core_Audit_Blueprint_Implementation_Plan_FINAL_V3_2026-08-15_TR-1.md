# AdsTable — Funnel Core Audit + Backend Blueprint / Contract + Implementation Plan

**Tarih:** 15 Ağustos 2026  
**Durum:** Implementation öncesi nihai teknik karar — V3  
**Ana anayasa:** Funnel  
**Kaynaklar:** Güncel Funnel HTML, güncel proje/server.js, canlı Supabase (`adstable-dev`) ve daha önce doğrulanmış Audit 1–2–3 / Risk Audit kararları yalnızca gerektiğinde referans olarak kullanılmıştır.

---

## 0. Amaç ve kapsam

Bu çalışma yeni bir “audit dönemi” açmak için hazırlanmadı. Amaç üç parçayı tek yerde kapatmaktır:

1. Güncel proje üzerinde yapılan son denetimin sonuçlarını sabitlemek.
2. Funnel’ın gerçek verilerle, backend hesaplamasıyla çalışması için **Backend Blueprint / Contract** tanımlamak.
3. Dokümantasyonu kapatıp uygulamaya geçebilmek için **Implementation İş Planı** çıkarmak.

Bu çalışma özellikle aşağıdakileri **şimdilik implementation kapsamı dışında** bırakır:

- 62 günlük initial backfill’in uygulanması,
- ek kullanıcı / workspace modeli,
- Google PMax API/reporting implementasyonunun tamamlanması,
- Top Selling / Ranking özelliğinin ürün olarak aktive edilmesi,
- eski Audit 1–2–3’te bulunan bütün güvenlik ve düşük/orta öncelikli risklerin tek tek kapatılması,
- genel amaçlı `server.js` refactor’u.

**Not:** PMax API implementasyonu sonraya bırakılır; ancak PMax’in `Campaign → Asset Group` entity/hierarchy contract’ı bu dokümanda bugünden freeze edilir. Top Selling ise core zorunluluk değil, ayrı ticari/ürün kararıdır.

### V3 freeze ekleri

Bu revizyonda aşağıdaki noktalar ayrıca sabitlenmiştir:

- **0 ≠ unsupported:** ölçülmüş gerçek sıfır `0`, desteklenmeyen/gelmeyen metrik `null` olacaktır; `metric_support` bunu açıkça taşıyacaktır.
- **Dataset V2 source-of-truth = raw facts:** CTR/CPC/ROAS gibi derived KPI’lar canonical raw gerçek değildir; Formula Engine üretir.
- **Organic account identity:** Organic satır eşleşmiş AdsTable platform hesabının `platform_account_id` değerini taşır; GA4 Property ayrı provenance olarak tutulur. Deterministic account match yoksa satır canonical Dataset’e girmez.
- **Klaviyo channel:** `channel=email|sms` ayrımı canonical contract’a eklenir; Klaviyo tek platform olarak kalır.
- **Klaviyo Spend:** SMS’te provider gerçek spend’i kullanılabilir; Email spend otomasyonu ayrı araştırma notunda park edilmiştir. Mevcut manuel değer/gelecekteki Plan Cost Override yalnız fallback rolündedir.

---

# BÖLÜM I — SON DENETİM RAPORU

## 1. Nihai mimari prensip

AdsTable’ın final analiz mimarisinde sorumluluk paylaşımı şu olmalıdır:

> **Funnel ürün anayasasıdır: hangi veri ve hangi metrik gösterilecek onu belirler.**  
> **Backend veriyi toplar, normalize eder, FX uygular, aggregate eder ve bütün business matematiğini hesaplar.**  
> **Funnel hazır sonucu gösterir; business math sahibi olmaz.**

Mevcut Funnel HTML bir prototip olarak birçok hesabı frontend’de yapıyor. Final mimaride bu davranış korunmayacaktır.

Funnel’ın güncel kodunda `sumRows()` fonksiyonu bugün şu hesapları frontend’de yeniden üretmektedir:

- CTR
- CPC
- Add to Cart Rate
- Checkout Rate
- Abandoned Rate
- Sales/Purchase Rate
- Revenue/Profit türevi
- Margin
- CPS
- ROAS
- CPM
- ACOS
- CVR
- AOV

Bu yapı prototip için faydalıdır; production mimaride tek gerçek Formula Engine backend’de olmalıdır.

---

## 2. Funnel’ın gerçek veri ihtiyacı

Güncel Funnel mock contract’ı günlük leaf satırı mantığına dayanıyor.

### 2.1 Dimensions / identity

Her kaynak satırın temel kimliği:

- `date`
- `platform`
- `traffic_type`
- `source_system`
- `currency`
- `platform_account_id`

Final platform sözlüğü:

```text
platform = meta | google | tiktok | klaviyo
traffic_type = paid | organic
source_system = meta_ads | google_ads | tiktok_ads | klaviyo | ga4
```

GA4 bir AdsTable platformu değildir; Organic verinin source system’idir. Örneğin `Meta Organic` satırı `platform=meta`, `traffic_type=organic`, `source_system=ga4` olarak modellenir.

Direct ve Others final Funnel analytical contract’a alınmaz. GA4 raw/evidence katmanında gözlenebilirler; ancak AdsTable platformlarından birine deterministic olarak eşleşmiyorlarsa canonical Funnel Dataset’e taşınmazlar.

Entity kimliği sabit `Campaign → AdGroup → Ad` kolonlarına zorlanmayacaktır. Provider’ın gerçek capability’si `entity_type / parent / root lineage` ile taşınacaktır. GA4 Organic için paid entity ID uydurulmaz; platform-level organic identity kontrollü şekilde oluşturulur.

### 2.2 Raw metrics

Funnel’ın ihtiyaç duyduğu temel ham gerçekler:

- `impression`
- `ad_click`
- `session`
- `spend_value`
- `add_to_cart`
- `add_to_cart_value`
- `checkout`
- `checkout_value`
- `purchase`
- `purchase_value`

Bu ham değerler Formula Engine’in girdisidir.

### 2.3 Funnel’ın beklediği ana output metrikleri

Güncel Funnel Table metrik seti:

- Impression
- Click
- Spend
- CTR
- CPC
- Add to Cart
- Add to Cart Value
- Checkout
- Checkout Value
- Abandoned
- Abandoned Value
- Purchase
- Sales
- Revenue / Profitability metriği
- Margin
- ROAS
- CPS

Ek ekranlarda Compare ve Intent Analysis aynı canonical veri setinden türetilecektir.

**Top Selling / Ranking özelliği core zorunluluk değildir.** Ürüne eklenip eklenmeyeceği ayrı ticari karardır; bu raporda yalnız ileride eklenirse uyması gereken capability-aware sınır tanımlanır.

---

# 3. Platform bazlı gerçek durum

## 3.1 Meta — temel sağlam, mapping eksik

### Mevcut doğru taraf

`server.js` içindeki Meta normalizer mevcut API cevabından şu değerleri çıkarabiliyor:

- spend
- impression
- ad click
- add to cart count
- add to cart value
- checkout count
- checkout value
- purchase count
- purchase value

Campaign → Ad Set → Ad hierarchy de mevcut.

### Sorun

Sorun Meta API’den verinin gelmemesi değil. Sorun sonraki snapshot/persistence katmanında bazı raw value alanlarının düşürülmesi.

Özellikle mevcut snapshot builder hattı Funnel için gerekli:

- Add to Cart Value
- Checkout Value
- explicit Purchase Value provenance

alanlarını canonical satıra eksiksiz taşımıyor.

### Karar

**Meta OAuth / account selection / fetch mantığı KORUNACAK.**  
Meta reporting output’u yeni Canonical Raw Contract’a tam map edilecek.

Meta için yeni bir API entegrasyonu baştan yazmak gerekmiyor. İşin ana kısmı **mapping + canonical persistence**.

---

## 3.2 Google Ads — temel sağlam, conversion value mapping eksik

### Mevcut doğru taraf

Google conversion breakdown sorgusu şu conversion aksiyonlarını ayırt edebiliyor:

- ADD_TO_CART
- BEGIN_CHECKOUT
- PURCHASE

ve her eşleşmede hem:

- count
- value

bilgisi mevcut.

### Sorun

Normalizer bugün:

- ATC count’u taşıyor fakat ATC value’yu kaybediyor,
- Checkout count’u taşıyor fakat Checkout value’yu kaybediyor,
- Purchase count/value’yu daha doğru taşıyor.

Ayrıca canlı operational kayıtlarında Google hesap currency/timezone alanlarının eksik veya UTC fallback ile kalabildiği görüldü. Bu, FX ve günlük business date için güvenilir final contract değildir.

### Karar

**Google OAuth / account / fetch mantığı KORUNACAK.**  
Conversion mapping tamamlanacak; gerçek account currency ve timezone kesin kaynak haline getirilecek.

PMax API/reporting implementasyonu bu aşamada kapsam dışıdır. Ancak final entity contract bugünden freeze edilmiştir:

```text
Google Standard: Campaign → AdGroup → Ad
Google PMax:     Campaign → Asset Group
```

PMax altında sahte AdGroup, Ad veya sentetik “PMax Ad” oluşturulmayacaktır.

---

## 3.3 TikTok — reporting contract production Funnel seviyesinde değil

### Mevcut durum

Mevcut TikTok report request’i esas olarak:

- spend
- impressions
- clicks
- CTR
- CPC
- conversion

alanlarına dayanıyor.

### Eksik alanlar

Funnel raw contract için gerekli olan şu alanlar mevcut reporting hattında eksik veya kesin mapping sahibi değil:

- Add to Cart
- Add to Cart Value
- Checkout
- Checkout Value
- Purchase
- Purchase Value

### Kritik çelişki

Kod içinde TikTok truth contract mantığı `conversion != purchase` prensibini kabul ediyor.

Buna rağmen başka bir noktada `complete_payment_count` yoksa generic `conversion` purchase fallback’i olarak kullanılabiliyor.

Bu final Funnel için kabul edilemez.

### Hierarchy riski

Campaign / AdGroup / Ad raporlarının ayrı çekilip daha sonra aynı total hesaplarında birlikte kullanılması aynı performansın hierarchy seviyeleri arasında tekrar sayılmasına yol açabilir.

### Canlı durum

Canlı son TikTok snapshot örneklerinde `sandbox_empty_report_fallback` / synthetic satırlar görüldü. Synthetic fallback production canonical dataset’e gerçek performans olarak girmemelidir.

### Karar

**TikTok OAuth / account lifecycle KORUNACAK.**  
Reporting adapter Funnel Canonical Raw Contract’a göre yeniden tamamlanacak.

Exact TikTok API metric isimleri provider dokümanından implementation sırasında doğrulanacak; tahmin edilerek map yapılmayacak.

---

## 3.4 Klaviyo — paid analytics hattı yeniden düzenlenmeli

Klaviyo’da sorun sadece “eksik metrik” değildir; birkaç farklı sözleşme uyuşmazlığı vardır.

### 3.4.1 Estimated Spend

Dashboard güncel Estimated Spend akışı kullanıcı değerini metadata içine obje olarak kaydediyor:

`estimated_monthly_spend = { amount, currency }`

Backend’in farklı yerlerinde ise farklı isimler kullanılmaktadır:

- `estimatedMonthlySpend`
- `spendCurrency`
- `estimatedPeriodSpend`

Bu alanlar aynı contract değildir.

Canlı son Klaviyo snapshot örneklerinde kullanıcı tarafında Estimated Spend bulunmasına rağmen snapshot Spend değerlerinin 0 olabildiği doğrulandı.

### 3.4.2 Click mapping

Normalizer gerçek click verisini elde edebiliyor.

Fakat snapshot tarafında `ad_clicks` üretirken bazı durumlarda **open count click’in önüne geçebiliyor**.

Bu Funnel için doğrudan yanlış davranıştır:

> Email Open ≠ Ad/Link Click.

CTR/CPC ve journey matematiği bu şekilde beslenmemelidir.

### 3.4.3 Journey eksikleri

Mevcut Klaviyo hattında:

- Placed Order → mevcut
- Purchase Revenue → mevcut
- ATC → eksik/ikincil event mekanizmasına bağlı
- Checkout → eksik/ikincil event mekanizmasına bağlı
- ATC Value → final contract’ta yok
- Checkout Value → final contract’ta yok

### 3.4.4 Hierarchy — final karar

Mevcut Funnel mock’undaki `Campaign → Email Flow → Flow` yapısı final contract değildir. Klaviyo generic `Campaign → AdGroup → Ad` kalıbına zorlanmayacaktır.

Final provider-aware yapı:

```text
Klaviyo
 ├─ Campaigns
 │   └─ Campaign
 │       └─ Campaign Message
 │
 ├─ Flows
 │   └─ Flow
 │       └─ Flow Message
 │
 └─ Klaviyo Organic
```

Campaign ve Flow kardeş root entity tipleridir; Flow bir Campaign altına yerleştirilmez. Campaign Message ve Flow Message gerçek leaf entity olarak taşınabilir. Olmayan AdGroup/Ad seviyesi uydurulmaz.

Klaviyo Organic GA4 kaynaklı platform-level Organic katmandır; Campaign/Flow/Message altına dağıtılmaz.

### Karar

**Klaviyo Connect/OAuth/account/Estimated Spend UI korunacak.**  
Analytics adapter ve spend contract yeniden düzenlenecek; entity mapper Campaign/Message ve Flow/Message kollarını ayrı gerçek hierarchy olarak üretecek.

---

# 4. GA4 / Organic — mevcut final Funnel için güvenilir değil

Bu bölüm kritik.

Mevcut GA4 hattı gerçekten “Organic getir” şeklinde çalışmıyor.

### 4.1 Mevcut query mantığı

GA4 sorgusu ağırlıklı olarak:

- `sessionSource`
- `sessionMedium`

boyutlarını ve:

- sessions
- addToCarts
- checkouts
- ecommercePurchases
- purchaseRevenue

metriklerini çekiyor.

Ancak explicit Organic filter yok.

### 4.2 Mevcut sınıflandırma

Backend bütün source/medium satırlarını alıyor; daha sonra kendi heuristic kurallarıyla paid sandığı trafiği çıkarıp kalanı şu sınıflara bölebiliyor:

- Direct
- Meta Organic
- Google Organic
- TikTok Organic
- Klaviyo Organic
- Others

Bu final AdsTable Organic contract’ı olamaz.

### 4.3 Domain / Property eşleştirme eksik

Seçili GA4 Property mevcut fakat final doğruluk için gerekli olan:

- Web Stream discovery
- domain / site URL doğrulaması
- GA4 Property ↔ gerçek website match

hattı tamamlanmış değil.

Bu nedenle yanlış property seçimi veya yanlış site attribution riski vardır.

### 4.4 Timezone problemi

GA4 property timezone final source-of-truth olarak alınmıyor; UTC fallback kullanılabiliyor.

Bu, günlük data sınırlarını yanlış güne yazabilir.

### 4.5 Currency problemi

GA4 property currency final source-of-truth olarak alınmıyor.

Canlı operational tarafta Organic/GA4 hesapların `base_currency` alanının null kalabildiği görüldü. Buna rağmen snapshot tarafında user/account currency etiketi kullanılarak GA4 purchaseRevenue başka currency’den geliyorsa yanlış currency etiketi alma riski doğuyor.

### 4.6 Journey value eksikleri

GA4 hattında:

- Add to Cart count var
- Checkout count var
- Purchase count var
- Purchase Revenue var

fakat Funnel contract için:

- Add to Cart Value
- Checkout Value

yok.

### 4.7 Organic attribution prensibi

Final sistemde:

- Meta Organic ancak deterministic GA4 source/medium / channel kurallarıyla Meta’ya bağlanabilir.
- Google Organic aynı şekilde doğrulanmalıdır.
- TikTok Organic aynı şekilde doğrulanmalıdır.
- Klaviyo Organic aynı şekilde doğrulanmalıdır.
- Direct / Others hiçbir paid platforma zorla yazılmamalıdır.
- Direct / Others final Funnel Dataset’e dahil edilmemelidir.

Attribution uydurulmamalıdır. Yalnız Meta / Google / TikTok / Klaviyo ile deterministic eşleşen Organic trafik canonical analytical hatta alınır.

### Karar

**GA4 OAuth + property selection altyapısı korunabilir.**  
**GA4 analytics / organic classification core’u yeniden kurulmalıdır.**

GA4 core yeniden kurulurken zorunlu metadata:

- property_id
- property_name
- web_stream_id
- default_uri / domain
- property timezone
- property currency

olmalıdır.

---

# 5. Formula Engine Audit — bugün tek sahibi yok

Mevcut sistemde matematik farklı katmanlara dağılmıştır:

- platform normalizer’ları,
- snapshot builder’ları,
- dataset spread,
- range aggregation,
- Funnel frontend.

Aynı metrik farklı yerde tekrar hesaplanabiliyor.

Bu mimari production source-of-truth için uygun değildir.

### Karar

Tek business math owner:

> **Backend Formula Engine**

olacaktır.

Frontend/Funnel business formula hesaplamayacaktır.

---

# 6. FX / Currency Audit

Mevcut FX katmanında bazı hatlarda yalnız:

- spend
- sales
- revenue

gibi üst seviye monetary alanlar çevriliyor.

Raw journey value alanlarının tamamı aynı FX contract’a sahip değil.

Bu nedenle örneğin bir satırda:

- Sales target currency’ye çevrilmiş,
- Conversion Value / Purchase Value source currency’de kalmış

olma riski vardır.

### Karar

FX **Formula Engine’den önce** uygulanacaktır.

Bütün monetary raw fields aynı FX contract’a tabi olacaktır:

- spend_value
- add_to_cart_value
- checkout_value
- purchase_value

ve canonical satır hem source currency hem display/account currency provenance’ını taşıyacaktır.

---

# 7. Snapshot / Dataset Audit

## 7.1 Çalışan taraf

Mevcut sistemde şu parçalar operational olarak değerlidir ve korunacaktır:

- Dashboard operational frontend
- Auth
- Sign up / Forgot Password
- My Account
- account currency
- Connect / Disconnect
- account selection
- OAuth/token lifecycle
- global Refresh
- Refresh status / job lifecycle
- `snapshot_jobs`
- `snapshot_schedules`
- mevcut snapshot capture hattı geçiş süresince

## 7.2 Problemli analiz hattı

Mevcut `performance_dataset_rows` final Funnel canonical dataset değildir.

Nedenleri:

- row identity `snapshot_id` üzerinden kuruludur,
- aynı gerçek entity aynı gün farklı snapshot versionlarında tekrar bulunabilir,
- `traffic_type` canonical first-class alan değildir,
- `sessions` first-class değildir,
- Add to Cart Value yoktur,
- Checkout Value yoktur,
- Abandoned Value yoktur,
- Formula Engine version yoktur,
- provider provenance final contract seviyesinde değildir.

Canlı DB’de snapshot identity dışarı alındığında aynı günlük entity için yüzlerce tekrar grup ve binlerce ekstra tekrar satır bulundu.

Bu, immutable snapshot/evidence için anlaşılabilir fakat Funnel source-of-truth için uygun değildir.

### Karar

**Snapshot sistemi bir anda silinmeyecek.**  
**Mevcut Dataset Funnel source-of-truth olarak kullanılmayacak.**

Yeni canonical Dataset V2 paralel kurulacak.

---

# BÖLÜM II — BACKEND BLUEPRINT / CONTRACT

## 8. Hedef mimari

Final data path:

```text
Dashboard / Global Refresh
        ↓
Existing Auth / Account / OAuth / Job Lifecycle
        ↓
Platform & GA4 Adapters
        ↓
Canonical Raw + Source / Entity Contract
        ↓
Time + Currency / FX Normalization
        ↓
Canonical Dataset V2 — RAW FACT SOURCE-OF-TRUTH
        ↓
Scope-aware Aggregation
(Paid / Organic / Paid+Organic Blend)
        ↓
Formula Engine
        ↓
Funnel Query / Compare / Intent Layer
        ↓
/api/funnel/data
        ↓
Funnel Presentation
```

Geçiş sırasında legacy path de yaşamaya devam edebilir:

```text
Existing Refresh
        ↓
Existing Snapshot Capture
        ↓
Legacy Dashboard Analysis / Old Dataset
```

Yeni Funnel doğrulanana kadar çalışan operasyonel sistem sökülmeyecektir.

---

# 9. `server.js` organizasyon kararı

Amaç 325KB mevcut `server.js`i tek seferde yeniden yazmak değildir.

Amaç yeni Funnel data motorunu `server.js` monolitinden kontrollü olarak ayırmaktır.

Önerilen hedef modüller:

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

`server.js` geçişte:

- mevcut operational route’ları taşımaya devam eder,
- global Refresh’i taşımaya devam eder,
- platform adapter’larını çağırır,
- yeni Funnel Core’a orchestration yapar.

General refactor yapılmayacaktır.

---

# 10. Canonical Raw Contract

Her adapter aynı temel contract’a normalize edecektir; ancak provider’ın gerçek entity capability’si korunacaktır.

## 10.1 Platform / source semantics

```text
platform = meta | google | tiktok | klaviyo
traffic_type = paid | organic
source_system = meta_ads | google_ads | tiktok_ads | klaviyo | ga4
channel = email | sms | null
```

`channel` yalnız provider’ın gerçekten channel ayrımı verdiği yerde kullanılır. V3 core’da bu alanın ana kullanımı Klaviyo Paid satırlarıdır. Meta/Google/TikTok ve Organic satırlarda `null` olabilir.

GA4 platform değildir; Organic verinin source system’idir.

Örnek:

```text
Meta Paid       → platform=meta,    traffic_type=paid,    source_system=meta_ads, channel=null
Meta Organic    → platform=meta,    traffic_type=organic, source_system=ga4,      channel=null
Klaviyo Email   → platform=klaviyo, traffic_type=paid,    source_system=klaviyo,  channel=email
Klaviyo SMS     → platform=klaviyo, traffic_type=paid,    source_system=klaviyo,  channel=sms
```

Direct ve Others analytical platform/traffic type değildir ve final Funnel Dataset’e taşınmaz.

## 10.2 Capability-aware entity / hierarchy contract

Canonical model olmayan seviyeyi uydurmaz. Leaf satır provider’ın gerçek desteklediği en düşük analytical entity’yi temsil eder ve lineage açıkça taşınır.

```text
Meta:            Campaign → AdSet → Ad
Google Standard: Campaign → AdGroup → Ad
Google PMax:     Campaign → Asset Group
Klaviyo:         Campaign → Campaign Message
Klaviyo:         Flow → Flow Message
Organic:         Platform-level Organic identity
```

Campaign / AdGroup / Ad ilişkisi kaybolmaz. Örneğin Google Standard Ad satırı aynı canonical identity içinde kendi Campaign ve AdGroup lineage’ını taşır. PMax’ta olmayan AdGroup/Ad, Klaviyo’da olmayan AdGroup/Ad uydurulmaz.

Önerilen contract:

```json
{
  "identity": {
    "user_id": "uuid",
    "platform": "meta|google|tiktok|klaviyo",
    "traffic_type": "paid|organic",
    "source_system": "meta_ads|google_ads|tiktok_ads|klaviyo|ga4",
    "channel": "email|sms|null",
    "platform_account_id": "string",
    "date": "YYYY-MM-DD"
  },
  "entity": {
    "campaign_type": "standard|performance_max|null",
    "root_entity_type": "campaign|flow|organic|null",
    "root_entity_id": "string|null",
    "root_entity_name": "string|null",
    "parent_entity_type": "adset|adgroup|campaign|flow|null",
    "parent_entity_id": "string|null",
    "parent_entity_name": "string|null",
    "entity_type": "ad|asset_group|campaign_message|flow_message|organic",
    "entity_id": "string",
    "entity_name": "string"
  },
  "raw_metrics": {
    "impression": "number|null",
    "ad_click": "number|null",
    "session": "number|null",
    "spend_value": "number|null",
    "add_to_cart": "number|null",
    "add_to_cart_value": "number|null",
    "checkout": "number|null",
    "checkout_value": "number|null",
    "purchase": "number|null",
    "purchase_value": "number|null"
  },
  "metric_support": {
    "impression": "supported|unsupported|unknown",
    "ad_click": "supported|unsupported|unknown",
    "session": "supported|unsupported|unknown",
    "spend_value": "supported|unsupported|unknown",
    "add_to_cart": "supported|unsupported|unknown",
    "add_to_cart_value": "supported|unsupported|unknown",
    "checkout": "supported|unsupported|unknown",
    "checkout_value": "supported|unsupported|unknown",
    "purchase": "supported|unsupported|unknown",
    "purchase_value": "supported|unsupported|unknown"
  },
  "currency": {
    "source_currency": "USD",
    "target_currency": "TRY",
    "fx_rate": 1,
    "fx_rate_date": "YYYY-MM-DD",
    "fx_provider": "provider",
    "fx_engine_version": "vN"
  },
  "time": {
    "source_timezone": "IANA timezone",
    "business_date": "YYYY-MM-DD",
    "time_engine_version": "vN"
  },
  "provenance": {
    "source_system": "meta_ads|google_ads|tiktok_ads|klaviyo|ga4",
    "adapter_version": "vN",
    "source_confidence": "real|fallback|partial",
    "synthetic": false,
    "ga4_property_id": "string|null",
    "raw_reference": {}
  }
}
```

JSON örneğindeki `number|null` ifadeleri type contract gösterimidir; gerçek payload’da alan ya gerçek sayı ya da `null` olur.

## 10.3 Metric Support & NULL Policy

Bu ayrım canonical sistem için zorunludur:

```text
Provider gerçek olarak 0 ölçtü          → 0
Provider metriği desteklemiyor          → null + unsupported
Provider cevabında metric belirsiz/yok  → null + unknown/partial provenance
Synthetic fallback                      → production analytical fact değildir
```

Özellikle PMax, Klaviyo ve GA4’te desteklenmeyen metriği `0` yazmak yasaktır. `0` iş sonucu olarak “ölçüldü ve sıfır çıktı” anlamına gelir.

Formula Engine unsupported/null input’u otomatik olarak gerçek `0` kabul edemez. Derived KPI hesaplanamıyorsa canonical NULL policy uygulanır.

## 10.4 Organic Platform Account Mapping Contract

Organic satırın source system’i GA4 olsa da analytical platform identity’si eşleşmiş AdsTable platform hesabıdır.

Örnek:

```text
GA4 Property → deterministic Meta account/domain match

platform             = meta
traffic_type          = organic
source_system         = ga4
platform_account_id   = eşleşmiş Meta platform account ID
ga4_property_id       = provenance içindeki gerçek GA4 Property ID
```

Kurallar:

- `platform_account_id`, Organic satırda GA4 Property ID değildir.
- GA4 Property ID source provenance olarak ayrıca tutulur.
- Aynı kural Google/TikTok/Klaviyo Organic için geçerlidir.
- Deterministic platform-account match yoksa o GA4 satırı Funnel canonical Dataset’e yazılmaz; evidence/unmapped katmanında kalabilir.

## 10.5 Klaviyo Channel Contract — Email / SMS

Klaviyo iki ayrı AdsTable platformuna bölünmez.

```text
platform = klaviyo
channel  = email | sms
```

Hierarchy gerçek provider entity’siyle korunur:

```text
Campaign → Campaign Message
Flow     → Flow Message
```

Her paid Message satırı gerçek channel bilgisini taşır. Böylece kullanıcı Funnel’da tek Klaviyo görürken backend Email ve SMS’i doğru source/spend mantığıyla ayırabilir.

## 10.6 Klaviyo Spend Source Contract — Email vs SMS

V3 core kararı:

```text
Klaviyo SMS   → provider gerçek SMS spend metriği destekleniyorsa provider spend
Klaviyo Email → Email spend otomasyon modeli ayrı araştırma notunda park edilmiştir
```

Email tarafında mevcut manuel Estimated Spend davranışı core geçişinde fallback compatibility olarak korunabilir. Nihai hedef, ayrı araştırma notundaki model doğrulandıktan sonra manuel tahmini ana kaynak olmaktan çıkarmaktır.

Ayrı belge:

```text
AdsTable_Klaviyo_Spend_Model_Research_Note_2026-08-15_TR.md
```

Bu araştırma notundaki otomatik Billing Usage / pricing yaklaşımı bu ana Implementation Plan’ın core acceptance şartı değildir.

## 10.7 Klaviyo Manual Plan Cost Override — Fallback Policy

Gelecekte otomatik Email pricing aktif edildiğinde de provider/account-specific gerçek fiyat güvenilir biçimde belirlenemeyen contract/discount/legacy hesaplar olabilir.

Bu durumda manuel giriş:

> ana Spend motoru değil, yalnız **Plan Cost Override / fallback**

olarak davranır.

Mevcut Estimated Spend UI, migration sırasında bu fallback rolüne dönüştürülebilir; fakat canonical provenance manuel/override kaynaklı spend’i provider gerçek spend’den ayırabilmelidir.

### Genel production kuralı

Canonical Dataset’e production performance olarak giren satırda:

> `synthetic = false`

olmalıdır.

Synthetic/sandbox fallback gerçek satış veya performans gibi Funnel’a gösterilmeyecektir. Entity isimleri UI kolaylığı için sahte hierarchy üretmek amacıyla kullanılmayacaktır.

---

# 11. Time Engine contract

Her satır günlük fact olarak yazılacağı için tarih provider account timezone’una göre belirlenmelidir.

### Paid platformlar

Kaynak account timezone provider’dan okunur.

### GA4

Kaynak timezone GA4 Property metadata’dan okunur.

### Kural

Server UTC zamanı hiçbir platformun business date’i yerine geçmez.

Canonical key içindeki `date` her zaman provider business date’tir.

---

# 12. FX Engine contract

FX, aggregation/formula’dan önce uygulanır.

Her monetary raw field aynı rate ile source → account/display currency’ye çevrilir:

- spend_value
- add_to_cart_value
- checkout_value
- purchase_value

Source değer de provenance/raw içinde korunabilir.

Aynı satırın farklı monetary alanları farklı currency’de bırakılamaz.

---

# 13. Formula Engine contract

Formula Engine yalnız normalized + FX uygulanmış ham değerlerden hesap yapar. Business math sahibi yalnız backend’dir.

## 13.1 Analysis Scope Contract — ürünün güçlü özelliği

Ana Funnel üç gerçek analiz scope’unu destekler:

```text
PAID
ORGANIC
PAID_ORGANIC_BLEND
```

Funnel’daki Paid / Organic kontrolleri hangi scope’un hesaplanacağını belirler; formülü frontend çalıştırmaz. En az bir scope aktif kalır.

### PAID

```text
funnel_click    = paid.ad_click
funnel_spend    = paid.spend
funnel_purchase = paid.purchase
funnel_sales    = paid.sales
```

### ORGANIC

```text
funnel_click    = organic.session
funnel_spend    = organic.spend
funnel_purchase = organic.purchase
funnel_sales    = organic.sales
```

### PAID_ORGANIC_BLEND

Paid ve Organic KPI yüzdelerinin basit aritmetik ortalaması alınmaz. Önce additive raw totals üst üste bindirilir, sonra derived KPI yeniden hesaplanır:

```text
funnel_click    = paid.ad_click + organic.session
funnel_spend    = paid.spend + organic.spend
funnel_purchase = paid.purchase + organic.purchase
funnel_sales    = paid.sales + organic.sales
```

Aynı prensip ATC, Checkout ve değer metriklerine uygulanır. Organic yalnız aynı seçili AdsTable platformuna deterministic olarak bağlı GA4 verisinden gelir.

**Intent Analysis istisnadır: Paid-only çalışır.**

## 13.2 Additive inputs

Önce seçili scope’a ait satırlar toplanır:

- impressions
- ad_clicks
- sessions
- spend
- add_to_cart
- add_to_cart_value
- checkout
- checkout_value
- purchase
- purchase_value / sales

## 13.3 Derived metrics

Sonra seçili scope aggregate’i üzerinden hesaplanır.

### Sales

```text
sales = purchase_value
```

### Abandoned

```text
abandoned = max(checkout - purchase, 0)
```

### Abandoned Value — blueprint kararı

```text
abandoned_value = max(checkout_value - purchase_value, 0)
```

Bu karar count davranışıyla aynı negatif-floor kuralını kullanır.

### CTR

```text
ctr = funnel_click / funnel_impression * 100
```

### CPC

```text
cpc = funnel_spend / funnel_click
```

### ROAS

```text
roas = funnel_sales / funnel_spend
```

### CPS

```text
cps = funnel_spend / funnel_purchase
```

Denominator 0 veya hesaplanamaz ise derived metric canonical NULL policy’ye göre `null` olur; frontend bunu `0`a çeviremez.

### Add to Cart Rate — Intent Analysis

Intent Analysis Paid-only’dır:

```text
add_to_cart_rate = paid_add_to_cart / paid_ad_click * 100
```

### Checkout Rate — Intent Analysis

```text
checkout_rate = paid_checkout / paid_add_to_cart * 100
```

### Abandoned Rate — Intent Analysis

```text
abandoned_rate = paid_abandoned / paid_checkout * 100
```

### Purchase / Sales Rate — Intent Analysis

```text
purchase_rate = paid_purchase / paid_checkout * 100
```

### Profit

Funnel’ın mevcut profitability matematiğiyle uyumlu canonical alan:

```text
profit = funnel_sales - funnel_spend
```

### Margin

```text
margin = profit / funnel_sales * 100
```

### Not — `Revenue` etiketi

Funnel prototipinde bazı yerlerde `sales - spend` değeri `revenue` adıyla kullanılmaktadır.

Backend canonical field adı **`profit`** olacaktır.

Funnel mevcut UI label’ını değiştirmeden önce presentation mapping ile bu alanı kullanabilir; canonical data sözleşmesinde yanlış isim kalıcılaştırılmayacaktır.

---

# 14. Aggregation contract

En önemli kural:

> **Oranlar toplanmaz. Ham gerçekler toplanır; oranlar toplam üzerinden Formula Engine tarafından yeniden hesaplanır.**

Aggregation hem hierarchy-aware hem analysis-scope-aware olacaktır. Campaign + child level’lar aynı total içinde double-count edilmez; Paid, Organic ve Paid+Organic Blend aynı raw facts üzerinden ayrı scope hesaplarıdır.

Doğru:

```text
SUM(click) / SUM(impression)
```

Yanlış:

```text
AVG(row_ctr)
```

Aynı kural CPC, ROAS, CPS, Margin, rate’ler için geçerlidir.

---

# 15. Canonical Dataset V2 — Supabase hedefi

## 15.1 Amaç

Dataset V2 Funnel’ın **raw fact source-of-truth** katmanıdır.

CTR, CPC, ROAS, CPS, Profit, Margin, rate’ler gibi derived KPI’lar Dataset V2 raw gerçeği değildir; backend Formula Engine tarafından raw aggregate üzerinden üretilir.

Snapshot’ın version geçmişi Dataset V2’ye entity duplication olarak taşınmaz.

## 15.2 Grain

Canonical row grain:

> **1 user + 1 platform + 1 platform account + 1 business date + 1 traffic type + 1 gerçek leaf entity**

Leaf entity provider capability’sine göre değişebilir; generic Ad seviyesi zorunlu değildir.

Örnek:

- Meta → Ad
- Google Standard → Ad
- Google PMax → Asset Group
- TikTok → Ad
- Klaviyo Campaign → Campaign Message
- Klaviyo Flow → Flow Message
- GA4 Organic → platform-level deterministic Organic identity

Direct / Others bu analytical Dataset grain’ine girmez.

## 15.3 Önerilen tablo

```text
performance_dataset_rows_v2
```

### Identity / hierarchy

- id
- user_id
- platform
- traffic_type
- source_system
- channel
- platform_account_id
- business_date
- campaign_type
- root_entity_type / root_entity_id / root_entity_name
- parent_entity_type / parent_entity_id / parent_entity_name
- entity_type / entity_id / entity_name
- entity_key
- metric_support

### Raw / normalized metrics

- impressions
- ad_clicks
- sessions
- spend
- add_to_cart
- add_to_cart_value
- checkout
- checkout_value
- purchase
- purchase_value

### Derived KPI — Dataset raw source-of-truth değildir

Aşağıdaki alanlar canonical raw fact kolonları olarak gerçek kabul edilmez:

- abandoned
- abandoned_value
- profit
- margin
- ctr
- cpc
- roas
- cps
- add_to_cart_rate
- checkout_rate
- abandoned_rate
- purchase_rate

Bunlar Formula Engine tarafından runtime’da üretilir. Performans gerekirse ayrı cache/materialized result olarak saklanabilir; ancak bu çıktı raw Dataset V2’nin yerine geçmez ve mutlaka `formula_engine_version` taşır.

### Provenance / version

- source_currency
- target_currency
- fx_rate
- fx_rate_date
- fx_engine_version
- source_timezone
- time_engine_version
- adapter_version
- source_confidence
- ga4_property_id
- source_job_id
- raw
- created_at
- updated_at

## 15.4 Unique key

Unique identity `snapshot_id` içermeyecektir.

Mantıksal key:

```text
user_id
+ platform
+ platform_account_id
+ business_date
+ traffic_type
+ entity_key
```

Aynı gün yeni Refresh geldiğinde aynı fact **UPSERT** edilir.

Snapshot version sayısı Dataset V2’de tekrar satır oluşturmaz.

Organic satırda `platform_account_id`, deterministic olarak eşleşmiş AdsTable platform hesabıdır. GA4 Property ID unique key’de platform hesabının yerine geçmez; provenance olarak ayrıca tutulur. Match yoksa canonical Organic row oluşturulmaz.

---

# 16. Snapshot V1 / Dataset V2 geçiş sözleşmesi

## Geçiş süresinde

Global Refresh bir süre iki çıktı üretebilir:

```text
A) Legacy Snapshot path
B) New Canonical Dataset V2 path
```

Bu dual-write yalnız migration dönemidir.

### Snapshot'ın rolü

- capture evidence
- job/debug history
- legacy Dashboard compatibility

### Dataset V2'nin rolü

- Funnel source-of-truth
- Paid / Organic / Paid+Organic Blend
- compare
- intent
- export

Top Selling / Ranking, ürün olarak aktive edilirse bu canonical source’u tüketir; Dataset V2’nin core acceptance şartı değildir.

Funnel Dataset V2’ye geçtiğinde eski Dashboard analiz parçaları kontrollü olarak emekli edilebilir.

Operational Dashboard korunur.

---

# 17. Funnel API Contract

Funnel Supabase’e doğrudan bağlanmamalıdır.

Backend endpoint:

```text
GET /api/funnel/data
```

Önerilen query:

```text
from=YYYY-MM-DD
to=YYYY-MM-DD
compare_from=YYYY-MM-DD
compare_to=YYYY-MM-DD
platform=optional
analysis_scope=paid|organic|blend
```

Backend:

1. auth/user scope kontrol eder,
2. Dataset V2’den ilgili rows’u okur,
3. platform/entity/filter scope uygular,
4. `analysis_scope` (`paid|organic|blend`) uygular,
5. scope-aware aggregate eder,
6. Formula Engine sonuçlarını üretir veya engine-versioned persisted sonuçları kullanır,
7. Compare değerlerini üretir,
8. Funnel-ready response döner.

### Response prensibi

Funnel’a business math bırakılmaz.

Funnel’a örneğin:

```json
{
  "period": {},
  "rows": [],
  "totals": {},
  "compare": {},
  "meta": {
    "formula_engine_version": "v1",
    "currency": "TRY",
    "metric_support": {}
  }
}
```

gibi hazır analiz contract’ı gider.

### Unsupported metric / NULL davranışı

Funnel API desteklenmeyen metriği `0`a dönüştürmez.

```text
measured zero → 0
unsupported   → null + metric_support=unsupported
unknown       → null + metric_support=unknown
```

Frontend bu farkı presentation seviyesinde gösterir; business anlamını değiştiremez.

---

# 18. Compare Engine contract

Compare frontend’de ayrı matematik motoru olmayacaktır.

Backend iki period için aynı Formula Engine’i çalıştırır.

Change:

```text
(current - previous) / abs(previous) * 100
```

Previous 0 olduğunda change = null.

Different-length periods için additive metrics günlük normalize edilecekse bu davranış tek backend policy olarak freeze edilecektir; Funnel kendi policy’sini uygulamayacaktır.

---

# 19. Intent Analysis contract

Intent Analysis Dataset V2 + Formula Engine çıktısını tüketmelidir.

Provider raw API’ye veya legacy snapshot JSON’a doğrudan bağlanmamalıdır.

Intent Analysis **Paid-only** scope’tur. Organic veya Blend seçimi ana Funnel için geçerlidir; Intent rate matematiğine Organic session/purchase karıştırılmaz.

Böylece:

- Funnel Table
- Compare
- Intent Analysis
- Export

aynı canonical source’dan beslenir.

## 19.1 Top Selling / Ranking — DEFERRED PRODUCT DECISION

Top Selling core Funnel implementation şartı değildir. Ürüne koyulup koyulmaması ayrı ticari karardır.

İleride aktive edilirse ayrı backend Ranking Engine canonical Dataset’i tüketir ve capability-aware çalışır:

```text
Standard Ads → Ad ranking
Google PMax  → Asset Group ranking
Klaviyo      → gerçek Message entity bucket’ları
Organic      → ranking dışı
```

Farklı entity tipleri sahte Ad üretilerek tek leaderboard’a karıştırılmaz.

---

# BÖLÜM III — IMPLEMENTATION İŞ PLANI

## 20. Implementasyon prensibi

Bu proje baştan yazılmayacaktır.

Ayrıca mevcut monolit `server.js` genel amaçla temizlenmeyecektir.

Implementation yalnız Funnel Core için yapılacaktır.

Her aşamada eski çalışan operational sistem korunacaktır.

---

# 21. PHASE 1 — Funnel Core iskeleti

### İşler

1. `/funnel-core` modül alanını oluştur.
2. `canonical-contract` oluştur.
3. `time-service` sınırını oluştur.
4. `fx-service` sınırını oluştur.
5. `entity-hierarchy` contract oluştur.
6. `analysis-scope` (`paid|organic|blend`) contract oluştur.
7. `formula-engine` oluştur.
8. `dataset-repository` interface oluştur.
9. `funnel-query-service` oluştur.

### Bu aşamada yapılmayacak

- Platform API davranışı değiştirilmeyecek.
- Supabase production schema henüz kırılmayacak.
- Funnel mock kaldırılmayacak.

### Kabul kriteri

Aynı fixture raw input Formula Engine’e verildiğinde deterministik Funnel-ready output üretmeli.

---

# 22. PHASE 2 — Supabase Dataset V2 migration

### İşler

1. `performance_dataset_rows_v2` migration oluştur.
2. Unique canonical key oluştur.
3. `source_system` + `channel` + generic entity lineage alanlarını ekle.
4. `metric_support` + NULL policy’yi schema seviyesinde taşı.
5. Organic satır için matched `platform_account_id` + ayrı `ga4_property_id` provenance kur.
6. PMax Asset Group ve Klaviyo Message entity tiplerini schema redesign gerektirmeden taşıyacak capability-aware identity kur.
7. Dataset V2’yi raw fact source-of-truth olarak kur; derived KPI kolonlarını raw truth olarak kullanma.
8. RLS user scope oluştur.
9. Minimum gerekli indexleri kur.
10. `adapter_version`, FX/time provenance alanlarını ekle; derived cache kurulursa `formula_engine_version` cache tarafında zorunlu olsun.
11. Legacy tablolara dokunma.

### Kabul kriteri

Aynı user/platform/account/date/entity ikinci kez yazıldığında duplicate row değil UPSERT oluşmalı.

---

# 23. PHASE 3 — Meta ilk gerçek adapter

Meta ilk referans adapter olacaktır çünkü mevcut fetch ve raw event mapping en olgun platformlardan biridir.

### İşler

1. Mevcut Meta fetch kodunu koru.
2. Meta response → Canonical Raw Contract map et.
3. ATC Value / Checkout Value / Purchase Value eksik geçişini tamamla.
4. account timezone + currency doğrula.
5. FX uygula.
6. Canonical raw fact’i Dataset V2’ye yaz.
7. Formula Engine’i Dataset V2 raw facts üzerinden çalıştır.
8. Aynı Refresh içinde legacy snapshot path’i bozmadan bırak.

### Kabul kriteri

Meta için:

- raw provider result,
- Dataset V2 row,
- Formula Engine output,
- Funnel expected totals

aynı test periodunda birebir reconciled olmalı.

---

# 24. PHASE 4 — Google adapter

### İşler

1. Mevcut fetch/query mantığını koru.
2. ATC Value map et.
3. Checkout Value map et.
4. Purchase count/value provenance’ı koru.
5. gerçek customer currency/timezone getir.
6. FX → Dataset V2 raw fact → Formula Engine hattına bağla.

### Kabul kriteri

Google Standard hierarchy ve totals provider ile reconciled olmalı; conversion action mapping explicit olmalı.

PMax API/reporting implementasyonu bu fazda yapılmaz; ancak adapter ve Dataset contract şu hierarchy’yi şimdiden kabul etmelidir:

```text
Campaign(type=performance_max) → Asset Group
```

Fake AdGroup/Ad üretilmemelidir.

---

# 25. PHASE 5 — TikTok adapter

### İşler

1. Production reporting metrics provider documentation ile kesinleştir.
2. Generic `conversion → purchase` fallback’ini kaldır.
3. ATC / Checkout / Purchase count + value mappinglerini kur.
4. Campaign/AdGroup/Ad double-count riskini kaldır.
5. Synthetic fallback’i canonical production Dataset’ten ayır.
6. FX → Dataset V2 raw fact → Formula Engine hattına bağla.

### Kabul kriteri

Synthetic row Funnel’da gerçek performance olarak görünmemeli.

---

# 26. PHASE 6 — Klaviyo adapter

### İşler

1. Klaviyo’yu tek platform tut; canonical `channel=email|sms` ayrımını ekle.
2. Campaign branch’i `Campaign → Campaign Message` olarak map et.
3. Flow branch’i `Flow → Flow Message` olarak ayrı root hierarchy şeklinde map et.
4. Mock’taki sentetik `Campaign → Email Flow → Flow` yapısını production contract’a taşıma.
5. Open ≠ Click mapping hatasını kaldır.
6. Purchase / Revenue metriclerini doğrula.
7. ATC / Checkout ve value eventlerini gerçek Klaviyo event mapping ile tamamla; unsupported metricleri `0` değil `null + metric_support` olarak taşı.
8. SMS Message satırlarında provider gerçek `text_message_spend` destekleniyorsa gerçek Spend’i kullan; unavailable/unsupported ise uydurma `0` üretme.
9. Email Spend tarafında mevcut Estimated Spend compatibility path’ini fallback olarak koru; otomatik Email pricing modeli bu core fazda zorunlu değildir.
10. Gelecekte automatic Email Spend devreye alınacaksa `AdsTable_Klaviyo_Spend_Model_Research_Note_2026-08-15_TR.md` ayrı karar belgesi olarak kullanılacak.
11. Manual Estimated Spend / Plan Cost Override kaynaklı spend ile provider gerçek spend provenance’da ayırt edilebilir olmalı.
12. Klaviyo Organic’i GA4 kaynaklı platform-level Organic katman olarak tut; Campaign/Flow altına dağıtma.
13. Klaviyo Organic satırını deterministic matched Klaviyo platform account ID’ye bağla; GA4 Property ID provenance’da ayrı tut.
14. FX → Dataset V2 raw fact → Formula Engine hattına bağla.

### Kabul kriteri

- Email ve SMS Message satırları aynı Klaviyo platformu altında gerçek `channel` ile ayrılmalı.
- SMS provider spend destekleniyorsa Dataset/API’de aynı gerçek değer görülmeli.
- Unsupported SMS spend veya journey metriği gerçek `0` gibi görünmemeli.
- Mevcut manuel Email spend fallback’i migration sırasında bozulmamalı.
- Campaign/Message ve Flow/Message hierarchy provider kimlikleriyle izlenebilir olmalı.

---

# 27. PHASE 7 — GA4 Organic Core

Bu ayrı ve ciddi bir adapter fazıdır; “sonra bakarız” değildir.

### İşler

1. GA4 account/property selection mevcut akışı koru.
2. seçili property metadata’yı getir.
3. Web Stream discovery ekle.
4. domain / site URL match doğrula.
5. property timezone al.
6. property currency al.
7. organic source/channel classification contract’ını yaz.
8. Direct/Others’ı paid platforma zorla attribute etme; final Funnel Dataset’e dahil etme.
9. sessions / ATC / Checkout / Purchase / Revenue raw facts üret.
10. ATC Value / Checkout Value provider’da güvenilir şekilde mevcutsa map et; mevcut değilse null/unsupported olarak açık provenance taşı, uydurma.
11. Dataset V2’ye `traffic_type=organic` ile yaz.
12. Formula Engine `ORGANIC` ve `PAID_ORGANIC_BLEND` scope’larına bağla.

### Kabul kriteri

Her GA4 satırı hangi property/domain/source/medium/channel’dan geldiğini açıklayabilmeli.

Yanlış currency ve UTC fallback kabul edilmez.

---

# 28. PHASE 8 — Funnel API

### İşler

1. `/api/funnel/data` endpoint oluştur.
2. auth/user scope uygula.
3. date range uygula.
4. platform/entity filter scope uygula.
5. `analysis_scope=paid|organic|blend` uygula.
6. scope-aware aggregate + Formula Engine sonuçlarını döndür.
7. Compare backend output’unu ekle.
8. Intent’i Paid-only contract ile aynı canonical source’a bağla.
9. `metric_support` ve null/unsupported bilgisini Funnel-ready response’a taşı.
10. Top Selling’i core endpoint acceptance şartı yapma; ürün kararı verilirse ayrı Ranking service ekle.

### Kabul kriteri

Endpoint response kendi başına Funnel UI’yı business math yapmadan besleyebilmeli; unsupported metric hiçbir noktada gerçek `0`a dönüşmemeli.

---

# 29. PHASE 9 — Funnel mock → gerçek backend

### İşler

1. Funnel içindeki hardcoded/mock `SOURCE_ROWS` kaldırılmadan önce feature flag koy.
2. Backend API response adapter’ını frontend’e ekle.
3. `sumRows()` ve duplicate Formula logic presentation path’inden çıkar.
4. Funnel yalnız render/filter/expand/collapse/download davranışlarını taşısın.
5. Paid / Organic checkbox’ları backend `analysis_scope` contract’ını kullansın.
6. Paid+Organic birlikte aktifken backend blended result kullanılsın; frontend oran ortalaması veya business math yapmasın.
7. Compare backend result kullansın.
8. Intent backend result kullansın.
9. `null + metric_support` presentation contract’ını uygula; unsupported metriği `0` gibi gösterme.

### Kabul kriteri

Aynı fixture için:

> eski prototype Funnel görünümü = yeni backend-powered Funnel görünümü

olmalı.

---

# 30. PHASE 10 — Parity / geçiş kontrolü

Her platform için:

1. Provider raw sample
2. Canonical Raw
3. FX result
4. Dataset V2 raw fact row
5. Formula Engine result
6. Funnel API output
7. Funnel rendered value
8. Metric Support / NULL sonucu

aynı zincirde doğrulanacak.

Hiçbir aşamada “ekranda doğru görünüyor” tek başına kabul kriteri değildir. Özellikle `0`, `null`, `unsupported` ve `unknown` ayrımı provider sample’dan Funnel render’a kadar korunmalıdır.

---

# 31. PHASE 11 — Legacy analiz hattını emekli etme

Yalnız parity tamamlandıktan sonra:

- eski Dashboard analysis hesapları,
- Funnel için kullanılmayan snapshot spread pathleri,
- `performance_dataset_rows` V1’in final analysis sorumluluğu

kontrollü olarak kapatılabilir.

### Kural

Operational Dashboard / Auth / Connect / My Account / Refresh / Job lifecycle bundan etkilenmeyecek.

---

# 32. Sonraki işler — Core sonrası

Core Funnel gerçek veride çalıştıktan sonra ayrı sırayla:

1. 62 günlük initial backfill
2. ek kullanıcı / workspace
3. Google PMax API/reporting implementation (`Campaign → Asset Group` contract’ı zaten core’da hazır)
4. Klaviyo Email automatic Spend araştırma modelinin değerlendirilmesi / activation kararı (`AdsTable_Klaviyo_Spend_Model_Research_Note_2026-08-15_TR.md`)
5. Top Selling / Ranking — yalnız ürün/ticari karar ile aktive edilirse
6. Audit 1–2–3 / Risk Audit içindeki kalan security/performance/cleanup maddeleri
7. gerekli diğer provider geliştirmeleri

---

# 33. Nihai karar

## GO

Implementation’a geçilebilir.

Ancak implementation'ın başlangıç noktası **Meta API kodu yazmak değildir.**

İlk implementation:

> **Funnel Core iskeleti + Canonical Raw/Source Contract + Capability-aware Entity Contract + Metric Support/NULL Policy + Dataset V2 raw persistence + Paid/Organic/Blend Formula Engine**

olmalıdır.

Sonra platformlar sırayla yeni hatta alınacaktır:

> **Meta → Google → TikTok → Klaviyo → GA4 Organic**

GA4 bu sırada son adapter olabilir ancak core scope’un dışı değildir.

### Korunacak çalışan mimari

- Dashboard operational frontend
- Auth / profile / currency
- Connect / Disconnect
- account selection
- OAuth/token lifecycle
- global Refresh
- job/schedule lifecycle
- geçiş süresince snapshot capture

### Değiştirilecek analiz mimarisi

- dağınık platform formulas
- snapshot JSON’un analiz source-of-truth rolü
- V1 dataset’in snapshot-version bağımlı identity’si
- frontend business math
- GA4 heuristic organic classification
- eksik platform journey value mappingleri
- GA4’ın platform gibi modellenmesi
- Direct/Others’ın analytical Funnel’a taşınması
- sabit Campaign→AdGroup→Ad varsayımı
- Klaviyo sentetik Email Flow hierarchy’si

### Final mimari cümlesi

> **AdsTable çalışan operasyonel sistemini koruyacak; mevcut Refresh ve platform bağlantılarını kullanacak; Meta/Google/TikTok/Klaviyo’yu platform, GA4’ü Organic source system olarak modelleyecek; Direct/Others’ı final analytical Funnel dışında tutacak; provider’ları capability-aware Canonical Raw/Entity Contract’a normalize edecek; `0` ile unsupported/null ayrımını `metric_support` ile koruyacak; Dataset V2’yi raw fact source-of-truth olarak kuracak; Paid / Organic / Paid+Organic Blend matematiğini tek backend Formula Engine’de hesaplayacak ve Funnel yalnız hazır backend sonucunu gösterecek. Organic satırlar deterministic olarak eşleşmiş platform account ID’ye bağlanacak, GA4 Property provenance’da ayrı tutulacak. Google PMax `Campaign → Asset Group`; Klaviyo `Campaign → Campaign Message` ve `Flow → Flow Message` contract’ları bugünden freeze olacak. Klaviyo tek platform kalacak ve Paid Message satırları `channel=email|sms` taşıyacak; SMS gerçek provider spend’i desteklendiğinde kullanılacak, Email automatic spend araştırması ayrı karar notunda park edilecek. Legacy snapshot/analysis yolu parity tamamlanana kadar paralel yaşayacak ve daha sonra kontrollü emekli edilecek.**

---

# 34. Uygulamaya başlama emri

Bu doküman onaylandığında artık yeni audit / yeni architecture document üretilmeyecektir.

İlk uygulama işi:

> **Phase 1 — Funnel Core iskeleti + Canonical Raw/Source Contract + Entity/Hierarchy Contract + Paid/Organic/Blend Formula Engine**

ardından:

> **Phase 2 — Supabase Dataset V2 migration**

ve sonrasında:

> **Phase 3 — Meta’yı ilk gerçek adapter olarak yeni hatta bağlama**

olacaktır.

