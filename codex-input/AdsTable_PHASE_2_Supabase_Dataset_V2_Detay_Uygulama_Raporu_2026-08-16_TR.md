# AdsTable — PHASE 2 Detay Uygulama Raporu
## Supabase Dataset V2 Migration / Physical Raw Fact Store / RLS / Index / UPSERT / Repository Adapter

**Tarih:** 16 Ağustos 2026  
**Durum:** PHASE 2 uygulama haritası — ana Implementation V3 raporunun detay eki  
**Ana referans:** `AdsTable_Funnel_Core_Audit_Blueprint_Implementation_Plan_FINAL_V3_2026-08-15_TR.md`  
**Bir önceki faz:** `AdsTable_PHASE_1_Funnel_Core_Detay_Uygulama_Raporu_2026-08-16_TR.md`  
**Bir önceki faz evidence:** `AdsTable_PHASE_1_Implementation_Evidence_2026-08-16_TR.md`  
**Destekleyici kapasite referansı:** `AdsTable_Supabase_Final_Kapasite_Analizi_20000_Kullanici_2026-08-14_TR.md`  
**Canlı teknik kaynak:** Supabase `adstable-dev` mevcut public schema kontrolü — 16 Ağustos 2026  
**Kural:** Ana V3 rapor nihai mimari karardır. PHASE 2 bu kararın fiziksel PostgreSQL/Supabase karşılığını kurar; yeni ürün davranışı icat etmez.

---

# 0. Bu belge neden var?

Ana Implementation V3 raporunda PHASE 2 şu kısa görevlerle tanımlanmıştır:

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

Bu belge bu 11 maddenin **tam olarak ne anlama geldiğini**, hangi kolonların oluşturulacağını, hangi constraint/index/policy’nin neden var olacağını, Phase 1 repository interface’in Supabase’e nasıl bağlanacağını ve PHASE 2’nin hangi testlerden sonra kapanacağını freeze eder.

Amaç iki gün sonra şu soruların tekrar çıkmamasıdır:

- Dataset V2’nin gerçek grain’i neydi?
- `snapshot_id` yeni tabloda var mıydı?
- CTR/ROAS kolonu açıyor muyduk?
- `0` ile unsupported nasıl ayrılacaktı?
- `metric_support` JSON mu olacaktı?
- Organic `platform_account_id` GA4 Property mi, platform hesabı mıydı?
- PMax’ta hangi entity saklanacaktı?
- Klaviyo Flow Campaign’in altında mıydı?
- Upsert hangi key’den yapılacaktı?
- RLS browser write’a izin verecek miydi?
- Kaç index kuracaktık?
- Phase 2’de server.js / provider API değişecek miydi?
- Migration yanlış giderse neyi geri alacaktık?

---

# 1. PHASE 2’nin tek cümlelik amacı

> **PHASE 1’de kod seviyesinde freeze edilen Canonical Raw Contract’ı Supabase’de ayrı, güvenli, deterministic UPSERT yapan ve Snapshot versionlarından bağımsız çalışan `performance_dataset_rows_v2` raw fact store’una fiziksel olarak dönüştürmek.**

PHASE 2 sonunda:

- Dataset V2 tablosu Supabase’de var olacak,
- schema/constraint/RLS/index yapısı doğrulanmış olacak,
- Phase 1 `DatasetRepository` interface’inin gerçek Supabase persistence adapter’ı hazır olacak,
- fixture satırı DB’ye yazılıp okunabilecek,
- aynı canonical key tekrar yazıldığında duplicate değil UPSERT oluşacak,
- ancak henüz Meta/Google/TikTok/Klaviyo/GA4 production Refresh bu tabloya bağlanmış olmayacak.

Yani PHASE 2’nin ürünü:

> **“Provider’dan gerçek veri alma” değil, gerçek verinin güvenli biçimde yazılacağı production-grade fiziksel canonical store’dur.**

---

# 2. Canlı mevcut durum — 16 Ağustos 2026 doğrulaması

PHASE 2 başlamadan önce canlı `adstable-dev` Supabase tekrar kontrol edilmiştir.

## 2.1 Mevcut önemli tablolar

Canlı public schema’da bugün:

```text
users
platform_connections
platform_ad_accounts
platform_businesses
user_settings
subscriptions
dashboard_snapshots
insight_logs
fx_rates
platform_account_ownerships
snapshot_jobs
snapshot_schedules
performance_dataset_rows
fx_rates_daily
```

bulunmaktadır.

`performance_dataset_rows_v2` henüz bulunmamaktadır.

## 2.2 Mevcut V1 Dataset

Canlı:

```text
performance_dataset_rows
rows = 4.378
RLS = enabled
index count = 16
```

V1’in unique identity’si bugün:

```text
snapshot_id
+ platform
+ platform_account_id
+ level
+ id_in_platform
+ date_start
+ date_end
```

üzerinden kuruludur.

Yani aynı gerçek entity yeni snapshot versionıyla yeniden geldiğinde yeni canonical fact olarak değil, snapshot-version bağımlı ayrı kayıt olarak yaşar.

Bu, evidence/history mantığı için anlaşılır; final Funnel source-of-truth için uygun değildir.

## 2.3 Mevcut V1 RLS pattern

Canlı V1 Dataset’te authenticated SELECT policy:

```text
auth.uid() = user_id
```

mantığıyla kullanıcının kendi satırlarını okumasına izin vermektedir.

Mevcut V1’de authenticated rol için mutation policy bulunmamaktadır; backend write tarafı service-role/server context ile çalışmaktadır.

PHASE 2’de yeni V2 tablo için bu security yönü korunacak fakat grant/policy yüzeyi daha açık tanımlanacaktır.

## 2.4 Mevcut V1 derived kolon problemi

V1 içinde bugün fiziksel olarak:

```text
ctr
cpc
roas
revenue
abandoned
profit
cps
```

gibi türetilmiş alanlar bulunmaktadır.

PHASE 1 ve V3 kararı gereği V2’de bunlar **raw source-of-truth kolonları olmayacaktır**.

## 2.5 Mevcut account identity

Canlı `platform_ad_accounts` tablosu:

```text
user_id
platform
platform_account_id
currency
timezone
...
```

taşımakta ve:

```text
(user_id, platform, platform_account_id)
```

için unique index bulunmaktadır.

Bu, gelecekte provider adapter’ların deterministic account identity oluşturması için uygun referans noktasıdır.

PHASE 2 V2 tablosu bu gerçek platform account ID’yi canonical identity olarak taşıyacaktır.

## 2.6 Phase 1’den hazır gelen code contract

Phase 1’de şu interface freeze edilmiştir:

```text
DatasetRepository
  upsertCanonicalRawFacts()
  readCanonicalRawFacts()
```

Canonical unique semantic:

```text
user_id
+ platform
+ platform_account_id
+ business_date
+ traffic_type
+ entity_key
```

olarak kodda hazırdır.

PHASE 2 bu interface’in **Supabase implementation’ını** kuracaktır.

---

# 3. PHASE 2’nin sistem içindeki yeri

PHASE 1:

```text
Canonical Contract
Entity Hierarchy
Time
FX
Analysis Scope
Formula Engine
Repository Interface
Query Service
```

oluşturdu.

PHASE 2 şu aradaki fiziksel katmanı tamamlar:

```text
Canonical Raw Contract
        ↓
Time + FX Normalization
        ↓
[ PHASE 2 ]
performance_dataset_rows_v2
RAW FACT SOURCE-OF-TRUTH
        ↓
DatasetRepository
        ↓
Scope-aware Aggregation
        ↓
Formula Engine
```

PHASE 3’te Meta ilk gerçek adapter olarak:

```text
Meta API
→ Canonical Raw
→ Time/FX
→ Dataset V2
→ Formula Engine
```

hattına bağlanacaktır.

---

# 4. PHASE 2 boyunca korunacak çalışan sistem

Aşağıdakiler PHASE 2 sırasında **aynen korunacaktır**:

- `dashboard.html`
- login/signup/sign out
- Auth
- My Account
- account currency
- Connect / Disconnect
- platform account selection
- OAuth/token lifecycle
- Global Refresh
- Snapshot Jobs
- Snapshot Schedules
- mevcut `dashboard_snapshots`
- mevcut `performance_dataset_rows` V1
- mevcut Snapshot capture
- legacy Dashboard analytics
- Funnel mock
- mevcut provider API fetch davranışları
- mevcut Vercel production route’ları

Yeni V2 tablo **parallel** oluşturulacaktır.

Eski tablo rename/drop/alter edilmeyecektir.

---

# 5. PHASE 2 sırasında özellikle yapılmayacaklar

Aşağıdakiler PHASE 2 kapsamı değildir:

- V1 `performance_dataset_rows` silmek
- V1 kolonlarını değiştirmek
- `dashboard_snapshots` değiştirmek
- mevcut 16 V1 index’i temizlemek
- Snapshot version history’yi migrate etmek
- 4.378 V1 satırı otomatik V2’ye taşımak
- historical data backfill yapmak
- Meta production adapter’ını V2’ye bağlamak
- Google/TikTok/Klaviyo/GA4 mapping değiştirmek
- Global Refresh’i dual-write’a almak
- `/api/funnel/data` açmak
- Funnel mock’u kaldırmak
- Dashboard/Funnel bind yapmak
- Derived KPI cache/materialized table kurmak
- Compare result persist etmek
- Formula Engine output’u V2 raw table’a yazmak
- Top Selling/Ranking schema’sı kurmak
- 62 günlük backfill yapmak
- partitioning başlatmak
- read replica kurmak
- retention/archive policy freeze etmek
- genel Supabase cleanup yapmak
- genel `server.js` refactor yapmak

PHASE 2 yalnız yeni Dataset V2 katmanını kurar.

---

# 6. PHASE 2’nin ana fiziksel kararları

## 6.1 Yeni tablo

```text
public.performance_dataset_rows_v2
```

## 6.2 Grain

Her row:

> **1 user + 1 platform + 1 platform account + 1 business date + 1 traffic type + 1 gerçek leaf entity**

temsil eder.

## 6.3 Snapshot bağımsızlığı

V2 identity içinde:

```text
snapshot_id
```

**yoktur.**

Snapshot bir evidence/version mekanizmasıdır.

Dataset V2 ise aynı günlük entity’nin canonical current fact’idir.

## 6.4 Raw fact source-of-truth

V2’de raw olarak saklanır:

```text
impressions
ad_clicks
sessions
spend
add_to_cart
add_to_cart_value
checkout
checkout_value
purchase
purchase_value
```

Derived KPI raw truth değildir.

## 6.5 Derived kolonlar V2’ye konmayacak

Aşağıdakiler V2 raw table’da canonical kolon olmayacaktır:

```text
ctr
cpc
roas
cps
profit
margin
abandoned
abandoned_value
add_to_cart_rate
checkout_rate
abandoned_rate
purchase_rate
```

Bunlar backend Formula Engine output’udur.

## 6.6 0 / NULL semantiği DB’de korunacak

```text
supported + 0   = gerçekten ölçüldü ve sıfır
unsupported     = metric NULL
unknown         = metric NULL
```

DB yanlış kombinasyonu kabul etmemelidir.

---

# 7. İş 1 — Migration alanını repository’de oluştur

Güncel repo’da bugün version-controlled Supabase migration klasörü bulunmamaktadır.

PHASE 2’de ilk kez aşağıdaki migration alanı oluşturulmalıdır:

```text
/supabase
  /migrations
    <timestamp>_create_performance_dataset_rows_v2.sql
```

Amaç:

- canlı DB değişikliğinin GitHub’da izlenebilmesi,
- ne zaman/ne kurulduğunun kaybolmaması,
- Phase 2 checkpoint’inin yalnız Supabase Studio’ya bağlı kalmaması,
- daha sonra migration history’nin proje ile birlikte yaşaması.

## Kural

Canlı DB’ye DDL uygulanırken raw `execute_sql` ile plansız değişiklik yapılmayacaktır.

Implementation sırasında migration bir **isimli Supabase migration** olarak uygulanacaktır.

## Bu migration’ın içeriği

Tek migration içinde mümkün olduğunca:

1. V2 table
2. constraints
3. unique key
4. minimum indexes
5. RLS enable
6. SELECT-own policy
7. privilege/grant sınırı

kurulacaktır.

Legacy tabloya DDL uygulanmayacaktır.

---

# 8. İş 2 — `performance_dataset_rows_v2` fiziksel kolon contract’ı

Aşağıdaki kolon seti PHASE 2 hedefidir.

---

## 8.1 Primary identity

| Kolon | Type | Null | Amaç |
|---|---|---:|---|
| `id` | `uuid` | NO | internal row PK |
| `user_id` | `uuid` | NO | AdsTable user |
| `platform` | `text` | NO | meta/google/tiktok/klaviyo |
| `traffic_type` | `text` | NO | paid/organic |
| `source_system` | `text` | NO | provider/source |
| `channel` | `text` | YES | email/sms only where valid |
| `platform_account_id` | `text` | NO | matched AdsTable platform account |
| `business_date` | `date` | NO | provider business date |

### `id`

```text
default gen_random_uuid()
primary key
```

### `user_id`

V2 user identity:

```text
references public.users(id)
```

olmalıdır.

Silme davranışında yeni `ON DELETE CASCADE` politikası bu fazda uydurulmayacaktır. Mevcut project FK davranışına paralel default FK semantics korunur.

### `platform`

Allowed:

```text
meta
google
tiktok
klaviyo
```

GA4 allowed değildir.

### `traffic_type`

```text
paid
organic
```

### `source_system`

```text
meta_ads
google_ads
tiktok_ads
klaviyo
ga4
```

### `channel`

```text
email
sms
NULL
```

Klaviyo Paid dışında `email|sms` kullanılamaz.

### `platform_account_id`

Organic dahil her analytical row’da gerçek AdsTable platform account ID’dir.

GA4 Property ID değildir.

---

# 9. İş 3 — Platform / Source / Channel DB constraint’leri

DB application validator’ın yerine geçmez; fakat açık semantic çelişkileri kabul etmemelidir.

PHASE 2 schema şu ilişkileri korumalıdır:

## Meta Paid

```text
platform=meta
traffic_type=paid
source_system=meta_ads
channel=NULL
```

## Google Paid

```text
platform=google
traffic_type=paid
source_system=google_ads
channel=NULL
```

## TikTok Paid

```text
platform=tiktok
traffic_type=paid
source_system=tiktok_ads
channel=NULL
```

## Klaviyo Paid

```text
platform=klaviyo
traffic_type=paid
source_system=klaviyo
channel=email|sms
```

## Organic

```text
platform=meta|google|tiktok|klaviyo
traffic_type=organic
source_system=ga4
channel=NULL
```

Bu check’ler şu hataları DB seviyesinde reddeder:

```text
platform=ga4
Meta Paid + source_system=ga4
Meta Paid + channel=email
Klaviyo Paid + channel=NULL
Organic + channel=sms
Organic + source_system=meta_ads
```

---

# 10. İş 4 — Capability-aware hierarchy fiziksel kolonları

V2 generic lineage taşır.

## Kolonlar

| Kolon | Type | Null |
|---|---|---:|
| `campaign_type` | `text` | YES |
| `root_entity_type` | `text` | NO |
| `root_entity_id` | `text` | NO |
| `root_entity_name` | `text` | YES |
| `parent_entity_type` | `text` | YES |
| `parent_entity_id` | `text` | YES |
| `parent_entity_name` | `text` | YES |
| `entity_type` | `text` | NO |
| `entity_id` | `text` | NO |
| `entity_name` | `text` | NO |
| `entity_key` | `text` | NO |

## Allowed `campaign_type`

```text
standard
performance_max
NULL
```

## Allowed root

```text
campaign
flow
organic
```

## Allowed parent

```text
adset
adgroup
campaign
flow
NULL
```

Not: Phase 1’in bugünkü gerçek branches’inde Klaviyo parent kullanmaz; allowed vocabulary gelecekte canonical contract’ın aynı field setini taşıyabilmesi için korunur. Validation actual provider branch’e göre yine Phase 1 `entity-hierarchy.js` tarafından yapılır.

## Allowed leaf entity

```text
ad
asset_group
campaign_message
flow_message
organic
```

---

# 11. Hierarchy branch contract

## Meta

```text
root_entity_type = campaign
parent_entity_type = adset
entity_type = ad
campaign_type = NULL
```

## Google Standard

```text
campaign_type = standard
root_entity_type = campaign
parent_entity_type = adgroup
entity_type = ad
```

## Google PMax

```text
campaign_type = performance_max
root_entity_type = campaign
parent = NULL
entity_type = asset_group
```

Fake AdGroup veya Ad yoktur.

## TikTok

```text
root_entity_type = campaign
parent_entity_type = adgroup
entity_type = ad
campaign_type = NULL
```

## Klaviyo Campaign

```text
root_entity_type = campaign
parent = NULL
entity_type = campaign_message
```

## Klaviyo Flow

```text
root_entity_type = flow
parent = NULL
entity_type = flow_message
```

Flow Campaign’ın altına sokulmaz.

## Organic

```text
root_entity_type = organic
parent = NULL
entity_type = organic
campaign_type = NULL
```

Paid Campaign/Ad identity uydurulmaz.

---

# 12. `entity_key` contract

Phase 1’de deterministic entity key builder hazırdır.

Bugünkü Phase 1 semantic:

```text
platform
platform_account_id
traffic_type
channel
root_entity_type
root_entity_id
entity_type
entity_id
```

kombinasyonundan deterministic key üretir.

Amaç:

- aynı gerçek leaf her refresh’te aynı key,
- farklı platform çakışmaz,
- farklı account çakışmaz,
- Paid/Organic çakışmaz,
- Klaviyo email/sms çakışmaz,
- Campaign Message / Flow Message çakışmaz,
- PMax Asset Group / Standard Ad çakışmaz.

DB `entity_key`i kendi başına yeniden hesaplamayacaktır.

**Entity key’in sahibi application canonical layer’dır.**

DB bunu `NOT NULL` olarak saklar ve unique key içinde kullanır.

---

# 13. İş 5 — Raw metric fiziksel contract

V2 kolonları:

| Canonical Core | DB V2 |
|---|---|
| `impression` | `impressions` |
| `ad_click` | `ad_clicks` |
| `session` | `sessions` |
| `spend_value` | `spend` |
| `add_to_cart` | `add_to_cart` |
| `add_to_cart_value` | `add_to_cart_value` |
| `checkout` | `checkout` |
| `checkout_value` | `checkout_value` |
| `purchase` | `purchase` |
| `purchase_value` | `purchase_value` |

Bu mapping repository adapter’da açık ve tek yerde olmalıdır.

## Physical type

Bütün metric kolonları:

```text
numeric NULL
```

olmalıdır.

### Neden count alanları integer değil?

Provider attribution sistemleri bazı conversion metriclerinde fractional attribution/decimal sonuç üretebilir.

Bu nedenle:

```text
purchase integer
```

gibi gereksiz sert schema varsayımı yapılmayacaktır.

`numeric` hem gerçek integer count’u hem provider’dan gelebilecek fractional conversion count’u taşır.

## Negatif değer constraint’i

PHASE 2 raw metrics için genel:

```text
CHECK metric >= 0
```

koymayacaktır.

V3/Phase 1 raw contract finite number kabul etmektedir; provider adjustment/refund semantics ileride negatif value üretebilir.

Provider semantiği doğrulanmadan DB’ye pozitiflik varsayımı gömülmez.

---

# 14. İş 6 — `metric_support` fiziksel contract

Kolon:

```text
metric_support jsonb NOT NULL
```

olacaktır.

Object en az şu 10 key’i taşımalıdır:

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

Her value:

```text
supported
unsupported
unknown
```

olabilir.

## DB-level integrity

Schema yalnız JSON’ın varlığını kontrol etmekle yetinmemelidir.

PHASE 2 check’leri en az şunu garanti etmelidir:

### Supported

```text
metric_support.<metric> = supported
→ DB metric column IS NOT NULL
```

### Unsupported / Unknown

```text
metric_support.<metric> IN (unsupported, unknown)
→ DB metric column IS NULL
```

Bu 10 metric için aynı rule geçerlidir.

## Örnek — geçerli

```text
purchase = 0
metric_support.purchase = supported
```

## Örnek — geçerli

```text
checkout_value = NULL
metric_support.checkout_value = unsupported
```

## Örnek — RED

```text
checkout_value = 0
metric_support.checkout_value = unsupported
```

Bu row DB tarafından kabul edilmemelidir.

## Örnek — RED

```text
purchase = NULL
metric_support.purchase = supported
```

Bu da kabul edilmemelidir.

---

# 15. İş 7 — Organic Account Identity ve GA4 provenance

Bu PHASE 2’nin en kritik identity kararlarından biridir.

## Organic row

Örnek Meta Organic:

```text
platform = meta
traffic_type = organic
source_system = ga4
platform_account_id = <eşleşmiş Meta account ID>
```

GA4 Property ayrı:

```text
ga4_property_id = <real GA4 Property ID>
```

olarak tutulur.

## `ga4_property_id`

Physical:

```text
text NULL
```

Ancak constraint:

```text
traffic_type=organic
→ ga4_property_id IS NOT NULL
```

olmalıdır.

Paid row’da `ga4_property_id` normalde NULL’dır.

## Deterministic match yoksa

Row V2’ye yazılmaz.

DB “hangi GA4 Property hangi Meta hesabına aittir?” kararını vermez.

Bu karar GA4/adapter mapping katmanının işidir.

PHASE 2 yalnız doğru mapping yapıldıktan sonra kimliği saklar.

---

# 16. İş 8 — Currency / FX provenance

V2 Data Path:

```text
Provider Raw
→ Time
→ FX
→ Dataset V2
```

olduğu için V2 row normalized monetary values taşır.

## Kolonlar

```text
source_currency
target_currency
fx_rate
fx_rate_date
fx_provider
fx_engine_version
```

## Physical policy

### `source_currency`

```text
text NOT NULL
```

3-letter ISO-style code semantic.

### `target_currency`

```text
text NOT NULL
```

3-letter code.

### `fx_rate`

```text
numeric NOT NULL
CHECK fx_rate > 0
```

Same currency:

```text
fx_rate = 1
```

Cross currency:

gerçek rate zorunlu.

### `fx_rate_date`

```text
date NOT NULL
```

PHASE 1 FX Service zaten rate date’i business date’e default edebilir.

### `fx_provider`

```text
text NOT NULL
```

### `fx_engine_version`

```text
text NOT NULL
```

Phase 1 başlangıç:

```text
v1
```

## Currency code check

DB currency code için project’i yalnız:

```text
USD/TRY/EUR/GBP
```

ile kısıtlamayacaktır.

Platform hesaplarında başka gerçek currency’ler olabilir.

Minimum check:

```text
3 uppercase alphabetic character
```

olmalıdır.

---

# 17. Source monetary evidence / `raw` alanı

V2’de FX uygulanmış money metrics saklanacağı için source value’nun izlenebilirliği kaybolmamalıdır.

V3 bunun için provenance/raw alanını açık bırakmaktadır.

PHASE 2 kararı:

```text
raw jsonb NOT NULL DEFAULT '{}'
```

alanı bulunur.

Ancak `raw`, her provider’ın devasa response’unu her leaf row’a kopyalamak anlamına gelmez.

Kapasite raporunda raw JSON’ın row footprint’i ciddi biçimde büyüttüğü görülmüştür.

Bu nedenle V2 `raw` şu amaçla sınırlı tutulmalıdır:

- source monetary raw values gerektiğinde,
- provider row/reference identity,
- mapping debug için gerekli minimal evidence,
- adapter’a özgü küçük trace metadata.

Örnek conceptual:

```json
{
  "source_raw_metrics": {
    "spend_value": 100,
    "add_to_cart_value": 400,
    "checkout_value": 300,
    "purchase_value": 250
  },
  "provider_reference": {}
}
```

**Full provider report response her entity row’a kopyalanmamalıdır.**

Snapshot/evidence katmanı zaten ayrıca yaşamaya devam etmektedir.

---

# 18. İş 9 — Time provenance

Kolonlar:

```text
source_timezone
time_engine_version
business_date
```

## `source_timezone`

```text
text NOT NULL
```

IANA timezone validation application Time Service’te yapılır.

DB server UTC’yi silent fallback olarak üretmez.

## `business_date`

Canonical daily key.

```text
date NOT NULL
```

Provider business date’tir.

## `time_engine_version`

```text
text NOT NULL
```

Phase 1:

```text
v1
```

---

# 19. İş 10 — Adapter / source provenance

Kolonlar:

```text
canonical_contract_version
adapter_version
source_confidence
synthetic
ga4_property_id
source_job_id
raw
created_at
updated_at
```

## `canonical_contract_version`

PHASE 1’de code contract:

```text
CANONICAL_CONTRACT_VERSION = v1
```

olarak freeze edilmiştir.

Phase 1 Detay Raporu fiziksel schema kararını Phase 2’ye bırakmıştı.

PHASE 2 implementation kararı:

```text
canonical_contract_version text NOT NULL
```

olarak V2 row’da tutulmalıdır.

Başlangıç:

```text
v1
```

Bu, ileride contract değiştiğinde hangi row’un hangi canonical semantic ile yazıldığını açıklayacaktır.

## `adapter_version`

```text
text NOT NULL
```

Phase 2 fixture integration testinde test adapter version kullanılabilir.

Provider production versionları Phase 3+ ile gelir.

## `source_confidence`

Allowed:

```text
real
fallback
partial
```

## `synthetic`

```text
boolean NOT NULL DEFAULT false
```

V2 production analytical Dataset için ek DB integrity:

```text
CHECK synthetic = false
```

olmalıdır.

Synthetic/sandbox fallback V2 production fact olamaz.

## `source_job_id`

```text
uuid NULL
```

olacaktır.

Bu alan Refresh/job provenance içindir.

PHASE 2’de strict FK zorunlu tutulmayacaktır; çünkü Phase 2 henüz bütün write path’lerini production’a bağlamamaktadır.

Phase 3+ gerçek Refresh wiring sırasında her canonical write’ın job semantics’i kesinleştiğinde FK ihtiyacı tekrar değerlendirilebilir.

---

# 20. Proposed V2 fiziksel schema — tek görünüm

PHASE 2 hedef kolon seti:

```text
id
user_id

platform
traffic_type
source_system
channel
platform_account_id
business_date

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
entity_key

metric_support

impressions
ad_clicks
sessions
spend
add_to_cart
add_to_cart_value
checkout
checkout_value
purchase
purchase_value

source_currency
target_currency
fx_rate
fx_rate_date
fx_provider
fx_engine_version

source_timezone
time_engine_version

canonical_contract_version
adapter_version
source_confidence
synthetic
ga4_property_id
source_job_id
raw

created_at
updated_at
```

Bu tablo:

```text
Derived KPI table değildir.
Snapshot table değildir.
Provider response archive table değildir.
```

Bu tablo:

> **Funnel canonical normalized raw fact store’dur.**

---

# 21. İş 11 — Unique Canonical Key

Ana V3 + Phase 1 unique semantic:

```text
user_id
+ platform
+ platform_account_id
+ business_date
+ traffic_type
+ entity_key
```

PHASE 2 bunu DB’de **UNIQUE constraint/index** olarak kuracaktır.

Önerilen isim:

```text
performance_dataset_rows_v2_canonical_uidx
```

## Neden `snapshot_id` yok?

Çünkü aynı entity aynı gün 6 Refresh alsa:

```text
6 version
```

değil:

```text
1 canonical daily fact
```

olmalıdır.

## Neden `entity_key` var?

Provider capability-aware hierarchy’yi tek deterministic leaf identity’ye indirger.

## `channel` neden unique key’de ayrı kolon değil?

Phase 1 `entity_key` channel’ı zaten taşımaktadır.

Örneğin aynı Klaviyo message ID’si Email/SMS branch semantic’inde farklı key üretir.

Ana V3 unique key değiştirilmez.

---

# 22. UPSERT davranışı — exact contract

İlk write:

```text
canonical key K
→ INSERT row
```

Aynı gün aynı entity tekrar Refresh:

```text
canonical key K
→ UPDATE existing row
```

Yeni duplicate row oluşturulmaz.

## Conflict sırasında güncellenecek

Aynı real entity’nin yeni provider sonucu geldiğinde en az:

- entity/root/parent names
- metric_support
- raw metrics
- FX provenance
- time provenance
- adapter version
- source confidence
- source_job_id
- raw
- updated_at

güncellenebilir.

## Stable kalacak identity

Conflict key belirleyen:

```text
user_id
platform
platform_account_id
business_date
traffic_type
entity_key
```

değişmez.

`id` de mevcut row’un PK’si olarak korunur.

`created_at` korunur.

`updated_at` yeni write time olur.

## Entity rename

Provider Campaign/Ad/Message adını değiştirirse aynı entity ID/key korunur; `entity_name` güncellenebilir.

Identity name’e bağlı değildir.

---

# 23. Dataset Repository — gerçek Supabase adapter

Phase 1’de yalnız:

```text
DatasetRepository interface
InMemoryDatasetRepository
```

vardır.

PHASE 2’de yeni gerçek adapter oluşturulmalıdır.

Önerilen alan:

```text
funnel-core/supabase-dataset-repository.js
```

veya aynı anlamı taşıyan açık repository klasörü.

Bu detay raporda file name amaçtır; mevcut project naming ile çelişki çıkarsa implementasyon sırasında en minimal isim seçilebilir.

## Adapter sorumlulukları

### Write

```text
canonical row
→ validateCanonicalRow
→ validateEntityHierarchy
→ canonical → DB physical column mapping
→ UPSERT canonical unique key
```

### Read

DB physical row:

```text
performance_dataset_rows_v2
```

okunur ve tekrar Phase 1 Canonical Row şekline map edilir.

Yani Query Service’in DB kolon isimlerini bilmesine gerek kalmaz.

## Mapping owner

Örneğin:

```text
canonical raw_metrics.impression
↔ DB impressions

canonical raw_metrics.ad_click
↔ DB ad_clicks

canonical raw_metrics.spend_value
↔ DB spend
```

mapping **yalnız repository adapter’da** yaşamalıdır.

Formula Engine DB kolon ismi bilmez.

## Client

Backend persistence adapter mevcut server-side:

```text
SUPABASE_URL
SUPABASE_SERVICE_ROLE_KEY
```

ile çalışan server Supabase client mantığıyla uyumlu olacaktır.

Browser/publishable key ile Dataset write yapılmayacaktır.

## PHASE 2’de server.js wiring

Adapter yazılır; fakat Global Refresh’e production write bağlanmaz.

Yani:

```text
server.js existing Refresh
```

Phase 2 nedeniyle Dataset V2’ye yazmaya başlamaz.

Bu wiring Meta Phase 3 ile başlar.

---

# 24. RLS Contract

V2 public schema’da olduğu için RLS açık olacaktır.

```text
ALTER TABLE performance_dataset_rows_v2
ENABLE ROW LEVEL SECURITY
```

## Authenticated SELECT

Policy semantic:

```text
TO authenticated
USING ((select auth.uid()) = user_id)
```

olmalıdır.

Supabase resmi RLS önerileri de `auth.uid()` fonksiyonunu `select` ile sararak statement-level initPlan kullanımını ve policy column’un indexlenmesini önermektedir.

## Browser mutation

V2 final architecture’da frontend raw Dataset’e yazmaz.

Bu nedenle authenticated kullanıcıya:

```text
INSERT
UPDATE
DELETE
```

policy açılmayacaktır.

## Anon

Anon user V2 analytical Dataset’e erişmemelidir.

## Service role

Backend server write/read operations service role ile devam edebilir.

Service role browser’a çıkmaz.

---

# 25. Grant sınırı

Mevcut V1 table’da schema grant’leri geniş olsa da mutation RLS policy bulunmadığı için browser mutation pratikte engellenmektedir.

V2 için yüzey daha net tutulmalıdır.

PHASE 2 hedefi:

```text
authenticated → SELECT
anon          → no analytical Dataset access
service_role  → backend required privileges
```

Authenticated/anon için gereksiz:

```text
INSERT
UPDATE
DELETE
TRUNCATE
```

privilege’ları yeni V2 üzerinde açık bırakılmamalıdır.

Bu, existing V1 tabloyu değiştirmez; yalnız yeni V2’nin daha dar security contract’ıdır.

---

# 26. Minimum Index Strategy

Canlı V1 Dataset bugün 16 index taşımaktadır.

PHASE 2 V2’de bu yapı körü körüne kopyalanmayacaktır.

Kapasite raporu yüksek write hacminde her index’in yazma maliyeti ve storage footprint yarattığını göstermiştir.

Bu nedenle:

> **Önce actual query contract için minimum index. Sonra gerçek Query Performance ölçümüne göre ek index.**

## Index 1 — Canonical unique UPSERT

```text
UNIQUE
(user_id,
 platform,
 platform_account_id,
 business_date,
 traffic_type,
 entity_key)
```

Amaç:

- duplicate engelleme
- UPSERT conflict target

## Index 2 — Main user/date read

```text
(user_id, business_date)
```

Amaç:

Funnel genel date-range query.

## Index 3 — Platform/account scoped read

```text
(user_id,
 platform,
 platform_account_id,
 traffic_type,
 business_date)
```

Amaç:

Provider/platform/account + Paid/Organic scope.

## Index 4 — Entity history/drilldown

```text
(user_id,
 platform,
 platform_account_id,
 entity_key,
 business_date)
```

Amaç:

aynı Campaign/Ad/Asset Group/Message leaf identity tarih aralığı lookup.

## Neden şimdilik daha fazla index yok?

Phase 2’de henüz production Funnel Query API yok.

Aşağıdakiler ölçüm olmadan eklenmez:

- spend sort index
- ROAS sort index
- revenue sort index
- source_job_id index
- ga4_property_id index
- channel-only index
- source_system-only index
- BRIN
- partition indexes

Bunlar gerçek query plan ihtiyacında eklenir.

---

# 27. RLS index ilişkisi

RLS condition:

```text
user_id = auth.uid()
```

olduğu için `user_id` index prefix’leri önemlidir.

Yukarıdaki bütün primary read indexes `user_id` ile başlamaktadır.

Bu, user isolation query planı için bilinçli tercihtir.

---

# 28. Phase 2’de partitioning kararı

PHASE 2’de:

```text
table partitioning
```

yapılmayacaktır.

Neden:

- tablo yeni ve boş,
- gerçek production V2 write/read planları henüz ölçülmedi,
- partition key/retention kararı henüz ürün seviyesinde freeze değil,
- gereksiz erken complexity istemiyoruz.

Kapasite analizi Supabase’in ölçek yolunun bulunduğunu, fakat uzun dönem retain-all storage’ın ayrı ürün/retention kararı olduğunu göstermiştir.

Dataset V2 daily canonical UPSERT olduğu için V1 snapshot-version retention modelinden zaten çok daha kontrollü büyüyecektir.

Partitioning gerçek ihtiyaç doğarsa ayrıca ölçümle ele alınır.

---

# 29. V2 row growth mantığı

V1 Snapshot path:

```text
aynı entity
× her snapshot version
→ yeni evidence/version row
```

V2:

```text
aynı entity
+ aynı business date
→ UPSERT aynı row
```

Bu nedenle normal 4 saatte bir Refresh:

```text
6 Refresh/day
```

olsa bile Dataset V2 aynı günlük entity için 6 persistent version üretmez.

Bu Phase 2’nin storage açısından en önemli farklarından biridir.

---

# 30. Historical/backfill bu fazda neden yok?

62 günlük initial backfill daha sonra ayrı fazdır.

PHASE 2 yalnız schema’nın:

```text
business_date
```

üzerinden geçmiş günleri de taşıyabilecek yapıda olduğunu garanti eder.

Backfill uygulaması yapılmaz.

---

# 31. Migration öncesi safety baseline

Canlı migration uygulanmadan hemen önce implementation sırasında şu baseline tekrar alınmalıdır:

```text
performance_dataset_rows_v2 exists? → false
performance_dataset_rows row count
dashboard_snapshots row count
existing V1 index count
existing RLS enabled states
existing project health
```

Ayrıca GitHub Phase 1 checkpoint commit SHA not edilir.

Amaç:

migration sonrası “ne değişti?” sorusunu net cevaplamaktır.

---

# 32. Migration uygulama sırası

Önerilen exact sıra:

## Step 1 — Repo baseline

- Phase 1 GitHub state doğrula.
- Working copy temiz.
- Existing files hash/diff baseline.

## Step 2 — Migration file

- `supabase/migrations/` oluştur.
- V2 migration SQL yaz.
- Legacy DDL içerilmediğini kontrol et.

## Step 3 — Local/static migration review

Kontrol:

- table name doğru
- column names doğru
- constraints V3 ile uyumlu
- unique key exact
- no derived KPI
- no snapshot_id
- no accidental V1 ALTER/DROP

## Step 4 — Apply migration

Supabase named migration olarak `adstable-dev` project’e uygulanır.

## Step 5 — Schema introspection

Canlı DB’den tekrar:

- columns
- types
- nullable
- constraints
- unique
- indexes
- RLS
- policies
- grants

okunur.

“Migration success” mesajı tek başına acceptance değildir.

## Step 6 — Supabase repository adapter

- physical mapping
- upsert
- read
- round-trip canonical validation

## Step 7 — Controlled integration fixture

DB’de canonical fixture write/read/upsert testleri.

## Step 8 — Cleanup test rows

Phase 2 test fixture rows kalıcı user analytics verisi olarak bırakılmaz.

## Step 9 — Regression check

- V1 row count/structure unchanged
- Snapshot unchanged
- Dashboard unchanged
- Global Refresh unchanged

## Step 10 — Evidence report

Phase 2 kapanış evidence oluşturulur.

---

# 33. Controlled DB fixture stratejisi

PHASE 2’de provider API çağrısı yapılmaz.

Phase 1 fixtures kullanılır.

## Test row prefix

Test entity IDs/names açıkça:

```text
phase2_fixture_...
```

gibi ayrılabilir.

## User identity

Integration test için existing dev user ID configuration üzerinden kullanılabilir.

User ID source code içine hardcode edilmemelidir.

## Cleanup

Test sonunda aynı known entity keys silinir.

PHASE 2 DB fixture satırları gerçek campaign analytics olarak bırakılmaz.

## Güvenli alternatif

Schema-only checks transaction içinde insert/update/rollback ile doğrulanabilir.

Repository integration test ise gerçek Supabase client kullanıyorsa fixture cleanup zorunludur.

---

# 34. PHASE 2 test fixture matrix

Phase 1 fixture seti tekrar DB persistence üzerinden koşturulmalıdır.

## A — Meta Paid

```text
Campaign → AdSet → Ad
```

Beklenen:

- INSERT
- physical mapping doğru
- read-back canonical eşit

## B — Google Standard

```text
Campaign → AdGroup → Ad
campaign_type=standard
```

## C — PMax shape

```text
Campaign → Asset Group
campaign_type=performance_max
parent=NULL
```

Beklenen:

schema redesign gerekmeden yazılır.

## D — TikTok

```text
Campaign → AdGroup → Ad
```

## E — Klaviyo Campaign Email

```text
root=campaign
entity=campaign_message
channel=email
```

## F — Klaviyo Flow SMS

```text
root=flow
entity=flow_message
channel=sms
```

## G — Organic

```text
platform=meta
traffic_type=organic
source_system=ga4
platform_account_id=<Meta account>
ga4_property_id=<GA4 property>
entity=organic
```

## H — real zero

```text
purchase=0
support=supported
```

DB kabul eder.

## I — unsupported

```text
checkout_value=NULL
support=unsupported
```

DB kabul eder.

## J — invalid unsupported zero

```text
checkout_value=0
support=unsupported
```

DB RED.

## K — invalid supported NULL

```text
purchase=NULL
support=supported
```

DB RED.

## L — synthetic

```text
synthetic=true
```

V2 production table RED.

## M — duplicate UPSERT

Aynı exact unique key iki kez yazılır.

İkinci write’ta:

```text
row count = 1
```

kalmalı.

## N — new business date

Aynı entity, farklı business date:

```text
row count +1
```

olmalı.

## O — different entity

Aynı date/account, farklı entity key:

ayrı row.

---

# 35. UPSERT Acceptance Test — exact örnek

İlk write:

```text
key:
user=u1
platform=meta
account=a1
date=2026-08-16
traffic=paid
entity_key=e1

purchase=2
purchase_value=100
```

İkinci write:

aynı key:

```text
purchase=3
purchase_value=150
```

Beklenen:

```text
COUNT(key) = 1
purchase = 3
purchase_value = 150
created_at = ilk insert time
updated_at = ikinci write time
```

Bu test PHASE 2 ana kabul kriteridir.

---

# 36. Metric Support DB tests

Her 10 raw metric için automated pair test olmalıdır.

Örneğin:

## supported number

```text
supported + 0
supported + 5
```

PASS.

## supported NULL

RED.

## unsupported NULL

PASS.

## unsupported number

RED.

## unknown NULL

PASS.

## unknown number

RED.

Bu yalnız JavaScript validator testi değildir.

**Canlı Postgres constraint** de yanlış row’u reddetmelidir.

---

# 37. Organic DB tests

## PASS

```text
traffic_type=organic
source_system=ga4
ga4_property_id=123
channel=NULL
```

## RED

```text
traffic_type=organic
source_system=ga4
ga4_property_id=NULL
```

## RED

```text
traffic_type=organic
source_system=meta_ads
```

## RED

```text
traffic_type=organic
channel=email
```

## RED

```text
platform=ga4
```

---

# 38. Klaviyo channel DB tests

## PASS

```text
platform=klaviyo
traffic_type=paid
source_system=klaviyo
channel=email
```

## PASS

```text
channel=sms
```

## RED

```text
channel=NULL
```

## RED

```text
platform=meta
channel=email
```

---

# 39. RLS acceptance

PHASE 2 RLS acceptance:

## User A

User A kendi row’unu SELECT edebilir.

## User B

User B, A’nın row’unu SELECT edemez.

## Anon

Anon V2 data alamaz.

## Authenticated mutation

Authenticated/browser context V2’ye direct INSERT/UPDATE/DELETE yapamaz.

## Backend

Service-role repository adapter canonical row UPSERT yapabilir.

---

# 40. Query/Index acceptance

Phase 2 production scale benchmark fazı değildir; fakat index kullanım yönü doğrulanmalıdır.

Representative queries:

```text
user + date range
user + platform + account + traffic + date range
user + entity_key + date range
```

için `EXPLAIN` planlarında uygun index’in kullanılabilir olduğu kontrol edilir.

Tam performance tuning PHASE 8 Query/API gerçek workload ile yapılabilir.

---

# 41. `raw` payload size guard

Dataset V2’nin hot store olduğu unutulmamalıdır.

Testlerde repository adapter yanlışlıkla bütün provider response’u `raw` içine kopyalamamalıdır.

Phase 2 acceptance sırasında sample row JSON boyutu kontrol edilmelidir.

Kural:

> `raw` minimal trace/evidence’tir; Snapshot’ın yerine yeni JSON archive değildir.

Bu, kapasite raporundaki raw JSON/storage footprint riskine karşı bilinçli sınırdır.

---

# 42. Phase 2’de Formula Engine ilişkisi

Formula Engine Phase 1’de hazırdır.

V2 yalnız raw facts saklar.

Akış:

```text
DB raw facts
→ Repository read
→ Analysis Scope aggregate
→ Formula Engine
```

DB’ye Formula Engine sonucu yazılmaz.

## Yasak

```text
ctr column
roas column
profit column
margin column
```

ekleyip “kolay olsun” denmeyecektir.

Bu Phase 1/V3 source-of-truth kararını bozar.

---

# 43. Derived cache gelecekte gerekirse

Ana V3 kararına göre performans için derived result persist etmek gerekirse:

- ayrı cache/materialized result olur,
- `formula_engine_version` zorunlu olur,
- raw Dataset V2’nin yerine geçmez.

PHASE 2’de bu cache oluşturulmayacaktır.

---

# 44. Phase 2’de `formula_engine_version` neden V2 raw row’da zorunlu değil?

V2 raw fact source-of-truth’tur.

Row’u üretmek için Formula Engine çalışması gerekmez.

Bu nedenle:

```text
formula_engine_version
```

V2 raw row’un truth version’ı değildir.

V2’de zorunlu engine versions:

```text
canonical_contract_version
adapter_version
time_engine_version
fx_engine_version
```

Formula version derived output/cache tarafına aittir.

---

# 45. Phase 2’de `source_job_id` index neden yok?

Main user-facing query contract:

```text
date/platform/account/entity/scope
```

üzerinden çalışacaktır.

`source_job_id` audit/debug provenance alanıdır.

Gerçek job-based lookup sıklığı ölçülmeden index açılmayacaktır.

Gerekirse sonradan düşük riskle eklenebilir.

---

# 46. Phase 2’de physical FK sınırı

## `user_id`

FK:

```text
public.users(id)
```

uygundur.

## `platform_account_id`

V3 semantic olarak real platform account ID gerektirir.

Ancak PHASE 2’de V2’ye:

```text
(user_id, platform, platform_account_id)
→ platform_ad_accounts
```

strict composite FK koymak zorunlu değildir.

Neden:

- disconnected/lifecycle davranışı,
- GA4 Organic deterministic match timing’i,
- provider adapter transition,
- future account history

henüz V2 production writes üzerinde doğrulanmadı.

Application canonical validator + adapter mapping identity’yi doğrular.

Fiziksel FK Phase 3/7 gerçek write evidence’i sonrası gerekirse eklenebilir.

Bu açık bırakılmış bir integrity eksikliği değil, **transition sırasında yanlış FK ile valid analytics write’ı bloklamama kararıdır.**

---

# 47. PHASE 2’de `source_job_id` FK sınırı

Aynı şekilde:

```text
source_job_id → snapshot_jobs.id
```

strict FK Phase 2’de zorunlu değildir.

PHASE 3 Meta dual-path ile gerçek job ilişkisinin her write için garanti edildiği doğrulanırsa daha sonra eklenebilir.

---

# 48. Migration rollback sınırı

PHASE 2 yeni parallel tablo oluşturduğu için rollback göreceli olarak güvenlidir.

Migration sonrası ciddi structural hata çıkarsa ve Phase 3 production write henüz başlamadıysa:

```text
V2 table/policies/indexes
```

geri alınabilir.

Legacy path bundan etkilenmez.

## Rollback kuralı

Phase 2 sırasında:

- V1 drop edilmediği,
- Snapshot alter edilmediği,
- Refresh V2’ye bağlı olmadığı

için rollback legacy service’i kesmez.

## Phase 3 sonrası

Gerçek provider canonical writes başladıktan sonra V2 drop artık basit rollback değildir.

Bu yüzden Phase 2 acceptance Phase 3’ten önce eksiksiz yapılmalıdır.

---

# 49. PHASE 2’de server.js sınırı

İdeal Phase 2 diff:

```text
server.js = unchanged
```

olmalıdır.

Supabase repository adapter ayrı modül olarak yazılabilir/test edilebilir.

Eğer test harness için minimal import gerekir ise production route davranışı değişmemelidir.

Global Refresh Phase 2’de yeni repository’yi çağırmaya başlamaz.

---

# 50. PHASE 2’de frontend sınırı

Değişmeyecek:

```text
public/dashboard.html
Funnel HTML
login.html
signup.html
```

PHASE 2 DB migration’dır.

UI’da yeni metric, card, switch veya filter oluşturulmaz.

---

# 51. PHASE 2’de mevcut Supabase tabloları için NO-TOUCH listesi

PHASE 2 migration şu tablolara ALTER/DROP yapmamalıdır:

```text
users
platform_connections
platform_ad_accounts
platform_businesses
user_settings
subscriptions
dashboard_snapshots
insight_logs
fx_rates
platform_account_ownerships
snapshot_jobs
snapshot_schedules
performance_dataset_rows
fx_rates_daily
```

`users` yalnız FK target olarak referanslanabilir.

---

# 52. PHASE 2 repository file değişiklikleri — hedef

Expected new project artifacts:

```text
supabase/
  migrations/
    <timestamp>_create_performance_dataset_rows_v2.sql

funnel-core/
  supabase-dataset-repository.js

funnel-core/tests/
  phase2.test.js
```

Gerekirse fixtures küçük ölçüde genişletilebilir:

```text
funnel-core/fixtures/index.js
```

Ama existing Phase 1 behavior bozulmamalıdır.

## Beklenen unchanged

```text
server.js
package.json
public/dashboard.html
```

Package’da `@supabase/supabase-js` zaten bulunduğu için yeni dependency zorunlu değildir.

---

# 53. Phase 1 regression

PHASE 2’den sonra Phase 1 test suite tekrar çalışmalıdır.

Beklenen:

```text
53 / 53 PASS
```

Phase 2 yeni testleri ayrıca PASS olmalıdır.

Phase 1 testlerinden biri bozulursa:

> Phase 2 kapanmaz.

---

# 54. PHASE 2 exact test categories

## 54.1 Static migration tests

- V2 table only
- no V1 drop/alter
- no snapshot_id
- no derived KPI columns
- required raw columns
- required provenance

## 54.2 Schema introspection tests

Canlı DB output:

- column names
- data types
- not-null
- check constraints
- FK
- unique
- indexes

raporla birebir.

## 54.3 Canonical round-trip

```text
Canonical JS row
→ Supabase physical row
→ DB read
→ Canonical JS row
```

semantic equality.

## 54.4 UPSERT

same key → one row.

## 54.5 NULL/metric support

DB constraint tests.

## 54.6 Hierarchy

all provider shapes persist.

## 54.7 RLS

own/select isolation.

## 54.8 Regression

legacy unchanged.

---

# 55. PHASE 2 acceptance criteria — detaylı

PHASE 2 ancak aşağıdakilerin tamamı doğruysa kapanır.

## A. Table

- `performance_dataset_rows_v2` var.
- Yeni ve ayrı tablo.
- V1 aynı.

## B. Identity

- user/platform/account/date/traffic/entity key mevcut.
- `snapshot_id` yok.
- GA4 platform değil.
- channel contract var.

## C. Hierarchy

- Meta Ad.
- Google Standard Ad.
- Google PMax Asset Group.
- TikTok Ad.
- Klaviyo Campaign Message.
- Klaviyo Flow Message.
- Organic.
- fake generic levels gerekmiyor.

## D. Raw facts

- 10 canonical raw metric fiziksel mevcut.
- derived KPI kolonları yok.

## E. Metric Support

- jsonb object mevcut.
- 10 metric support status taşınıyor.
- supported number rule.
- unsupported/unknown NULL rule.
- real zero korunuyor.

## F. Organic

- matched platform account ID.
- GA4 property ayrı.
- Organic GA4 source check.
- match olmayan row adapter tarafından yazılmıyor.

## G. Time/FX

- business date.
- source timezone.
- source/target currency.
- positive FX rate.
- FX/time engine version.

## H. Provenance

- canonical contract version.
- adapter version.
- source confidence.
- synthetic=false.
- source job.
- raw minimal evidence.

## I. Unique / UPSERT

Aynı:

```text
user/platform/account/date/traffic/entity_key
```

ikinci write duplicate üretmiyor.

## J. RLS

- enabled.
- authenticated own SELECT.
- anon no access.
- authenticated direct mutation yok.
- service backend works.

## K. Index

- canonical unique.
- user/date.
- account/scope/date.
- entity/date.
- gereksiz V1-style 16-index kopyası yok.

## L. Repository

- real Supabase adapter.
- canonical physical mapping tek yerde.
- write/read round-trip.
- Query Service DB column bilmek zorunda değil.

## M. Regression

- Phase 1 53/53 PASS.
- Snapshot system unchanged.
- Global Refresh unchanged.
- Dashboard unchanged.
- V1 Dataset unchanged.

---

# 56. PHASE 2 başarısız sayılacağı durumlar

Aşağıdakilerden biri varsa PHASE 2 kapanmaz:

- V2’de `snapshot_id` identity varsa
- V1 Dataset alter edilmişse
- CTR/ROAS/Profit gibi derived KPI raw table’a eklenmişse
- unsupported metric 0 olarak yazılabiliyorsa
- supported metric NULL yazılabiliyorsa
- GA4 `platform` olarak kabul ediliyorsa
- Organic row GA4 Property ID’yi `platform_account_id` yapıyorsa
- PMax için fake AdGroup/Ad zorunluysa
- Klaviyo Flow Campaign altına zorlanıyorsa
- Klaviyo Paid channel’sız yazılabiliyorsa
- same canonical key duplicate oluşturuyorsa
- authenticated user başka user satırını okuyabiliyorsa
- browser V2’ye direct mutation yapabiliyorsa
- V2 RLS kapalıysa
- her V1 index körü körüne V2’ye kopyalanmışsa
- repository adapter Formula Engine hesaplaması yapıyorsa
- server.js Global Refresh Phase 2’de production V2 write’a geçirilmişse
- Phase 1 tests bozulmuşsa

---

# 57. PHASE 2 kapanış evidence paketi

Phase tamamlandığında evidence raporu en az şunları göstermelidir:

1. Applied Supabase migration adı/version.
2. New table live schema.
3. V2 column list.
4. Constraint list.
5. Unique key.
6. Index list.
7. RLS enabled.
8. Policy list.
9. Grant summary.
10. V1 row count/structure before/after unchanged.
11. `dashboard_snapshots` unchanged evidence.
12. Canonical → DB → Canonical round-trip fixture.
13. same-key UPSERT test.
14. real zero test.
15. unsupported NULL test.
16. invalid unsupported-zero rejection.
17. Organic identity test.
18. PMax persistence test.
19. Klaviyo Email/SMS persistence test.
20. synthetic rejection.
21. RLS user isolation test.
22. Phase 1 53/53 regression result.
23. New/changed repo file diff.
24. Test fixture cleanup evidence.

---

# 58. PHASE 2’den Phase 3’e geçiş şartı

PHASE 3 Meta adapter’ın ilk gerçek production Dataset V2 writer’ı olacaktır.

PHASE 3 başlamadan önce Phase 2 şu garantiyi vermelidir:

```text
Meta adapter
canonical valid row ürettiğinde

repository.upsertCanonicalRawFacts(row)

çağrısı:
- doğru fiziksel mapping yapar,
- doğru unique key kullanır,
- duplicate üretmez,
- data loss yapmaz,
- metric support semantiğini bozmaz.
```

Yani PHASE 3’ün işi database tasarlamak değil:

> **Mevcut Meta fetch output’unu bu hazır canonical persistence hattına bağlamak** olmalıdır.

---

# 59. PHASE 2 uygulama sırası — operasyon emri

Exact sıra:

```text
1. Live Supabase baseline
2. GitHub Phase 1 baseline
3. supabase/migrations alanı
4. V2 physical schema
5. platform/source/channel checks
6. hierarchy vocabulary/checks
7. metric_support + NULL checks
8. provenance/time/FX
9. canonical unique key
10. minimum indexes
11. RLS + grants
12. migration apply
13. live schema introspection
14. SupabaseDatasetRepository
15. canonical round-trip tests
16. UPSERT tests
17. invalid-row rejection tests
18. RLS tests
19. fixture cleanup
20. Phase 1 regression
21. legacy no-change diff
22. Phase 2 evidence report
```

---

# 60. PHASE 2 — Yönetici özeti

PHASE 2’nin amacı Supabase’i yeniden tasarlamak değildir.

Amaç:

> **Yeni Funnel için tek canonical daily raw fact tablosunu, Phase 1 contract’ının birebir fiziksel karşılığı olarak kurmak.**

Bu fazda:

```text
Snapshot ölmez.
V1 Dataset ölmez.
Dashboard değişmez.
Provider API değişmez.
Funnel değişmez.
```

Sadece yeni yolun database temeli eklenir:

```text
performance_dataset_rows_v2
```

Bu tablo:

- snapshot version’dan bağımsız,
- daily canonical grain,
- capability-aware hierarchy,
- Paid/Organic,
- Klaviyo Email/SMS,
- PMax Asset Group,
- GA4 provenance,
- 0/null/unsupported,
- Time/FX provenance,
- deterministic UPSERT,
- user-scoped RLS

taşır.

PHASE 2 kapanınca ilk gerçek platform olan Meta için artık database sorusu kalmamalıdır.

---

# 61. Bundan sonraki çalışma standardı

Ana plan değişmez:

```text
ANA IMPLEMENTATION RAPORU
→ tüm program

PHASE DETAY RAPORU
→ başlanacak fazın exact görev haritası

PHASE IMPLEMENTATION EVIDENCE
→ gerçekten ne yapıldı / ne test edildi / ne değişti
```

PHASE 2 kapanışından sonra:

```text
PHASE 3 — Meta ilk gerçek adapter
```

için yeni Detay Uygulama Raporu hazırlanır.

---

# 62. PHASE 2 nihai uygulama emri

> **V1 ve Snapshot’a dokunmadan `performance_dataset_rows_v2` adlı ayrı canonical raw fact store’u oluştur; Phase 1 Canonical Contract’ın bütün identity/entity/raw/metric-support/time/FX/provenance semantiğini fiziksel schema ve constraint seviyesinde koru; canonical unique key ile same-day same-entity write’ı UPSERT yap; minimum query indexleri ve user-scoped RLS kur; gerçek Supabase repository adapter ile round-trip ve duplicate testlerini tamamla; Phase 1 regresyonunu doğrula; ancak hiçbir provider production write’ını henüz V2’ye bağlama.**
