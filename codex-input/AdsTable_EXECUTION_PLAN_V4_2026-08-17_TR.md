# AdsTable — V4 Execution Plan

**Sürüm:** V4  
**Tarih:** 17 Ağustos 2026  
**Belge türü:** Güncel uygulama, kabul ve takip planı  
**Durum:** Mutabık kalınan execution baseline  

## 0. Belge hiyerarşisi ve kullanım kuralı

Bu belge, aşağıdaki iki belgeyi değiştirmez:

1. **Final analiz raporu:** Tarihli mevcut durum ve karar baseline'ıdır; değiştirilmeden korunur.
2. **V3 Implementation Plan:** Canonical contract, platform semantiği, hierarchy, metric support, time, FX, formula, Dataset V2 ve Funnel API için teknik referanstır.

**V4 Execution Plan**, bu iki kaynağı uygulanabilir epic, task, kabul kapısı ve kanıt yapısına dönüştürür. Günlük ilerleme bu belge üzerinden takip edilir. Teknik contract çatışmasında V3; öncelik, güvenlik, geçiş ve production kabul sıralamasında Final Rapor; iş takibinde V4 esas alınır. Bir çatışma görülürse sessizce yorumlanmaz, karar kaydı açılır.

### 0.1 Planlanan ve gerçekleşen ayrımı

- **Planlanan:** Başlangıçta onaylanan kapsam, bağımlılık, kabul, test ve rollback'tir.
- **Gerçekleşen:** Yalnız repository, canlı sistem ve evidence ile doğrulanmış sonuçtur.
- **Sapma:** Planlanandan farklı yapılan veya yapılmayan her şey, gerekçesi ve etkisiyle kaydedilir.
- Bir task kodu yazıldığı için değil, kabul kriterleri ve evidence tamamlandığı için `Done` olur.
- Plan geçmişe dönük olarak gerçekleşene uydurulmaz; değişiklikler decision log ile versionlanır.

### 0.2 Durum sözlüğü

| Durum | Anlamı |
|---|---|
| `Not started` | İşe başlanmadı. |
| `Ready` | Bağımlılıklar tamam, başlanabilir. |
| `In progress` | Uygulama devam ediyor. |
| `Blocked` | Belgelenmiş dış bağımlılık nedeniyle ilerleyemiyor. |
| `Verification` | Kod tamam; kabul, test ve evidence doğrulanıyor. |
| `Done` | Bütün kabul, test, rollback ve evidence koşulları sağlandı. |
| `Deferred` | Açık karar ve gerekçeyle ileri tarihe taşındı. |

## 1. Değiştirilemez execution prensipleri

1. Proje baştan yazılmayacaktır.
2. Çalışan V1 snapshot/dashboard hattı kontrollü geçiş boyunca korunacaktır.
3. Yeni canonical analytics kaynağı Dataset V2 olacaktır.
4. Business math'in sahibi backend Formula/Compare/Intent katmanlarıdır.
5. Funnel UI presentation-only olacaktır.
6. Güvenlik ve ownership kontrolleri provider ingest'ten önce tamamlanacaktır.
7. Provider geçişleri feature flag, dual-write, parity ve rollback ile yapılacaktır.
8. Backfill resumable, idempotent ve ölçülebilir olacaktır.
9. Legacy retirement en son yapılacaktır.
10. Production GO yalnız ölçülebilir kabul kapılarıyla verilecektir.

### 1.1 Monolit büyütmeme kuralı

V4 başladıktan sonra yeni Funnel, provider adapter, OAuth, analytics ve job business logic'i doğrudan kök `server.js` veya inline `public/dashboard.html` içine eklenemez.

### 1.2 Dokunurken çıkarma kuralı

Her epic dokunduğu eski davranışı hedef modüle taşır; fakat ilgisiz alanlarda big-bang refactor yapmaz. Taşıma characterization testleriyle başlar, delegation/feature flag ile devreye alınır ve parity kanıtlanmadan eski uygulama silinmez.

### 1.3 Zorunlu task aynası

Her task aşağıdaki alanları eksiksiz taşımalıdır:

```markdown
## [EPIC]-[TASK] İş başlığı

### Amaç
### Mevcut durum
### Planlanan durum
### Kapsam
### Kapsam dışı
### Bağımlılıklar
### Uygulama adımları
### Kabul kriterleri
### Test planı
### Rollback planı
### Gözlemlenebilirlik
### Güvenlik ve veri etkisi
### Planlanan
### Gerçekleşen
### Sapmalar
### Evidence
### Durum
```

Alanlardan biri uygulanabilir değilse silinmez; `Uygulanamaz — gerekçe` yazılır.

## 2. V3 gerçekleşme haritası

| V3 fazı | Planlanan | Doğrulanmış gerçekleşen | V4 kararı |
|---|---|---|---|
| Phase 1 | Funnel Core iskeleti | Canonical contract, hierarchy, analysis scope, Formula/Time/FX servisleri, repository arayüzü ve Query Service uygulanmış; yerel senaryolar mevcut | **Tamamlandı — koruma/regresyon kapsamı** |
| Phase 2 | Dataset V2 migration | Migration, corrective migration ve Supabase repository uygulanmış; canlı tablo mevcut | **Kod artefaktı tamam; canlı kabul E2'de açık** |
| Phase 3 | Meta adapter | Provider→canonical→V2 production vertical slice | **Açık — E4** |
| Phase 4 | Google adapter | Standard mapping; PMax contract hazırlığı | **Açık — E5; PMax gerçek adapter kapsamına yükseltildi** |
| Phase 5 | TikTok adapter | Gerçek metrics ve synthetic ayrımı | **Açık — E6** |
| Phase 6 | Klaviyo adapter | Campaign/Flow/Message ve Email/SMS | **Açık — E7** |
| Phase 7 | GA4 Organic | Property/domain/timezone/currency/provenance | **Açık — E8** |
| Phase 8 | Funnel API | Authenticated, scope-aware backend output | **Açık — E10** |
| Phase 9 | Funnel backend binding | Mock/API flag ve presentation-only UI | **Açık — E11** |
| Phase 10 | Parity/geçiş | Uçtan uca zincir doğrulaması | **E4–E12 boyunca zorunlu kapı** |

> Phase 1 ve Phase 2'nin `Done` işareti yalnız kendi önceki faz sınırları içindir. Canlı DB kabulü, runtime ingest ve production binding'in tamamlandığı anlamına gelmez.

### 2.1 Canonical provider envelope ve capability-aware hierarchy — V4 freeze

V3 §10.2'nin ana kararı yalnız entity seviyelerinin farklılığı değildir. **Meta, Google, TikTok, Klaviyo ve GA4 kaynaklı Organic dahil bütün adapter'ların aynı canonical envelope'a normalize edilmesidir.** Bu standart provider'ların farklı API şekillerini Dataset V2, Formula Engine, Funnel API ve UI için tek dile çeviren mimari omurgadır. Bir adapter'ın bu envelope dışına çıkması provider-specific şemaları yeniden bütün katmanlara sızdırır ve sistemi başlangıç noktasına döndürür.

Bu nedenle aşağıdaki envelope V4'te E0–E13 boyunca **değiştirilemez cross-cutting contract ve acceptance gate** olarak freeze edilmiştir. Adapter'lar yalnız alanların değerini ve provider capability'sine göre support durumunu belirler; blokları kaldırmaz, yeniden adlandırmaz veya provider'a özel paralel payload üretmez.

Canonical model provider'da bulunmayan bir seviyeyi uydurmaz. Her leaf satır provider'ın gerçekten desteklediği en düşük analytical entity'yi temsil eder; root ve parent lineage açıkça taşınır.

| Capability branch | Zorunlu canonical hierarchy | Yasaklanan sentetik davranış |
|---|---|---|
| Meta Paid | `Campaign → AdSet → Ad` | AdSet'i AdGroup olarak yeniden adlandırmak veya lineage'ı düşürmek |
| Google Standard | `Campaign → AdGroup → Ad` | Campaign/AdGroup lineage'ı olmayan leaf üretmek |
| Google PMax | `Campaign(type=performance_max) → Asset Group` | Sahte AdGroup veya Ad üretmek |
| TikTok Paid | `Campaign → AdGroup → Ad` | Aynı fact'i birden fazla seviyede toplayarak double-count üretmek |
| Klaviyo Campaign | `Campaign → Campaign Message` | Sahte AdGroup/Ad seviyesi üretmek |
| Klaviyo Flow | `Flow → Flow Message` | Flow'u Campaign altına yerleştirmek veya sentetik `Email Flow` parent üretmek |
| GA4 Organic | `Platform-level Organic identity` | Organic satırı Campaign/AdGroup/Ad altına zorlamak |

#### Tek standart canonical envelope

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

Envelope her adapter için aynı yedi bloğu taşır: `identity`, `entity`, `raw_metrics`, `metric_support`, `currency`, `time`, `provenance`. `identity.date` ile `time.business_date` aynı canonical business date'i ifade eder. `identity.source_system` ile `provenance.source_system` aynı olmalıdır. Her raw metric anahtarı karşılık gelen bir `metric_support` anahtarıyla birlikte bulunur.

Provider'da bir metrik veya entity seviyesi bulunmuyorsa contract değiştirilmez: değer `null`, support durumu `unsupported|unknown` olur ve provenance sebebi açıklar. Gerçek ölçülen `0` ise `supported` olarak korunur. Provider'a özgü ek ham detay gerekiyorsa canonical alanları değiştirmek yerine redacted `provenance.raw_reference` veya versionlı adapter evidence içinde tutulur; Formula Engine bu provider-specific ayrıntıya bağımlı olamaz.

Entity alanlarının geçerli kombinasyonu capability branch tarafından belirlenir. Alanın provider'da bulunmaması durumunda değer `null` kalır; görünen ad, placeholder ID veya sentetik entity ile doldurulmaz. Stable `entity_key`, identity ve hierarchy branch'inden deterministik üretilir; frontend görünen isimlerden identity üretmez.

#### Canonical envelope invariants

- Bütün provider ve GA4 Organic adapter'ları aynı yedi top-level bloğu eksiksiz üretir.
- Platforma özel alternatif raw fact DTO'su Dataset V2 repository sınırını geçemez.
- `source_system`, `traffic_type`, `channel` ve platform kombinasyonu canonical validation'dan geçer.
- On raw metric anahtarının tamamı ve birebir support anahtarları bulunur.
- `supported` metrik finite number taşır; gerçek `0` geçerlidir. `unsupported|unknown` metrik yalnız `null` taşır.
- Time normalization tamamlanmadan `identity.date/time.business_date`; FX tamamlanmadan monetary facts production-ready sayılmaz.
- Organic satır `source_system=ga4` ve `ga4_property_id` provenance taşır; GA4 bir paid platform olarak modellenmez.
- `synthetic=true` production canonical performance olarak Dataset V2'ye yazılamaz.
- Contract, adapter, time ve FX version provenance'ı yeniden üretim ve parity için izlenebilir olur.
- Derived KPI'lar bu envelope'un raw fact kaynağına yazılmaz; aggregate sonrası Formula Engine tarafından hesaplanır.

#### Hierarchy kabul kriterleri

- Adapter output'u ilgili capability branch'in root/parent/leaf kombinasyonunu taşır.
- Aynı provider ID ve branch aynı deterministik `entity_key`i üretir.
- Klaviyo Campaign Message ve Flow Message aynı leaf ID'ye sahip olsa bile branch identity nedeniyle çakışmaz.
- PMax, Klaviyo ve Organic için olmayan canonical seviyeler `null` kalır.
- Dataset V2 round-trip hierarchy ve lineage alanlarını kayıpsız korur.
- Funnel API stable identity ile capability-aware child ilişkisi döndürür.
- Funnel UI yalnız API hierarchy'sini render eder; klasik Ad hierarchy'sine zorlamaz.
- Aggregate yalnız seçilen analytical grain'deki canonical leaf fact'leri toplar; parent/leaf double-count oluşmaz.

#### Hierarchy test kapısı

- Her branch için accepted golden fixture.
- Her yasak sentetik şekil için canonical ve DB rejection fixture'ı.
- Deterministic key ve same-ID/different-branch collision testi.
- Provider raw→canonical→V2 round-trip lineage testi.
- Parent/leaf double-count negatif testi.
- API drilldown ve UI capability render contract/E2E testi.

#### Canonical envelope test kapısı

- Meta, Google Standard, Google PMax, TikTok, Klaviyo Email/SMS ve GA4 Organic için aynı schema validator'a giren golden fixture.
- Eksik top-level blok, eksik metric/support anahtarı ve provider-specific paralel şekil rejection testleri.
- Identity/source/channel, date/business-date ve provenance/source-system cross-field invariant testleri.
- Gerçek `0`, unsupported `null`, unknown `null`, partial provenance ve synthetic rejection testleri.
- Time/FX öncesi ve sonrası envelope testi; dört monetary fact'in birlikte normalize edildiğinin kanıtı.
- Canonical envelope→Dataset V2→canonical envelope kayıpsız round-trip testi.
- Adapter/contract/time/FX version provenance ve replay testleri.

#### Contract rollback ve değişiklik yönetimi

- Hierarchy contract değişikliği adapter içinde sessizce yapılamaz; contract/adapter version artışı ve decision log gerektirir.
- Yeni branch provider/account feature flag ile açılır; eski branch verisiyle aynı aggregate'e version kontrolü olmadan karıştırılmaz.
- Hatalı hierarchy yazımında ilgili adapter version durdurulur, run ID ile etkilenen V2 satırları belirlenir ve doğrulanmış adapter ile yeniden üretilir.
- Rollback hiçbir zaman sahte entity üretmeye veya unsupported seviyeyi `0`/placeholder ile doldurmaya dönemez.
- Canonical blok/alan değişikliği yalnız versionlı contract migration, bütün adapter fixture'ları, Dataset V2 mapper, API contract ve rollback planı birlikte kabul edilirse yapılabilir.
- Tek bir provider ihtiyacı ortak envelope'u sessizce çatallayamaz; yeni capability önce ortak contract decision log'unda değerlendirilir.

### 2.2 Normalization pipeline — V4 freeze

V3 §11–12'deki Time ve FX kararları canonical envelope'un opsiyonel yardımcıları değildir. Bütün provider'lar için production fact oluşma sırası aşağıdaki tek pipeline'dır:

```text
Provider raw response
→ provider adapter mapping
→ canonical identity/entity/support validation
→ provider business-date normalization
→ monetary raw facts için tek-rate FX normalization
→ production canonical validation
→ Dataset V2 upsert
→ scope-aware aggregate
→ Formula/Compare/Intent
→ Funnel API
→ presentation-only UI/export
```

#### Time contract

- Paid satırın `source_timezone` değeri provider account metadata'sından gelir.
- Organic satırın `source_timezone` değeri GA4 Property metadata'sından gelir.
- Server UTC tarihi hiçbir provider'ın business date'i olarak kullanılamaz.
- `identity.date = time.business_date` olmalıdır ve canonical unique identity bu business date'i kullanır.
- Timezone bulunamıyorsa UTC fallback ile production fact üretilmez; satır rejection/evidence akışına gider.

#### FX contract

- FX aggregation ve Formula Engine'den önce uygulanır.
- `spend_value`, `add_to_cart_value`, `checkout_value` ve `purchase_value` aynı satırda aynı rate/date/provider ile normalize edilir.
- Aynı canonical satırın monetary alanları farklı currency veya rate halinde bırakılamaz.
- Cross-currency rate yoksa sentetik `1` kullanılmaz; satır retry/rejection akışına gider.
- Aynı currency durumunda rate `1` gerçek, izlenebilir normalization sonucu olarak taşınır.

#### Pipeline kabul/test/rollback kapısı

- Hiçbir adapter Dataset V2'ye Time/FX ve production canonical validation'ı atlayarak yazamaz.
- Her provider için timezone boundary/DST ve currency fixture'ları bulunur.
- Dört monetary metric'in aynı rate ile dönüştüğü ve source provenance'ın korunduğu test edilir.
- Missing timezone/rate, mixed currency ve invalid rate negatif testleri zorunludur.
- Time/FX engine version değişikliği decision log, version bump, parity ve hedefli replay planı gerektirir.
- Hatalı engine version feature flag ile durdurulur; run/version ile etkilenen facts yeniden üretilir. UTC fallback veya sentetik FX rollback değildir.

### 2.3 Analysis Scope, aggregation ve Formula Engine — V4 freeze

V3 §13–14'teki business math tek backend standardıdır. Dataset V2 yalnız normalized raw fact source-of-truth'tur; UI, export, adapter veya repository ayrı formül motoru olamaz.

#### Analysis Scope contract

```text
PAID:
  funnel_click = paid.ad_click

ORGANIC:
  funnel_click = organic.session

PAID_ORGANIC_BLEND:
  additive paid raw facts + additive organic raw facts
  → derived metrics toplam raw facts üzerinden yeniden hesaplanır

INTENT:
  Paid-only
```

Blend, Paid ve Organic satır KPI'larının ortalaması değildir. Organic yalnız seçili AdsTable platform hesabına deterministic olarak eşleşmiş GA4 facts'ten gelir. En az bir analysis scope aktif kalır.

#### Aggregate-first Formula contract

Önce aynı scope ve grain içindeki on canonical raw metric toplanır; sonra derived değerler hesaplanır:

```text
sales           = purchase_value
abandoned       = max(checkout - purchase, 0)
abandoned_value = max(checkout_value - purchase_value, 0)
ctr             = funnel_click / impressions * 100
cpc             = spend / funnel_click
roas            = sales / spend
cps             = spend / purchase
profit          = sales - spend
margin          = profit / sales * 100
```

Intent Paid-only oranları:

```text
add_to_cart_rate = paid_add_to_cart / paid_ad_click * 100
checkout_rate    = paid_checkout / paid_add_to_cart * 100
abandoned_rate   = paid_abandoned / paid_checkout * 100
purchase_rate    = paid_purchase / paid_checkout * 100
```

#### Formula invariants

- Oranlar toplanmaz veya satır oranlarının ortalaması alınmaz: `SUM(raw numerator) / SUM(raw denominator)` kullanılır.
- Denominator `0`, unsupported veya hesaplanamazsa derived sonuç `null` olur.
- Unsupported/unknown additive input kısmi toplamı sessizce gerçek toplam gibi sunulmaz; support sonucu propagate edilir.
- `sales - spend` canonical adı `profit`tir; `revenue` olarak kalıcılaştırılmaz.
- Campaign ve child facts aynı total içinde double-count edilmez.
- Compare iki period için aynı Formula Engine'i kullanır: `(current - previous) / abs(previous) * 100`; previous `0` ise change `null`dır.
- Different-length period normalization gerekiyorsa tek versionlı backend policy olur.
- Funnel Table, Compare, Intent ve Export aynı Dataset V2/engine output'unu tüketir.

#### Formula kabul/test/rollback kapısı

- Aynı aggregate fixture Formula, API, Compare, Intent, Export ve UI'da aynı sonucu verir.
- Blend aggregate-first sonucu ile yanlış KPI-average sonucu arasındaki negatif test bulunur.
- Zero denominator, unsupported propagation, abandoned floor, profit naming ve hierarchy double-count testleri zorunludur.
- Formula değişikliği `formula_engine_version`, golden parity, decision log ve önceki versiona read rollback gerektirir.
- Frontend veya adapter'da duplicate formula tespit edilirse production acceptance verilmez.

### 2.4 Dataset V2 grain, identity ve transition — V4 freeze

Dataset V2, snapshot geçmişi veya derived sonuç deposu değil, Funnel'ın daily canonical raw fact source-of-truth katmanıdır.

#### Tek canonical grain

```text
1 user
+ 1 platform
+ 1 platform account
+ 1 provider business date
+ 1 traffic type
+ 1 gerçek capability-aware leaf entity
```

Mantıksal unique key:

```text
user_id
+ platform
+ platform_account_id
+ business_date
+ traffic_type
+ entity_key
```

- Aynı key ile refresh yeni satır üretmez; idempotent UPSERT yapar.
- `snapshot_id` canonical identity'ye girmez.
- CTR, CPC, ROAS, CPS, abandoned, profit, margin ve rate'ler raw Dataset V2 facts değildir.
- Derived cache gerekirse Dataset V2'den ayrı olur ve `formula_engine_version` taşır.
- Direct/Others final analytical Dataset grain'ine girmez.

#### Organic account mapping contract

GA4 Organic satırın analytical `platform_account_id` değeri GA4 Property ID değildir; deterministic olarak eşleşmiş AdsTable Meta/Google/TikTok/Klaviyo platform hesabıdır. Gerçek GA4 Property ID `provenance.ga4_property_id` olarak ayrı kalır. Deterministic match yoksa canonical Organic row yazılmaz; unmapped evidence olarak tutulur.

#### V1/V2 transition contract

- Migration boyunca refresh, Legacy Snapshot ve Canonical Dataset V2'ye kontrollü dual-write yapabilir.
- Snapshot capture evidence, job/debug history ve legacy compatibility rolünü korur.
- Dataset V2 Funnel, Paid/Organic/Blend, Compare, Intent ve Export'un yeni source-of-truth'udur.
- Operational Dashboard/Auth/Connect/Account/Refresh/Job lifecycle cutover'dan etkilenmez.
- V1 read ve legacy analysis yalnız parity, consumer-zero ve rollback süresi tamamlanınca E13'te emekli edilir.

#### Dataset kabul/test/rollback kapısı

- Same-key upsert, different date/entity isolation ve concurrent retry testleri zorunludur.
- Organic platform-account/property ayrımı ve unmatched rejection test edilir.
- Raw tabloda derived KPI veya snapshot-version duplication bulunamaz.
- Dual-write run'ında V1 no-change ve V1/V2 raw parity evidence üretilir.
- V2 read flag kapatılabilir; legacy yol stabilizasyon boyunca korunur. Destructive retirement yalnız E13 kapsamındadır.

### 2.5 Backend analysis boundary ve deferred capability — V4 freeze

- Funnel browser'dan Supabase'e doğrudan bağlanamaz; authenticated Funnel API tek analysis boundary'dir.
- API user/account ownership, query bounds, Dataset V2 read, scope-aware aggregate, Formula, Compare ve Intent orchestration'ın sahibidir.
- UI ve Export hazır backend contract'ını tüketir; business anlamını değiştiremez.
- Intent yalnız Paid scope'tur; Organic/Blend facts Intent oranlarına karıştırılmaz.
- Top Selling/Ranking core acceptance değildir. Ürün kararıyla açılırsa ayrı backend Ranking Engine Dataset V2'yi capability-aware tüketir; farklı entity tiplerini sahte Ad üreterek aynı leaderboard'a zorlamaz.
- Klaviyo automatic Email Spend ayrı ürün/araştırma kararıdır; mevcut manual/estimated değer yalnız provenance'ı açık fallback olabilir ve provider gerçek spend ile karıştırılamaz.

Bu boundary'lerden sapma yeni provider ihtiyacı gerekçesiyle epic içinde yapılamaz; versionlı contract/decision log ve bütün consumer parity'si gerekir.

### 2.6 V3 contract → V4 enforcement matrisi

| V3 teknik standardı | V4 freeze/gate | Uygulama epic'leri |
|---|---|---|
| §10.1 Platform/source/channel | Canonical envelope invariants | E0, E4–E8 |
| §10.2 Capability-aware hierarchy | Hierarchy matrix, deterministic identity ve sentetik seviye yasağı | E0, E2, E4–E11 |
| §10.3 Metric Support/NULL | Envelope invariant, support propagation ve UI state | E2, E4–E11 |
| §10.4 Organic account mapping | Deterministic AdsTable account + ayrı GA4 provenance | E2, E8, E9 |
| §10.5–10.7 Klaviyo channel/spend | Ortak envelope, channel ve provenance/fallback ayrımı | E7 |
| §11 Time Engine | Provider/Property timezone ve UTC fallback yasağı | E4–E9 |
| §12 FX Engine | Formula öncesi dört monetary fact için tek rate | E4–E9 |
| §13 Analysis/Formula | Backend-only Paid/Organic/Blend ve versionlı formulas | E10–E11 |
| §14 Aggregation | Aggregate-first, ratio-average ve double-count yasağı | E4–E11 |
| §15 Dataset V2 | Raw source-of-truth, canonical grain ve unique upsert | E2, E4–E10 |
| §16 V1/V2 transition | Dual-write, parity, operational compatibility | E4–E9, E12–E13 |
| §17 Funnel API | Authenticated backend analysis boundary | E10–E11 |
| §18 Compare | Aynı engine, previous-zero `null`, versionlı period policy | E10–E11 |
| §19 Intent/Ranking | Paid-only Intent; Ranking deferred ve capability-aware | E10–E11 |

Bu matris V3 standardının yalnız “referans” olarak kalıp execution task'larında unutulmasını engeller. Bir V3 standardı uygulanırken ilgili V4 gate'in kabul, test, rollback ve evidence maddeleri task aynasına kopyalanır.

## 3. Revize epic mimarisi

| Epic | İçerik | Başlangıç şartı | Bitiş şartı |
|---|---|---|---|
| **E0** | Plan freeze, baseline ve mimari sınırlar | Mutabakat | V4 plan ve modül sınırları onaylı |
| **E1** | OAuth ve session güvenliği | E0 | Güvenlik testleri geçiyor |
| **E2** | Dataset V2 canlı kabulü | E1 | DB/RLS evidence paketi tamam |
| **E3** | Backend modularization foundation | E1 | Yeni işler `server.js` dışında geliştirilebiliyor |
| **E4** | Meta vertical slice | E2 + E3 | Dual-write ve parity kabulü |
| **E5** | Google Standard/PMax adapter | E4 | Google parity kabulü |
| **E6** | TikTok adapter | E4 | TikTok parity kabulü |
| **E7** | Klaviyo adapter | E4 | Campaign/Flow/channel kabulü |
| **E8** | GA4 Organic adapter | E4 | Organic provenance kabulü |
| **E9** | Backfill ve data readiness | İlgili adapter | Coverage/parity eşikleri sağlanmış |
| **E10** | Funnel API | E2 + E3 + gerçek V2 veri | API security/contract kabulü |
| **E11** | Dashboard modularization ve Funnel UI binding | E10 + parity | UI canary kabulü |
| **E12** | Production cutover | E4–E11 | Full production GO |
| **E13** | Legacy retirement ve monolit kapanışı | Stabilizasyon dönemi | V1 consumer sıfır; legacy yüzey kaldırılmış |

### 3.1 Bağımlılık grafiği

```text
E0 → E1 ─┬→ E2 ───────────────┬→ E4 → E5/E6/E7/E8 → E9 ─┐
         └→ E3 ───────────────┤                           │
                              └→ E10 ─────────────────→ E11
                                                           ↓
                                                          E12 → E13
```

E5–E8, Meta referans vertical slice kabul edildikten sonra kapasiteye göre paralel yürütülebilir. E9 her provider için ayrı cursor ve readiness durumu taşır.

## 4. E0 — Plan freeze, baseline ve mimari sınırlar

**Durum:** `Done` — V4 mutabakatıyla baseline oluşturuldu; repository kabulü commit/PR ile kanıtlanacaktır.

### Planlanan işler

- **E0-T1:** V3, Final Rapor ve V4 belge hiyerarşisini freeze et.
- **E0-T2:** Epic/task durum sözlüğünü ve zorunlu task şablonunu kabul et.
- **E0-T3:** `server.js` ve `dashboard.html` sorumluluk envanterini çıkar.
- **E0-T4:** Hedef backend/frontend modül sınırlarını karar kaydına bağla.
- **E0-T5:** Feature flag, evidence ve decision-log isimlendirmesini belirle.
- **E0-T6:** V3 §10.2 yedi bloklu canonical provider envelope ve capability-aware hierarchy matrixini V4 cross-cutting contract olarak freeze et.
- **E0-T7:** Time/FX pipeline, Analysis Scope/Formula/Aggregation, Dataset grain/transition ve backend analysis boundary contract'larını V4 enforcement matrisine bağla.

### Kabul kriterleri

- Tek execution takip belgesi V4'tür.
- V3 teknik referans, Final Rapor baseline olarak korunur.
- Epic bağımlılıkları ve GO/NO-GO sahipleri bellidir.
- Monolit büyütmeme ve dokunurken çıkarma kuralları onaylıdır.
- Her iş zorunlu task aynasını kullanır.
- Her adapter için aynı `identity/entity/raw_metrics/metric_support/currency/time/provenance` envelope'u zorunludur.
- Her provider branch için root/parent/leaf, deterministic key ve yasak sentetik şekiller bellidir.
- V3 §10.1–§19 arasındaki her cross-cutting standardın sahibi, uygulama epic'i ve acceptance gate'i bellidir.

### Test / kontrol

- Markdown link ve başlık kontrolü.
- V3 fazlarının V4 epic'lerinde karşılığı olduğunun izlenebilirlik kontrolü.
- V3 §10.1–§19 contract→V4 enforcement matrisi completeness kontrolü.
- Epic'lerde kabul, test, rollback ve bağımlılık alanlarının varlık kontrolü.

### Rollback

Belge geri alınabilir; V3 ve Final Rapor değişmediği için teknik baseline kaybolmaz. V4 değişikliği yeni sürüm ve decision log ile yapılır, geçmiş sessizce yeniden yazılmaz.

### Bağımlılıklar

Mutabakat dışında bağımlılık yoktur.

### Evidence

- V4 dosyasının repository commit'i.
- PR incelemesi ve mutabakat kaydı.

## 5. E1 — OAuth ve session güvenliği

**Durum:** `Done` — E1-T1–E1-T7 tamamlandı; production OAuth/session güvenliği, fail-closed config, encrypted-only token runtime, plaintext retirement ve CI security regression kapıları kabul edildi.

### Planlanan işler

- **E1-T1 — `Done` — OAuth route envanteri ve threat model:** Tüm start/callback yolları, identity kaynakları, state, replay ve token yazma noktaları çıkarıldı; executable current-state baseline eklendi.
- **E1-T2 — `Done` — Bearer-bound identity:** Aktif OAuth başlangıçları doğrulanmış bearer kullanıcıya bağlandı; legacy query `user_id` reddedildi ve dashboard bearer-authenticated JSON handshake'e geçirildi.
- **E1-T3 — `Done` — Transaction store:** Kısa ömürlü, tek kullanımlık, atomik tüketilen OAuth transaction store; SHA-256 state özeti, 10 dakika TTL, provider/redirect/user bağları ve Klaviyo PKCE taşımasıyla kuruldu.
- **E1-T4 — `Done` — Session elimination:** E1-T3 sonrasında runtime session consumer kalmadığı doğrulandığı için kullanılmayan shared store eklemek yerine Express session katmanı tamamen kaldırıldı. Böylece MemoryStore, known fallback secret, session cookie ve multi-instance affinity riski ortadan kaldırıldı.
- **E1-T5 — `Done` — Unsafe default guard:** Review/test hard-route ve sandbox varsayılanları kapatıldı. Production'daki `UNSAFE_PRODUCTION_CONFIG`, Production scope'undaki `TIKTOK_SANDBOX_ACCESS_TOKEN` değişkeninden kaynaklandı; PR #15'in secret-free structured diagnostic'i yalnız değişken adını gösterdi. Değişken kaldırılıp yeni deployment alındıktan sonra site ve login normale döndü; hiçbir secret değeri loglanmadı ve guard beklendiği gibi fail-closed çalıştı.
- **E1-T6 — `Done` — Token protection:** E1-T6A vault, E1-T6B canlı schema/RLS/grant acceptance, E1-T6C Production activation, E1-T6D backfill ve orphan cleanup ve E1-T6E plaintext nulling tamamlandı. Final Production değerleri encryption enabled = `true`, legacy read enabled = `false`; final DB acceptance 7 connected, 7 encrypted, 0 auth-orphan, 0 missing encrypted, 0 plaintext access ve 0 plaintext refresh sonucunu verdi. Site ve login çalışıyor; legacy read kapalıyken Refresh Completed ve encrypted-only provider runtime acceptance tamamlandı. Plaintext kolonların fiziksel drop işlemi E13 Legacy Retirement kapsamına taşındı.
- **E1-T7 — `Done` — Security regression suite:** Auth, IDOR, tamper, replay ve expiry regression suite hazırlandı; dedicated `test:security` komutu eklendi. Security CI pull request ve `main` push üzerinde production secret veya environment kullanmadan security ve full regression testlerini çalıştırır.

### Kabul kriterleri

- OAuth user kimliği yalnız doğrulanmış bearer context'ten gelir.
- State ve transaction user/provider/redirect bağlamına bağlıdır; bir kez tüketilir ve sürelidir.
- Callback replay, state mismatch, expired transaction ve cross-user tamper reddedilir.
- Uygulama session secret'a ihtiyaç duymaz; unsafe review/sandbox production config fail-fast olarak reddedilir.
- OAuth callback'leri shared transaction store ile multi-instance çalışır ve session affinity gerektirmez.
- Token değerleri response, log ve test artefaktlarında görünmez.

### Test planı

- OAuth start için unauthenticated `401`.
- Query/body user ID tamper negatif testi.
- State mismatch, replay, expiry ve provider mismatch testleri.
- User A transaction'ının User B tarafından tüketilememe testi.
- Session middleware, cookie, secret fallback ve dependency elimination testi.
- OAuth transaction store TTL ve atomic consume integration testi.
- Log redaction testi.

### Rollback planı

- Provider bazlı OAuth feature flag kullanılır.
- Yeni transaction store sorununda yeni bağlantı başlatma kontrollü kapatılır; güvenli olmayan eski identity yoluna dönülmez.
- Mevcut geçerli connection kayıtları korunur.
- Bilinmeyen kritik bir session consumer bulunursa merge durdurulur; merge sonrası yeniden ekleme yalnız açık evidence ve yeni security review ile değerlendirilir. Known fallback secret hiçbir rollback senaryosunda geri getirilmez.

### Bağımlılıklar

- E0.
- Provider callback URL envanteri.

### Evidence

- Threat model.
- Route matrisi.
- Security test çıktıları.
- Redacted production-like OAuth trace.
- Config startup testleri.
- E1-T1 gerçekleşen evidence: `security/oauth-route-inventory.js`, `tests/oauth-security-baseline.test.js` ve `docs/security/E1_T1_OAUTH_SECURITY_BASELINE.md`.
- E1-T2 gerçekleşen evidence: `security/oauth-access.js`, bearer/tamper/unauthenticated acceptance testleri ve `public/dashboard.html` authenticated OAuth handshake'i.
- E1-T5 corrective evidence: `UNSAFE_PRODUCTION_CONFIG` nedeninin Production scope'undaki `TIKTOK_SANDBOX_ACCESS_TOKEN` olduğu PR #15'in secret-free structured diagnostic'iyle, secret değeri loglanmadan belirlendi. Değişken Production'dan kaldırıldı; yeni deployment sonrasında site ve login normale döndü. E1-T5 guard doğru şekilde fail-closed çalıştı.
- E1-T3 gerçekleşen evidence: `security/oauth-transaction-store.js`, atomik transaction migration'ı ve replay/expiry/provider/redirect/PKCE testleri.
- E1-T4 gerçekleşen evidence: session-elimination runtime/package guard'ları, pasif Pinterest redirect regresyonu ve güncellenmiş security baseline.

### E1-T4 task aynası

**Planlanan:**
- Production secret fail-fast ve shared TTL session store.

**Gerçekleşen:**
- Aktif runtime session consumer kalmadığı doğrulandı.
- Express session dependency/middleware/cookie bütünüyle kaldırıldı.

**Sapma gerekçesi:**
- Kullanılmayan bir shared store eklemek gereksiz altyapı ve saldırı yüzeyi oluşturacaktı.
- Session elimination aynı güvenlik hedefini daha güçlü ve daha basit biçimde sağlıyor.

**Rollback:**
- OAuth transaction store geri alınmaz; query-controlled identity veya session-bound OAuth state geri getirilmez.
- Kritik bir legacy consumer tespit edilirse değişiklik merge edilmez.
- Merge sonrasında bilinmeyen session consumer bulunursa yalnız açık evidence ve yeni security review ile session altyapısı yeniden değerlendirilir.
- Known development secret fallback hiçbir rollback senaryosunda geri getirilmez.

## 6. E2 — Dataset V2 canlı kabulü

**Durum:** `In progress` — E2-T1/T2/T3/T4/T5 `Done`; E2-T6/T7/T8 `Not started`.

### Planlanan işler

- **E2-T1 — `Done`:** Canlı column/type/nullability introspection.
- **E2-T2 — `Done`:** Constraint, index, policy ve grant drift karşılaştırması.
- **E2-T3 — `Done`:** Canlı canonical round-trip acceptance yönetim sonucu tamamlandı.
- **E2-T4 — `Done`:** Same-key gerçek PostgreSQL upsert ve duplicate kontrolü tamamlandı.
- **E2-T5 — `Done`:** V2 preflight 18/18 PASS; 35 vakalı rollback-only canlı rejection acceptance PASS; mandatory postcheck 15/15 PASS; transaction ve postcheck retry edilmedi, production no-change korundu.
- **E2-T6 — `Not started`:** Canlı operation başlamadı; ortak operator altyapısı ileride yeniden kullanılabilir.
- **E2-T7 — `Not started`:** Canlı operation başlamadı; V2 fixture selector'ları gelecekteki envanterle uyumludur.
- **E2-T8 — `Not started`:** Canlı operation başlamadı.

#### E2-T8 task aynası — fresh-project restore readiness

**Amaç:** Eksik historical SQL’i uydurmadan application-owned current-state baseline capture, disposable Supabase restore ve normalized acceptance için fail-closed repository hazırlığı sağlamak.

**Mevcut durum:** E2-T1/T2 `Done`; E2-T3–T7 ve E2-T8 `Verification`. Ledger 37 olarak reconciled; 31 historical SQL body mevcut değil; altı repository migration’ı bulunuyor; fresh restore doğrulanmadı.

**Planlanan durum:** Ayrı insan onaylı capture ve disposable Supabase restore sonrasında normalized parity evidence’ının review edilmesi; o zamana kadar `Verification`.

**Kapsam:** Scope contract, altı migration classification manifest’i, schema-only capture planı, artifact validator, source inventory, target preflight/acceptance, redacted evidence converter ve executable contract testleri.

**Kapsam dışı:** Supabase/Management API bağlantısı; production schema veya row capture; baseline SQL; target provisioning; restore operatorü/çalıştırması; migration replay; `db push`; deployment ve secret/environment değişikliği.

**Bağımlılıklar:** Onaylı main/checksum, DB ledger baseline manifest, object-by-object capture classification, ayrı capture/restore insan onayları, environment-only credential ve managed primitives doğrulanmış disposable Supabase target.

**Uygulama adımları:** Contract’ı doğrula; fixed schema-only capture planını review et; gelecekte sanitize capture al; validator ve insan review’dan geçir; cutoff ve migration classification’ı kesinleştir; target preflight yap; ayrı restore operatorünü ancak accepted baseline sonrasında hazırla; restore ve read-only acceptance evidence’ını review et.

**Kabul kriterleri:** Exact inventory/checksum; sıfır row/secret/managed DDL; final migration classification ve cutoff; managed primitive preflight; normalized object parity; sıfır application row; human-reviewed redacted evidence ve gerçek fresh-project restore.

**Test planı:** Node artifact/validator/converter unit testleri, tek-statement read-only SQL static kontrolleri, previous E2 regresyonları, full `npm test` ve security suite.

**Rollback:** Bu preparation yalnız repository değişikliğidir ve commit revert ile geri alınır. Gelecekte disposable target failure’ı production’a yönlendirilmez; teardown ayrı onay gerektirir.

**Gözlemlenebilirlik:** Redacted PASS/FAIL, counts ve SHA-256 evidence; raw SQL, project ref, URI, identity veya credential yok.

**Güvenlik/veri etkisi:** Production bağlantısı ve data/schema/ledger/privilege/deployment etkisi yok; actual capture ve restore yok.

**Planlanan:** Scope review, ardından ayrı onaylı capture/classification/cutoff/restore/acceptance zinciri.

**Gerçekleşen:** Scope contract ve capture operator/validator/inventory/acceptance preparation hazır. Actual schema capture yapılmadı; baseline SQL üretilmedi; cutoff kesinleşmedi; target provision edilmedi; restore çalıştırılmadı; fresh restore doğrulanmadı; production değişmedi.

**Sapmalar:** Actual baseline olmadan restore operatorü hazırlanmadı. Altı migration bilinçli olarak `pending_capture_checksum` ve replay-disabled kaldı.

**Evidence:** `artifacts/dataset-v2-acceptance/e2-t8-restore/`, `docs/security/E2_T8_RESTORE_READINESS_RUNBOOK.md`, `docs/security/sql/E2_T8_*.sql`, `security/e2-t8-restore-contract.js`, `scripts/e2-t8-*.js`, `tests/e2-t8-restore-readiness-artifacts.test.js`.

**Durum:** `Verification` — actual capture, final cutoff/classification, disposable restore ve human-reviewed acceptance tamamlanmadan E2-T8 `Done` değildir.

### Kabul kriterleri

- Canlı DDL migration sözleşmesiyle uyumludur veya drift kapatılmıştır.
- Aynı canonical key ikinci yazımda duplicate değil upsert üretir.
- Geçersiz canonical satırlar DB tarafından reddedilir.
- User yalnız kendi satırını okur; anon okuyamaz; authenticated istemci yazamaz.
- Service-role backend write/read çalışır.
- Test verisi temizlenir ve legacy tablolar değişmez.
- Dataset V2 mapper hiçbir provider için ayrı persistence shape veya alan kaybı üretmez.

### Test planı

- HTTPS Management/Data API ve güvenli SQL introspection evidence.
- Gerçek repository integration testi.
- Constraint table-driven negatif testleri.
- İki izole kullanıcıyla RLS testi.
- Exact count öncesi/sonrası ve cleanup testi.
- Migration static testleri.

### Rollback planı

- V2 henüz production read source yapılmaz.
- Destructive migration uygulanmaz; corrective migration ileri yönlüdür.
- Acceptance fixture'ları namespaced run ID ile silinir.
- V1/snapshot hattı değişmeden kalır.

### Bağımlılıklar

- E1 güvenli identity/ownership temeli.
- Supabase HTTPS Management API erişimi.
- İzole test kullanıcıları ve service-role test harness'i.

### Evidence

`artifacts/dataset-v2-acceptance/<run-id>/` altında schema, constraint/index, RLS, round-trip, upsert, rejection, cleanup ve legacy-no-change kanıtları.

### E2-T1/T2 task aynası — 2026-08-24 metadata acceptance

**Amaç:** Canlı Dataset V2 column, constraint, index, RLS, policy ve grant sözleşmesini yalnız read-only metadata ile repository migration'larına karşı doğrulamak.

**Mevcut durum:** Ledger reconciliation tamamlandı ve ledger 37 kayıtta. Dataset V2 tablosu canlıda mevcut fakat satır sayısı sıfır.

**Planlanan durum:** Redacted, deterministic ve executable testlerle korunan E2-T1/T2 evidence paketinin review ve merge edilmesi.

**Kapsam:** Beş allowlist SELECT/WITH SELECT amacıyla column, constraint/index, semantic fingerprint, RLS/policy/grant ve ledger/safe-state doğrulaması.

**Kapsam dışı:** Dataset write, fixture, round-trip, upsert, rejection, iki kullanıcı RLS matrisi, cleanup, V1/snapshot mutation ve runtime/UI değişikliği. E2-T3–T7 açık kalır.

**Bağımlılıklar:** E1 güvenlik postcondition'ları, tamamlanan ledger reconciliation ve açık E2-T8 restore-readiness takibi, Management API read-only erişimi ve repository baseline commit'i.

**Uygulama adımları:** GitHub main ve migration checksum doğrulandı; canlı metadata beş read-only query amacıyla yeniden okundu; repository/live contract karşılaştırıldı; redacted evidence ve contract testi üretildi.

**Kabul kriterleri:** 47 kolon; PK + user FK + 19 check; beş fiziksel index; sıfır invalid/unvalidated object; enabled/non-forced RLS; exact authenticated SELECT policy; beklenen role grant'leri; ledger 37; Dataset V2 row count sıfır.

**Test planı:** Dedicated evidence contract testi, full test, security regression, JavaScript syntax, diff/secret/PII kontrolleri.

**Rollback:** Database değişmedi. Repository rollback gerekirse yalnız evidence/plan commit'i revert edilir.

**Gözlemlenebilirlik:** Object adı, checksum/fingerprint, count, boolean, PASS/FAIL, evidence version ve repository commit ile sınırlı.

**Güvenlik ve veri etkisi:** Canlı sorgular read-only; data/schema/ledger/privilege/deployment etkisi yok; credential veya row data evidence'a alınmadı.

**Planlanan:** E2-T1/T2 metadata sözleşmesinin canlı kabul evidence'ı.

**Gerçekleşen:** E2-T1 ve E2-T2 metadata kontrolleri PASS. Ledger reconciliation ve production root/login smoke daha önce tamamlandı. Dataset V2 satır sayısı sıfır olduğundan persistence acceptance yapılmadı.

**Sapmalar:** Yok. E2-T3–T7 özellikle uygulanmadı.

**Evidence:** `artifacts/dataset-v2-acceptance/20260824-metadata-acceptance/` ve `tests/e2-dataset-v2-metadata-evidence.test.js`.

**Durum:** `Done` — E2-T1/T2 evidence PR review ve merge süreci tamamlandı.

### E2-T3A task aynası — canonical round-trip hazırlığı

**Amaç:** Tek bir namespaced Meta paid canonical fixture'ını Dataset V2 fiziksel sözleşmesine map eden, transaction içinde insert/read-back yapan, yedi canonical bloğu kayıpsız karşılaştıran ve zorunlu rollback ile kalıcı veri bırakmayan acceptance paketini hazırlamak.

**Mevcut durum:** E2-T1/T2 metadata evidence merge edildi; Dataset V2 canlı metadata sözleşmesi kabul edildi ve canlı satır sayısı son doğrulamada sıfırdı. E2-T3 canlı write/read operation henüz çalıştırılmadı.

**Planlanan durum:** Ayrı insan onayından sonra exact preflight, tek insert/read/rollback transaction ve read-only postcheck çalıştırılarak redacted round-trip evidence üretilmesi.

**Kapsam:** Meta paid fixture; canonical→physical ve physical→canonical mapper; unsupported/null, supported zero ve positive metric semantiği; identity dışındaki yedi blok; internal eligible-user seçimi; Dataset V2/V1/snapshot/OAuth/token count parity; fail-closed evidence dönüştürme.

**Kapsam dışı:** Canlı operation, ikinci insert/upsert, rejection matrisi, RLS kullanıcı matrisi, commit/cleanup, V1 veya snapshot mutation, runtime/UI, migration/schema/grant/policy, OAuth/token, deployment ve environment işlemleri.

**Bağımlılıklar:** Merge edilmiş E2-T1/T2 evidence, ledger 37 baseline'ı, mevcut canonical validator/entity hierarchy ve Dataset V2 mapper sözleşmesi; canlı operation için ayrıca insan onayı ve uygun auth/public user.

**Uygulama adımları:** Deterministik canonical ve physical fixture üretildi; read-only preflight/postcheck, tek transaction rollback operation, redacted evidence converter, runbook ve executable contract testi eklendi; production credential veya canlı bağlantı kullanılmadı.

**Kabul kriterleri:** Local canonical/physical round-trip exact; tek Dataset V2 insert ve read-back guard'ları; `COMMIT` yok ve zorunlu `ROLLBACK`; korunan relation'larda mutation yok; identity/credential sızıntısı yok; canlı operation ve postcheck tamamlanmadan durum `Done` değil.

**Test planı:** Dedicated E2-T3 artifact testi, full test, security regression, JavaScript syntax, diff ve secret/PII pattern kontrolleri.

**Rollback:** Hazırlık database'i değiştirmez. Repository rollback yalnız E2-T3 artefakt/plan commit'inin revert edilmesidir; gelecekteki canlı operation'ın zorunlu normal sonu transaction rollback'tir.

**Gözlemlenebilirlik:** Run ID, operation status, count, boolean, canonical alan adı, redacted değer ve PASS/FAIL ile sınırlıdır; gerçek identity ve raw production row yasaktır.

**Güvenlik ve veri etkisi:** Bu hazırlıkta data/schema/ledger/privilege/runtime/deployment etkisi yoktur. Hazırlanan operation yalnız Dataset V2'de tek geçici satır oluşturabilir ve aynı transaction içinde rollback eder.

**Planlanan:** Kontrollü production E2-T3 operation ve postcheck evidence'ı.

**Gerçekleşen:** Repository paketi ve local exact mapper/round-trip doğrulaması hazırlandı; canlı SQL çalıştırılmadı ve production user seçilmedi.

**Sapmalar:** Yok. E2-T4–T7 `Not started`, E2-T8 `Verification` olarak açık kalır.

**Evidence:** `artifacts/dataset-v2-acceptance/e2-t3-roundtrip/`, `docs/security/sql/E2_T3_ROUNDTRIP_*.sql`, `docs/security/E2_T3_ROUNDTRIP_RUNBOOK.md`, `scripts/e2-t3-roundtrip-evidence.js`, `tests/e2-t3-roundtrip-artifacts.test.js`.

**Durum:** `Verification` — canlı operation ve postcheck review edilmeden E2-T3 `Done` değildir.

### E2-T4 task aynası — same-key PostgreSQL upsert hazırlığı

**Amaç:** Migration-defined canonical unique key'i paylaşan initial ve updated Meta paid fixture yazımlarının gerçek PostgreSQL `ON CONFLICT DO UPDATE` ile tek satırda sonuçlanmasını, mutable değerlerin güncellenmesini ve zorunlu rollback ile kalıcı veri bırakılmamasını kanıtlayacak acceptance paketini hazırlamak.

**Mevcut durum:** E2-T1/T2 `Done`; E2-T3 repository paketi merge edildi fakat credential bulunmadığından canlı E2-T3 kabulü çalıştırılmadı ve `Verification` kaldı. E2-T4 canlı acceptance henüz çalıştırılmadı.

**Planlanan durum:** Ayrı insan onaylı bir operasyonda read-only preflight, initial insert, exact same-key PostgreSQL upsert, aggregate/redacted evidence, koşulsuz rollback ve read-only postcheck uygulanması.

**Kapsam:** `e2_t4_same_key_v1` namespaced Meta paid A/B fixture'ları; exact canonical conflict target; initial/upsert/final count ve duplicate guard'ları; mutable metric update; identity/hierarchy ve unsupported-null/supported-zero parity; V1/snapshot/OAuth/token no-change; fail-closed evidence.

**Kapsam dışı:** Bu taskta canlı SQL, E2-T3 canlı kabulü, E2-T5 rejection, E2-T6 RLS matrisi, E2-T7 cleanup, runtime/UI, migration, schema, ledger, RLS/policy/privilege, environment, deployment ve gateway işlemleri.

**Bağımlılıklar:** Güncel main, Dataset V2 migration canonical unique index'i, canonical validator/hierarchy/mapper ve ilerideki canlı operation için Management API credential ile ayrı insan onayı.

**Uygulama adımları:** A/B canonical fixture ve updated physical expectation üretildi; read-only preflight/postcheck, rollback-only transaction, evidence converter, runbook ve executable static/contract test eklendi; test zinciri ve security manifest güncellendi.

**Kabul kriterleri:** Conflict target migration ile exact; initial/upsert operation count `1`; final fixture count `1`; duplicate/excess `0`; B mutable değerleri mevcut; identity/hierarchy ve null/zero semantiği korunmuş; korunan relation mutation'ı ve identity/credential sızıntısı yok; final statement `ROLLBACK`; canlı kabul olmadan `Done` yok.

**Test planı:** E2-T4 artifact testi; E2-T3 ve metadata regression testleri; full ve security suite; JavaScript syntax; SQL statement/conflict/mutation kontrolleri; diff ve secret/PII taraması.

**Rollback planı:** Repository preparation database'i değiştirmez. Gelecekteki controlled operation'ın koşulsuz normal sonu `ROLLBACK`tır; residue halinde ad hoc cleanup yetkilendirilmez. Repository rollback yalnız E2-T4 commit revert'idir.

**Gözlemlenebilirlik:** Namespaced fixture alanları, count, boolean, güvenli expected/actual fixture değeri ve PASS/FAIL ile sınırlıdır; production identity, UUID, credential ve raw production row yasaktır.

**Güvenlik ve veri etkisi:** Bu taskta data/schema/ledger/privilege/deployment etkisi yoktur; Management API kullanılmadı ve canlı SQL çalıştırılmadı.

**Planlanan:** Kontrollü rollback-only E2-T4 canlı preflight, same-key upsert ve postcheck evidence'ı.

**Gerçekleşen:** Repository preparation tamamlandı. Canlı preflight, initial insert, same-key upsert ve postcheck çalıştırılmadı. Management API erişimi bu taskta kullanılmadı. Data/schema/ledger/privilege/deployment değişikliği yapılmadı.

**Sapmalar:** Yok. E2-T3 `Verification`; E2-T5–T7 `Not started`; E2-T8 `Verification` kalır.

**Evidence:** `artifacts/dataset-v2-acceptance/e2-t4-upsert/`, `docs/security/sql/E2_T4_UPSERT_*.sql`, `docs/security/E2_T4_UPSERT_RUNBOOK.md`, `scripts/e2-t4-upsert-evidence.js`, `tests/e2-t4-upsert-artifacts.test.js`.

**Durum:** `Verification` — repository preparation canlı acceptance yerine geçmez.

### E2-T5 task aynası — rollback-only Dataset V2 rejection matrisi hazırlığı

**Amaç:** Dataset V2 migration CHECK ve NOT NULL sözleşmelerinin 35 invalid canonical vaka için PostgreSQL seviyesinde fail-closed reddini, güvenli diagnostics ve zorunlu outer rollback ile kanıtlayacak preparation paketini hazırlamak.

**Mevcut durum:** E2-T1–T5 `Done`; E2-T5 V2 canlı acceptance tamamlandı ve approval capsule tüketildi.

**Planlanan durum:** Tamamlandı — exact read-only preflight, tek intact rollback-only transaction, redacted evidence conversion ve read-only scalar postcheck kabul edildi.

**Kapsam:** `e2_t5_rejection_v2`; 32 CHECK ve üç NOT NULL vaka; valid canonical baseline; migration-derived closed constraint sets; static inserts; nested exception subtransactions; safe SQLSTATE/constraint/column diagnostics; `pg_temp` evidence; Dataset V2/V1/snapshot/OAuth/token/ledger parity.

**Kapsam dışı:** Migration/schema/ledger/RLS/policy/grant/privilege değişikliği, persistent DDL, cleanup, runtime/UI, environment, deployment ve E2-T6/T7 uygulaması. Canlı operation yalnız onaylı rollback-only E2-T5 V2 acceptance ile sınırlıydı.

**Eski kapsam dışı kaydı:** Management API ve canlı SQL preparation aşamasında kapsam dışıydı; kabul aşamasında ayrı production onayıyla kullanıldı.

**Korunan sınırlar:** E2-T3/T4 tekrar edilmedi; E2-T6/T7 uygulanmadı; schema, ledger, RLS, policy, grant, privilege, runtime, UI, environment ve deployment değiştirilmedi.

**Bağımlılıklar:** Onaylı main `135c9e880dd6db22059175977a3c2850ebe079fa`; Dataset V2 create ve Klaviyo corrective migration checksum'ları; canonical validator/hierarchy/repository sözleşmeleri; tamamlanan ayrı insan onayı, environment-only credential ve bütün preflight stop gate'leri.

**Uygulama adımları:** Repository paketi merge edildi; full regression 320/320 PASS oldu; read-only preflight 18/18 PASS verdi; repository dışı approval capsule oluşturuldu; açık production onayıyla 35 ayrı exception bloğu taşıyan transaction bir kez gönderildi; final `ROLLBACK` uygulandı; mandatory postcheck 15/15 PASS verdi; capsule tüketildi.

**Kabul kriterleri:** Tam 35 unique vaka; SQLSTATE exact; CHECK actual constraint case-specific closed allowlist üyesi ve non-empty; NOT NULL exact column; yanlış/missing/extra/duplicate/accepted/residue/parity sonucu FAIL; tek final response; `COMMIT` yok; final `ROLLBACK`; canlı evidence review tamamlandı.

**Test planı:** Dedicated E2-T5 artifact/converter testi; E2-T3/T4, metadata ve ledger regression'ları; full/security suite; JavaScript syntax, SQL statement/mutation/diagnostic, diff ve secret/PII kontrolleri.

**Rollback planı:** Repository preparation database'i değiştirmez ve commit revert edilebilir. Gelecekteki operation'ın tek yetkili normal sonu outer `ROLLBACK`tır. Unexpected accept outer transaction içinde kalıp fail sayılır ve rollback edilir. Residue halinde retry veya ad hoc cleanup yoktur.

**Gözlemlenebilirlik:** Yalnız case code, expected/actual SQLSTATE, closed expected constraints, actual constraint, expected/actual column ve boolean/count parity alanları; SQLERRM/message/detail/hint/context, raw SQL, production identity/value ve credential yasaktır.

**Güvenlik ve veri etkisi:** Management API yalnız onaylı preflight, rollback-only transaction ve read-only postcheck için kullanıldı. Transaction request 1, retry 0; postcheck request 1, retry 0. Kalıcı data, schema, ledger, privilege veya deployment değişikliği oluşmadı; production count, identity, credential ve raw row commit edilmedi.

**Planlanan:** Tamamlandı — insan onaylı controlled E2-T5 V2 acceptance ve redacted evidence review.

**Gerçekleşen:** Preflight 18/18 PASS; 35-vaka rejection transaction PASS; mandatory postcheck 15/15 PASS; final state `CONSUMED`. Transaction ve postcheck retry edilmedi. Fixture residue ve unexpected acceptance sıfır; korunan parity kapıları PASS.

**Sapmalar:** İlk tasarım exact tek constraint hedefledi; cross-field overlap nedeniyle uygulanabilir değildi. 35 vaka ve schema değişmeden korundu; SQLSTATE exact kaldı; case-specific closed `expected_constraints` kabul edildi. Constraint order kullanılmadı ve allowlist canlı sonuçtan öğrenilmedi.

**Evidence:** `artifacts/dataset-v2-acceptance/e2-t5-rejection/live-acceptance-v2.json`, `artifacts/dataset-v2-acceptance/e2-t5-rejection/`, `docs/security/sql/E2_T5_REJECTION_*.sql`, `docs/security/E2_T5_REJECTION_RUNBOOK.md`, `scripts/e2-t5-rejection-evidence.js`, `tests/e2-t5-rejection-artifacts.test.js`.

**Durum:** `Done` — canlı V2 rejection acceptance, mandatory postcheck, redacted evidence review ve production no-change kabul edildi.

## 7. E3 — Backend modularization foundation

**Durum:** `Not started`

### Hedef yapı

```text
src/
  app.js
  config/
  middleware/
  auth/
  oauth/
  routes/
  services/
  repositories/
  providers/
  jobs/
  funnel/
```

### Planlanan işler

- **E3-T1 — Characterization baseline:** Kritik V1 route/status/response davranışlarını sabitle.
- **E3-T2 — Composition root:** App oluşturma, dependency kurma ve `listen()` işlemini ayır.
- **E3-T3 — Config boundary:** Environment doğrulama ve typed config sınırı kur.
- **E3-T4 — Shared clients:** Supabase/provider client creation'ı merkezi dependency yap.
- **E3-T5 — Middleware boundary:** Auth, access, ownership, error, request ID ve logging'i ayır.
- **E3-T6 — Route registration:** İnce route→validation→authorization→service→repository akışını kur.
- **E3-T7 — OAuth extraction:** E1'de güvenli hale gelen OAuth'u modüle taşı.
- **E3-T8 — Job boundary:** Refresh/snapshot orchestration için test edilebilir job sınırı kur.
- **E3-T9 — Architecture guard:** Yeni business logic'in kök monolite eklenmesini CI kontrolüyle engelle.
- **E3-T10 — Canonical boundary guard:** Provider-specific DTO'nun canonical validator'ı atlayarak repository, Formula Engine veya Funnel API sınırına geçmesini engelle.

### Kabul kriterleri

- App port dinlemeden testte oluşturulabilir.
- Yeni provider/Funnel rotası kök `server.js` içine business logic eklemeden kaydedilebilir.
- Auth ve ownership'in tek canonical uygulaması vardır.
- Handler'lar dependency injection ile test edilebilir.
- Standart error contract ve request correlation vardır.
- Kritik V1 smoke/characterization testleri değişmeden geçer.
- `server.js` sorumluluk ve satır sayısı yeni epic'lerle artmaz.
- Bütün adapter'lar aynı canonical validator ve repository portunu kullanır; provider'a özel paralel analytics pipeline yoktur.

### Test planı

- App boot ve graceful shutdown.
- Route registration ve missing dependency.
- Auth/ownership negatif testleri.
- Error normalization.
- V1 critical route characterization/smoke.
- Import-cycle ve architecture boundary kontrolü.

### Rollback planı

- Her extraction küçük ve bağımsız değişikliktir.
- Route-level delegation/feature flag eski handler'a dönebilir.
- Parity sağlanmadan eski uygulama silinmez.
- DB schema değişikliği bu epic'e dahil edilmez.

### Bağımlılıklar

- E0 mimari kararları.
- E1 güvenli auth/OAuth davranışı.
- Kritik endpoint envanteri.

### Evidence

- Before/after responsibility map.
- Characterization sonuçları.
- Architecture guard çıktısı.
- Route parity raporu.

## 8. E4 — Meta referans vertical slice

**Durum:** `Not started`

### Planlanan işler

- **E4-T1:** Meta provider fixture ve mevcut fetch characterization.
- **E4-T2:** Client/mapper/capabilities/adapter modüllerini `src/providers/meta` altında kur.
- **E4-T2A:** `Campaign → AdSet → Ad` root/parent/leaf lineage ve deterministic entity key mappingini uygula.
- **E4-T2B:** Meta output'unu yedi bloklu canonical envelope'a eksiksiz normalize et; provider DTO'yu adapter sınırının dışına çıkarma.
- **E4-T3:** ATC/Checkout/Purchase count/value mapping ve provenance'ı explicit yap.
- **E4-T4:** Account timezone/currency doğrulaması, Time ve FX servislerini bağla.
- **E4-T5:** Canonical validation ve Dataset V2 idempotent write.
- **E4-T6:** Refresh job retry/idempotency/telemetry.
- **E4-T7:** Kullanıcı/account allowlist ile V1+V2 dual-write.
- **E4-T8:** Provider→canonical→FX→V2→Formula→expected totals parity.

### Kabul kriterleri

- Meta mapping route handler içinde değildir.
- Meta Ad leaf'i Campaign ve AdSet lineage'ını kayıpsız taşır; AdSet semantiği generic AdGroup'a dönüştürülmez.
- Meta aynı ortak envelope validator'ından geçer; eksik/özel paralel shape kabul edilmez.
- Wrong user/account write ownership guard ile reddedilir.
- Retry duplicate üretmez.
- Metric support ve gerçek `0`/`null` semantiği korunur.
- Aynı dönem provider raw, V2 ve Formula output kabul eşiğinde reconciled olur.
- Legacy snapshot sonucu dual-write nedeniyle değişmez.

### Test planı

- Golden fixtures, Campaign/AdSet/Ad lineage, deterministic key ve mapping unit testleri.
- Timezone/DST ve currency/FX testleri.
- Missing/partial metric support testleri.
- Repository integration ve idempotent retry.
- Ownership negatif testi.
- Dual-write legacy no-change ve parity raporu.

### Rollback planı

- `meta_v2_write` provider/account feature flag'i kapatılır.
- V1 snapshot read/write korunur.
- V2 yazıları run/adapter version ile izlenir; hatalı batch hedefli temizlenir.
- Provider fetch değişmeden tutulur; yeni adapter delegation geri alınabilir.

### Bağımlılıklar

- E2 ve E3 `Done`.
- Meta conversion mapping kararı.
- Parity eşiği ve canary account listesi.

### Evidence

- Mapping matrix, fixture sonuçları, dual-write run, parity raporu, rejection/error metrics.

## 9. E5 — Google Standard ve PMax adapter

**Durum:** `Not started`

### Planlanan işler

- **E5-T1:** Conversion action count/value mapping ve provenance.
- **E5-T2:** Gerçek customer currency/timezone.
- **E5-T3:** Standard Campaign→AdGroup→Ad adapter.
- **E5-T4:** PMax Campaign→Asset Group adapter; fake AdGroup/Ad yasağı.
- **E5-T4A:** Standard ve PMax output'larını aynı yedi bloklu envelope'a normalize et; farkı yalnız capability/entity değerlerinde koru.
- **E5-T5:** Time/FX/V2/job/telemetry entegrasyonu.
- **E5-T6:** Dual-write, Standard/PMax ayrı completeness ve parity.

### Kabul kriterleri

- Conversion mapping explicit ve versionlıdır.
- Standard hierarchy provider ile reconciled olur.
- PMax satırı yalnız desteklenen Asset Group capability'sini taşır.
- Standard ve PMax ayrı canonical şema üretmez; aynı envelope ve validator'ı kullanır.
- Unsupported alanlar `null + metric_support` kalır.
- Retry/idempotency, ownership ve legacy no-change testleri geçer.

### Test planı

Golden fixtures, conversion mapping, Standard/PMax hierarchy, timezone/FX, ownership, retry, dual-write ve parity testleri.

### Rollback planı

Standard ve PMax için ayrı flags; V1 korunur; adapter version/run ID ile hedefli geri alma yapılır.

### Bağımlılıklar

E4 referans slice kabulü; Google conversion action ve PMax reporting kararları.

## 10. E6 — TikTok adapter

**Durum:** `Not started`

### Planlanan işler

- **E6-T1:** Production metrics contract'ını resmi provider kaynağıyla freeze et.
- **E6-T2:** Generic conversion→purchase fallback'ini kaldır.
- **E6-T3:** ATC/Checkout/Purchase count/value mapping.
- **E6-T4:** Campaign/AdGroup/Ad double-count önleme.
- **E6-T4A:** `Campaign → AdGroup → Ad` root/parent/leaf lineage ve deterministic entity key mappingi.
- **E6-T4B:** TikTok output'unu aynı yedi bloklu canonical envelope'a normalize et.
- **E6-T5:** Synthetic fallback'i canonical production Dataset'ten ayır.
- **E6-T6:** Time/FX/V2, dual-write ve parity.

### Kabul kriterleri

- Synthetic row gerçek performance olarak görünmez.
- Provider hierarchy toplamları double-count üretmez.
- TikTok Ad leaf'i Campaign ve AdGroup lineage'ını kayıpsız taşır.
- TikTok provider-specific fact şekli adapter sınırını geçmez.
- Unknown/unsupported değerler sıfırlaştırılmaz.
- Ownership, retry, parity ve legacy no-change geçer.

### Test planı

Provider fixtures, Campaign/AdGroup/Ad lineage, deterministic key, synthetic rejection, hierarchy totals, metric support, time/FX, retry ve parity.

### Rollback planı

TikTok V2 flag kapatılır; V1 korunur; synthetic/canonical store ayrımı geriye uyumludur.

### Bağımlılıklar

E4; TikTok production reporting contract kararı.

## 11. E7 — Klaviyo adapter

**Durum:** `Not started`

### Planlanan işler

- **E7-T1:** Tek platform altında `channel=email|sms` contract'ı.
- **E7-T2:** Campaign→Campaign Message mapping.
- **E7-T3:** Flow→Flow Message ayrı root mapping.
- **E7-T3A:** Campaign ve Flow branch'leri için branch-aware deterministic entity key; same leaf ID collision koruması.
- **E7-T3B:** Campaign/Flow ve Email/SMS sonuçlarını aynı yedi bloklu envelope'a normalize et; ayrımı entity/channel değerleriyle taşı.
- **E7-T4:** Open≠Click düzeltmesi ve journey count/value support.
- **E7-T5:** SMS provider spend; unsupported ise `null`, uydurma `0` yok.
- **E7-T6:** Email estimated/manual spend fallback ve provenance.
- **E7-T7:** Klaviyo Organic'i GA4 platform-level olarak ayır.
- **E7-T8:** Time/FX/V2, dual-write ve channel-branch parity.

### Kabul kriterleri

- Campaign/Flow ve Message kimlikleri provider'a izlenebilir.
- Email/SMS gerçek channel ile ayrılır.
- Provider/manual/estimated spend provenance ayrıdır.
- Unsupported journey/spend gerçek sıfır görünmez.
- Organic Campaign/Flow altına dağıtılmaz.
- Campaign Message ve Flow Message branch identity'leri aynı leaf ID durumunda dahi çakışmaz.
- Klaviyo branch/channel farklılıkları paralel canonical şemalar üretmez.

### Test planı

Campaign/Flow fixtures, Email/SMS, open-click negative, spend provenance, unsupported/null, Organic separation, retry ve parity.

### Rollback planı

Channel/branch bazlı flags; mevcut Email spend compatibility path korunur; otomatik pricing ayrı karar olmadan açılmaz.

### Bağımlılıklar

E4; Klaviyo event/spend mapping kararları; matched platform account kuralı.

## 12. E8 — GA4 Organic adapter

**Durum:** `Not started`

### Planlanan işler

- **E8-T1:** Account/property selection compatibility.
- **E8-T2:** Property metadata ve Web Stream discovery.
- **E8-T3:** Domain/site URL match.
- **E8-T4:** Property timezone/currency; UTC fallback yasağı.
- **E8-T5:** Organic source/medium/channel classification.
- **E8-T6:** Session/ATC/Checkout/Purchase/Revenue facts ve support.
- **E8-T7:** Deterministic platform account + ayrı GA4 property provenance.
- **E8-T7A:** Platform-level Organic entity identity; paid root/parent seviyelerinin `null` kalması.
- **E8-T7B:** GA4 verisini paid adapter'larla aynı yedi bloklu envelope'a `traffic_type=organic, source_system=ga4` olarak normalize et.
- **E8-T8:** ORGANIC ve PAID_ORGANIC_BLEND entegrasyonu, dual-write/parity.

### Kabul kriterleri

- Her satır property/domain/source/medium/channel provenance taşır.
- Direct/Others paid platforma zorla yazılmaz.
- Organic satır Campaign/AdGroup/Ad hierarchy'sine zorlanmaz ve sentetik parent taşımaz.
- GA4 ayrı bir analytics schema veya platform oluşturmaz; ortak envelope içinde source system/provenance olarak kalır.
- Yanlış currency veya UTC fallback kabul edilmez.
- Bulunmayan value metriği uydurulmaz.
- Paid+Organic blend aggregate-first Formula Engine ile üretilir.

### Test planı

Property/domain match, platform-level Organic identity, sentetik parent rejection, classification fixtures, timezone/DST, currency/FX, support/null, provenance, Organic/Blend aggregate ve parity.

### Rollback planı

GA4 Organic V2 ve Blend ayrı flag'lerle kapatılabilir; mevcut selection/binding ve V1 yolu korunur.

### Bağımlılıklar

E4; domain match ve classification policy; property metadata erişimi.

## 13. E9 — Backfill ve data readiness

**Durum:** `Not started`

### Planlanan işler

- **E9-T1:** Ürün onaylı tarih aralığı ve provider/account kapsamı.
- **E9-T2:** Gün/platform/account bazında resumable cursor ve checkpoint.
- **E9-T3:** Rate-limit/quota budget ve adaptive retry.
- **E9-T4:** Canonical upsert ile idempotent batch.
- **E9-T5:** Completeness, duplicate, metric support, timezone, FX, freshness ölçümü.
- **E9-T6:** Provider bazlı parity/readiness dashboard'u.
- **E9-T7:** Pause/resume/cancel ve runbook.

### Kabul kriterleri

- Job restart sonrası kaldığı yerden duplicate üretmeden devam eder.
- Her provider/account/date için status ve failure nedeni görülebilir.
- Belgelenmiş coverage/parity/freshness eşikleri sağlanır.
- Rate limit aşımı veri kaybına dönüşmez.
- UI cutover readiness kapısı otomatik raporlanır.

### Test planı

Interrupted resume, same-batch replay, partial failure, quota/rate limit, missing FX, stale data, duplicate ve reconciliation testleri.

### Rollback planı

Backfill pause/cancel edilir; live ingest ayrıdır; run ID/adapter version ile hatalı satırlar hedeflenir; V1 etkilenmez.

### Bağımlılıklar

İlgili provider adapter acceptance; tarih kapsamı; quota ve parity eşikleri.

## 14. E10 — Funnel API

**Durum:** `Not started`

### Planlanan işler

- **E10-T1:** Versionlı `/api/funnel/data` request/response contract.
- **E10-T2:** Bearer auth, user/account ownership; query user ID yasağı.
- **E10-T3:** Date/platform/account/entity ve pagination guard'ları.
- **E10-T4:** Paid/Organic/Blend repository query ve aggregate.
- **E10-T5:** Formula, Compare ve Paid-only Intent backend output'u.
- **E10-T6:** Metric support, currency, contract/engine version metadata.
- **E10-T7:** Freshness, partial ve warnings metadata.
- **E10-T8:** Contract, security, performance ve observability testleri.
- **E10-T9:** V4 §2.1 hierarchy contract'ına göre branch-aware root/parent/leaf ve stable `entity_key` response'u.

### Kabul kriterleri

- UI business math yapmadan response ile render edebilir.
- Cross-user/account sorgu reddedilir.
- Unsupported/unknown hiçbir noktada gerçek `0`a dönüşmez.
- Previous denominator `0` için delta `null` olur.
- Capability-aware stable entity identity döner.
- API olmayan hierarchy seviyesini üretmez ve parent/leaf fact'lerini aynı analytical grain'de double-count etmez.
- Query bounds ve performans bütçesi uygulanır.

### Test planı

Auth/IDOR, ownership, scope, compare-zero, mixed support/currency, her capability branch için hierarchy/drilldown ve deterministic key, double-count negatif, bounds/pagination, empty/partial/stale ve load testleri.

### Rollback planı

API version ve read feature flag; V1 endpoint'leri korunur; breaking contract yeni versiyonla çıkar.

### Bağımlılıklar

E2, E3 ve en az bir kabul edilmiş gerçek provider V2 veri seti; contract freeze.

## 15. E11 — Dashboard modularization ve Funnel UI binding

**Durum:** `Not started`

### Hedef yapı

```text
public/
  dashboard.html
  assets/dashboard/
    bootstrap.js
    api-client.js
    auth-session.js
    state.js
    router.js
    components/
    features/
      connections/
      accounts/
      legacy-dashboard/
      funnel/
```

Framework değişimi bu planın ön koşulu değildir; önce sorumluluk sınırları kurulur.

### Planlanan işler

- **E11-T1:** Kritik dashboard davranışları için browser E2E baseline.
- **E11-T2:** Minimal dashboard shell/bootstrap ayrımı.
- **E11-T3:** Merkezi auth-aware API client ve error/timeout/abort davranışı.
- **E11-T4:** Feature-based state ve component sınırları.
- **E11-T5:** Funnel API→presentation adapter; business math yok.
- **E11-T6:** `0`, `null`, unsupported, unknown, loading, empty, partial, stale ve error state'leri.
- **E11-T7:** Standard/PMax/Klaviyo/Organic capability-aware hierarchy.
- **E11-T8:** Compare/Intent/Export backend contract binding.
- **E11-T9:** `legacy_dashboard|funnel_api_canary|funnel_api_enabled` flags.
- **E11-T10:** Mock/API golden parity, responsive E2E ve canary telemetry.
- **E11-T11:** Yeni UI business logic'inin inline `dashboard.html`a eklenmesini engelleyen architecture guard.

### Kabul kriterleri

- Production Funnel inline script içinde değildir.
- Funnel aggregation/formula/compare/intent frontend'de çalışmaz.
- UI state'leri ve capability hierarchy doğru render edilir.
- UI API'de bulunmayan AdGroup/Ad veya Campaign parent seviyesini üretmez; görünen isimden identity türetmez.
- Merkezi API client auth/error davranışının tek sahibidir.
- Mock/API golden parity ve kritik E2E geçer.
- Eski dashboard feature flag ile geri açılabilir.
- Yeni Funnel işi `dashboard.html` sorumluluk ve satır sayısını büyütmez.

### Test planı

- Presentation adapter unit testleri.
- Null/zero/support state testleri.
- Bütün V4 §2.1 branch'leri için hierarchy component ve forbidden synthetic level testleri.
- Auth expiry, network error, abort, partial/stale testleri.
- Compare/Intent/Export parity.
- Legacy/Funnel flag ve responsive browser E2E.
- Visual regression ve accessibility smoke.

### Rollback planı

- UI flag anında legacy dashboard'a döner.
- V1 ve mock yolu canary/stabilizasyon boyunca korunur.
- Yeni asset yüklenemezse güvenli fallback sunulur.
- Eski inline kod stabilizasyon bitmeden silinmez.

### Bağımlılıklar

- E10.
- Kabul edilmiş provider parity/readiness.
- UI E2E baseline ve feature flag altyapısı.

### Evidence

- Before/after UI responsibility map.
- Golden parity ve E2E sonuçları.
- Canary telemetry.
- Gerekli perceptible değişiklikler için ekran görüntüleri.

## 16. E12 — Production cutover

**Durum:** `Not started`

### Planlanan işler

- **E12-T1:** GO/NO-GO checklist ve sorumlu onayları.
- **E12-T2:** Provider/account yüzdeli canary ramp.
- **E12-T3:** Error, lag, partial, rejection ve parity alertleri.
- **E12-T4:** Backup/restore ve rollback tatbikatı.
- **E12-T5:** Support/incident runbook ve iletişim planı.
- **E12-T6:** Full production enable ve stabilizasyon gözlemi.

### Kabul kriterleri

- Hedef provider'lar coverage/parity eşiklerini belirlenen süre korur.
- API/UI security ve performance SLO'ları sağlanır.
- Backup/restore hedefi ve rollback uygulanarak doğrulanmıştır.
- Canary hata bütçesi aşılmamıştır.
- Product, engineering, security ve operations GO vermiştir.

### Test planı

Canary synthetic checks, smoke/E2E, load, failure injection, flag rollback ve restore rehearsal.

### Rollback planı

UI ve API read flag'leri legacy'ye döner; provider V2 write gerekirse ayrı kapatılır; V1 hattı ve veri korunur; incident evidence saklanır.

### Bağımlılıklar

E4–E11 kapsamındaki hedef provider, readiness, API ve UI kapıları.

## 17. E13 — Legacy retirement ve monolit kapanışı

**Durum:** `Not started`

### Planlanan işler

- **E13-T1:** V1/snapshot consumer envanteri ve sıfırlama.
- **E13-T2:** Read-disable/observe dönemi.
- **E13-T3:** Legacy route/function/inline UI dead-code kaldırma.
- **E13-T4:** `server.js`i composition root seviyesine indirme.
- **E13-T5:** `dashboard.html`ı minimal presentation shell seviyesine indirme.
- **E13-T6:** Dataset retention/audit ve ayrı retirement migration kararı.
- **E13-T7:** Operasyon/runbook/documentation kapanışı.

### Hedef son durum

`server.js` yalnız configuration, dependency composition, app creation, listen ve graceful shutdown taşır. Provider mapping, OAuth business logic, refresh/snapshot orchestration, FX ve Funnel query burada bulunmaz.

`dashboard.html` yalnız shell/root container ve asset bootstrap taşır. API erişimi, business state, aggregation/formula ve provider connection business logic'i inline bulunmaz.

### Kabul kriterleri

- V1 analytics consumer sayısı sıfırdır.
- Rollback/stabilizasyon dönemi tamamlanmıştır.
- Legacy read path kapatılıp gözlenmiştir.
- Kullanılmayan route, function, inline script ve asset kaldırılmıştır.
- Snapshot'ın evidence/operational rolü ve retention kararı belgelidir.
- Ayrı retirement migration ve geri dönüş planı onaylıdır.
- Monolitler yeni sistemin business logic sahibi değildir.

### Test planı

Consumer scan, dead-code/static analysis, full regression, production smoke, restore/rollback doğrulaması ve post-removal observability kontrolü.

### Rollback planı

Read-disable gözleminden önce destructive işlem yoktur. Retirement ayrı migration/release olur; restore noktası ve süreli legacy artifact saklama politikası bulunur.

### Bağımlılıklar

E12 stabilizasyon süresi; V1 consumer sıfır; retention/audit ve rollback onayı.

## 18. Ortak kalite, güvenlik ve evidence kapıları

### 18.1 Her provider için zorunlu zincir

```text
Provider raw sample
→ Tek standart Canonical Envelope
→ Time/FX result
→ Dataset V2 raw fact
→ Formula Engine
→ Funnel API
→ Funnel rendered value
→ Metric Support/NULL sonucu
```

Her halka aynı test periodu ve stable identity ile reconcile edilmelidir.

Her provider satırı ilk canonical halkada aynı yedi bloklu schema validator'dan geçmelidir. Provider'a göre değişen şey envelope değil; identity değerleri, capability-aware entity kombinasyonu, metric support ve provenance'dır.

### 18.2 Minimum CI kapısı

- Unit/core tests.
- Syntax/lint/format.
- Architecture boundary guard.
- Migration static validation.
- Security regression.
- DB/repository integration.
- API contract/security.
- Kritik browser E2E.
- `git diff --check` eşdeğeri whitespace kontrolü.

### 18.3 Evidence standardı

Evidence:

- Run ID, commit SHA, environment ve timestamp taşır.
- Secret, token ve PII içermez.
- Beklenen/gerçek sonucu ve kabul eşiğini gösterir.
- Başarısız sonuçlar silinmez; takip issue'suna bağlanır.
- Canlı veriyi değiştiren test cleanup sonucunu içerir.

### 18.4 GO kriterleri

**Provider ingest GO:** E1 ve E2 tamam; wrong-user/account write reddediliyor.  
**Funnel API GO:** Gerçek V2 veri, freshness ve null/support semantiği doğrulanmış.  
**UI canary GO:** Backfill/parity eşiği, golden parity ve rollback flag'i tamam.  
**Full cutover GO:** Coverage/parity belirlenen süre stabil; telemetry ve restore hazır.  
**Legacy retirement GO:** Consumer sıfır; retention ve geri dönüş planı onaylı.

## 19. Ortak task kayıt örneği

```markdown
## E4-T5 — Meta canonical V2 write

### Amaç
Meta raw sonucunu doğrulanmış user/account kapsamında canonical V2'ye idempotent yazmak.

### Mevcut durum
Meta legacy snapshot hattı çalışıyor; V2 production write yok.

### Planlanan durum
Meta adapter output'u canonical validation sonrası repository ile V2'ye yazılır.

### Kapsam
Validation, ownership, repository upsert, telemetry.

### Kapsam dışı
UI cutover ve legacy retirement.

### Bağımlılıklar
E2, E3, E4-T2–T4.

### Uygulama adımları
1. Ownership context oluştur.
2. Canonical output doğrula.
3. Repository upsert çağır.
4. Result/rejection telemetry üret.

### Kabul kriterleri
- Same-key retry duplicate üretmez.
- Wrong user/account reddedilir.
- Support/null korunur.

### Test planı
Unit mapping, repository integration, ownership negative, retry testleri.

### Rollback planı
`meta_v2_write` flag kapatılır; V1 etkilenmez.

### Gözlemlenebilirlik
Accepted/rejected/upserted counts, latency, adapter version.

### Güvenlik ve veri etkisi
Service-role yalnız backend'de; token/log redaction zorunlu.

### Planlanan
Onaylanan başlangıç kapsamı yazılır.

### Gerçekleşen
Commit, migration ve fiili davranış tamamlanınca yazılır.

### Sapmalar
Yok veya gerekçeli farklar.

### Evidence
CI run, integration run ID, parity raporu.

### Durum
Not started.
```

## 20. Decision log ve plan değişikliği

Her kapsam/sıra/contract değişikliği şu kayıtla yapılır:

| Alan | İçerik |
|---|---|
| Decision ID | `ADR/EXEC-YYYY-NNN` |
| Tarih | Karar tarihi |
| Sahip | Karar sahibi |
| Bağlam | Değişikliği gerektiren bulgu |
| Karar | Seçilen yaklaşım |
| Alternatifler | Reddedilen seçenekler |
| Etkilenen işler | Epic/task/contract listesi |
| Migration/Rollback etkisi | Geri dönüş ve veri etkisi |
| Evidence | Kaynak ve test bağlantıları |

## 21. İlk uygulama sırası

1. E0 repository/PR mutabakatını tamamla.
2. E1 OAuth/session threat model ve characterization ile başla.
3. E1 güvenlik uygulaması ve regresyonlarını kapat.
4. E2 canlı Dataset V2 acceptance paketini üret.
5. E3 composition/config/auth/route sınırlarını kur.
6. E4 Meta referans vertical slice'ı dual-write/parity ile aç.
7. E5–E8 provider'larını kontrollü ilerlet.
8. E9 backfill/readiness'i provider bazında işlet.
9. E10 Funnel API'yi gerçek V2 veriyle kabul et.
10. E11 dashboard modularization ve UI canary'yi tamamla.
11. E12 kontrollü production cutover yap.
12. E13'te consumer sıfırlandıktan sonra legacy ve monolit kapanışını gerçekleştir.

## 22. Nihai mutabakat

Bu V4 plan ile:

- Final Rapor değiştirilmeden baseline olarak tutulur.
- V3 Implementation Plan teknik referans olarak tutulur.
- Phase 1 tamamlanmış core, Phase 2 ise artefaktı tamam/canlı kabulü açık olarak izlenir.
- Meta, Google, TikTok, Klaviyo ve GA4 Organic aynı yedi bloklu canonical provider envelope'una normalize edilir; bu standart hiçbir adapter epic'inde çatallanamaz.
- Time→FX→Dataset V2→aggregate→Formula/Compare/Intent→API→UI işlem sırası hiçbir provider için atlanamaz veya yeniden sıralanamaz.
- Paid/Organic/Blend, aggregate-first formulas, canonical Dataset grain, deterministic Organic account mapping ve backend-only analysis boundary ortak mimari standartlardır.
- Gate 0/E1 güvenlik ve Gate 1/E2 canlı DB kabulü provider ingest'in önündedir.
- `server.js` ve `dashboard.html` yorgunluğu bağımsız kabul/test/rollback/bağımlılıkları olan E3, E11 ve E13 işleriyle yönetilir.
- Big-bang rewrite yapılmaz; monolit büyütülmez ve dokunulan alan güvenli biçimde çıkarılır.
- Her task planlanan/gerçekleşen/sapma/evidence ayrımını taşır.
- Production cutover ve legacy retirement ölçülebilir GO kapıları olmadan yapılmaz.


### E1-T5 task aynası

- Production configuration pure/testable bir modülde merkezileştirildi ve unsafe review/sandbox flag'leri production başlangıcında reddediliyor.
- Google/TikTok review fallback kimlikleri runtime/UI kaynaklarından kaldırıldı.
- TikTok sandbox token yalnız explicit non-production sandbox modunda `X-Sandbox-Access-Token` header'ından kabul ediliyor.
- `/tiktok-test` route matrisi ve güvenli UI varsayılanları otomatik testlerle korunuyor.
- Evidence metin/Markdown ve test çıktılarıyla sınırlıdır; E1-T6 sıradaki pakettir.
- Production incident'ında `TIKTOK_SANDBOX_ACCESS_TOKEN` değişken adı PR #15'in secret-free diagnostic'iyle güvenli biçimde belirlendi; değer kaldırılıp deployment yenilendiğinde site/login düzeldi ve hiçbir secret loglanmadı.

### E1-T6 task aynası — Provider token protection

**Planlanan:** Provider access/refresh token'larını application-level envelope encryption ile korumak; key rotation, backfill, rollback ve plaintext retirement kapılarını tanımlamak.

**Gerçekleşen (E1-T6A foundation):** AES-256-GCM token vault eklendi. Ciphertext; `user_id`, `platform` ve `token_type` AAD bağlamına bağlıdır. Raw token envelope içine yazılmaz. Version ve key ID envelope'da tutulur; önceki key'ler read-only keyring içinde kalabilir ve active key dışındaki envelope'lar rotation adayı olarak işaretlenir.

**Gerçekleşen (E1-T6B — canlı schema/grant acceptance tamamlandı):** İlk `platform_connection_tokens` migration'ı canlıda uygulandı; kolon, primary key, foreign key, envelope constraint, DDL, RLS ve grant acceptance'ı yapıldı. İlk kabulde `service_role` için gerekli CRUD'a ek `REFERENCES`, `TRIGGER` ve `TRUNCATE` yetkileri saptandı. PR #10 ile forward-only corrective migration merge edildi ve canlıda uygulandı. Post-migration kabulünde `service_role` üzerinde yalnız `SELECT`, `INSERT`, `UPDATE`, `DELETE` kaldığı; `anon`, `authenticated` ve `PUBLIC` tablo grantlerinin bulunmadığı; RLS'in enabled ve forced kaldığı doğrulandı. Böylece E1-T6B canlı schema/grant acceptance tamamlandı.

**Gerçekleşen (E1-T6C — Production activation tamamlandı):** Encrypted-only provider runtime kabulü tamamlandı. Final Production değerleri `PROVIDER_TOKEN_ENCRYPTION_ENABLED=true` ve `PROVIDER_TOKEN_LEGACY_READ_ENABLED=false` durumundadır.

**Gerçekleşen (E1-T6D — backfill ve orphan cleanup tamamlandı):** Final kabul 7 connected, 7 encrypted, 0 auth-orphan ve 0 connected-without-encrypted-token sonucunu verdi.

**Gerçekleşen (E1-T6E — plaintext nulling tamamlandı):** Global plaintext access token 0, plaintext refresh token 0 ve herhangi bir plaintext token 0 olarak kabul edildi. Encrypted envelope'lar korunmuştur. Fiziksel plaintext kolon drop işlemi E13 Legacy Retirement kapsamına taşındı.

**Sapma:** İlk backfill'de iki auth-orphan bağlantı görüldü ve guarded cleanup ile kaldırıldı. Production config incident'ı Production scope'undaki `TIKTOK_SANDBOX_ACCESS_TOKEN` nedeniyle oluştu; PR #15 secret-free diagnostic yalnız variable ismini gösterdi ve variable kaldırıldı. Plaintext kolonların fiziksel drop işlemi T6'dan E13 Legacy Retirement kapsamına taşındı.

**Kabul:** Final Production kabulü 7 connected, 7 encrypted, 0 auth-orphan, 0 connected-without-encrypted-token, 0 plaintext access ve 0 plaintext refresh sonucunu verdi. Encryption enabled = `true`, legacy read enabled = `false`; site/login başarılıdır ve legacy read kapalıyken Refresh Completed sonucu alınmıştır. Ciphertext/AAD tamper reddedilir; token veya secret log ve evidence artefaktlarına girmez.

**Rollback:** Güvenli olmayan query-controlled/session-bound OAuth identity yolu geri getirilemez ve active encryption key silinemez. Gerekli eski keyler rotation/rollback süresi boyunca keyring'de korunur; encrypted envelope'lar rollback amacıyla silinmez ve plaintext tokenlar geri yüklenmez. Runtime sorunu olursa yeni bağlantı/refresh kontrollü durdurulur; plaintext identity/token yoluna dönülmez. Fiziksel kolon drop E13 stabilizasyon kapısına kadar uygulanmaz.

**Evidence:** E1-T6B schema/RLS/grant acceptance tamamlandı. Production dry-run 9 eligible; controlled write 7 written / 2 auth-orphan failure verdi ve guarded orphan cleanup sonrasında final 7 connected / 7 encrypted / 0 auth-orphan / 0 missing encrypted kabulü alındı. Plaintext nulling sonrasında global plaintext access ve refresh sayıları 0 oldu. Production encryption `true`, legacy read `false` durumunda encrypted-only Refresh Completed sonucu alındı. PR #15 secret-free diagnostic production config incident'ında yalnız unsafe variable ismini raporladı. E1-T7 dedicated `test:security` komutu ve secretsiz CI kapısı security ve full regression paketlerini başarıyla çalıştırır.

**E1 kapanış evidence:** Production OAuth/session güvenlik kontrolleri, fail-closed production config, encrypted-only token runtime, plaintext retirement ve CI security regression tamamlandı. Site ve login çalışıyor; legacy read kapalıyken Refresh Completed sonucu alındı. E1-T7 security suite deterministik `test:security` komutunda toplandı; CI production secret/environment kullanmadan security ve full regression paketlerini çalıştırır. E1 `Done`.

**Sonraki adım:** Önce DB–Execution Plan drift kontrolü, ardından E2 Dataset V2 canlı acceptance.

**E1-T6D production dry-run acceptance ve write artefaktı (2026-08-19):** `production-token-backfill` GitHub Environment oluşturuldu; 3 variable ve 3 secret provision edildi. `main` üzerindeki `f97f1934a98016f129a1bc79263629c2ec8384fa` commit'i için **Provider token production dry-run** run `32245732566` genel, validation ve production dry-run sonuçları success oldu. Redacted sonuç: 9 scanned, 9 eligible, 0 written, 0 already encrypted, 0 rotation candidate, 0 empty, 0 failed ve `nextCursor=null`; dry-run acceptance tamamlandı. Daha sonraki controlled write 7 kayıt yazdı ve iki auth-orphan kayıtta fail-closed oldu; bu kayıtlar guarded cleanup ile kaldırıldı. Güncel production kabulü 7 connected/encrypted, 0 orphan ve 0 missing encrypted'dır; encrypted runtime refresh kabulü de tamamlanmıştır.


### E2-T6 task aynası — rollback-only Dataset V2 RLS acceptance hazırlığı

**Mevcut durum:** E2-T1/T2 `Done`; E2-T3/T4/T5/T6/T7/T8 `Verification`. E2-T6 repository preparation tamamlandı, canlı acceptance yapılmadı.

**Planlanan durum:** Ayrı insan onayından sonra exact read-only preflight, iki izole eligible kullanıcıyla tek intact rollback-only User A/User B/anon/service-role transaction, redacted evidence conversion ve read-only postcheck; review tamamlanana kadar E2-T6 `Verification`.

**Kapsam:** Exact 16-case RLS matrix, symbolic fixture contract, aggregate-only preflight/postcheck, transaction-local role/JWT claim emülasyonu, yalnız `pg_temp` evidence, ayrı nested authenticated mutation denial blokları, tek redacted response ve zorunlu final `ROLLBACK`.

**Kapsam dışı:** Canlı SQL/RLS acceptance, Management API, migration/schema/policy/grant/ledger/data değişikliği, persistent DDL, cleanup, auth/subscription/connection mutation, environment, deployment ve E2-T7.

**Test planı:** Dedicated E2-T6 artifact/converter testi; E2-T3/T4/T5, metadata ve ledger regression'ları; full/security suite; JavaScript syntax, SQL safety, allowlist ve secret/PII kontrolleri. Static testler canlı PostgreSQL acceptance değildir.

**Rollback:** Repository preparation canlı sistemi değiştirmez. Gelecekteki kontrollü operation'ın koşulsuz normal sonu `ROLLBACK`; residue halinde ad hoc cleanup ve automatic retry yasaktır.

**Gerçekleşen:** Repository preparation tamamlandı. Canlı preflight çalıştırılmadı; canlı fixture yazılmadı; User A/User B/anon/service-role canlı matrisi çalıştırılmadı; canlı postcheck çalıştırılmadı; Management API kullanılmadı; data/schema/policy/grant/ledger/deployment değişmedi.

**Durum:** `Verification` — canlı operation, postcheck ve redacted evidence insan review'ı tamamlanmadan E2-T6 `Done` değildir.


### E2-T7 task aynası — fixture cleanup ve no-change acceptance hazırlığı

**Amaç:** E2-T3–T6 outer rollback işlemleri sonrasında sıfır aggregate fixture residue ve Dataset V2/V1/snapshot ile ledger/OAuth/token/schema/RLS/policy/grant exact no-change kanıtı üretmek.

**Mevcut durum:** E2-T1/T2 `Done`; E2-T3/T4/T5/T6/T7/T8 `Verification`. Repository preparation tamamlandı; canlı evidence ve insan review'ı bekleniyor.

**Planlanan durum:** Ayrı insan onaylı baseline, rollback-only operation serisi, final read-only parity check ve redacted evidence review; tamamlanana kadar `Verification`.

**Kapsam:** Exact T3/T4 ve escaped-prefix T5/T6 aggregate residue, V2/V1/snapshot parity, ledger/OAuth/token/schema/index/RLS/policy/grant ve persistent-object kontrolleri.

**Kapsam dışı:** Canlı SQL, otomatik/ad hoc DELETE, fixture recovery, Management API, data/schema/policy/grant/ledger/environment/deployment değişikliği ve E2-T8 restore doğrulaması.

**Bağımlılıklar:** Merge edilmiş E2-T3–T6 SQL/runbook'ları, metadata evidence, ledger baseline, exact approved main/checksum ve her canlı adım için ayrı insan onayı.

**Uygulama adımları:** Exact source doğrula; baseline gates'i çalıştır; sayımları operator-local tut; ayrı onaylı rollback-only seriyi yürüt; üç placeholder'ı lokal doldur; final check ve converter çalıştır; insan review'ı al.

**Kabul kriterleri:** Dört residue ve total sıfır; V2/V1/snapshot exact; tüm security/metadata parity PASS; persistent object sıfır; redacted evidence PASS.

**Test planı:** Dedicated artifact/converter, önceki E2, metadata, ledger, full/security, syntax, SQL safety, diff ve leak taramaları. Static testler canlı kabul değildir.

**Rollback planı:** Repository değişikliği commit revert ile geri alınır. Canlı acceptance'ın tek normal cleanup'ı transaction outer `ROLLBACK`tır; residue halinde STOP, cleanup yoktur.

**Gözlemlenebilirlik:** Yalnız allowlisted check kodları, boolean sonuçlar ve aggregate residue; production count/row/identity committed evidence'a girmez.

**Güvenlik ve veri etkisi:** Preparation-only; credential/PII/raw row yoktur. Canlı data, schema, policy, grant, ledger veya deployment etkisi oluşmadı.

**Planlanan:** İnsan onaylı canlı baseline, rollback-only seri, final no-change evidence ve review.

**Gerçekleşen:** Repository preparation tamamlandı. Canlı baseline, fixture cleanup ve final check çalıştırılmadı. Management API kullanılmadı. Data/schema/policy/grant/ledger/deployment değişmedi.

**Sapmalar:** Yok; canlı execution bilinçli olarak ayrı onaya bırakıldı.

**Evidence:** Fixture inventory, no-change contract, iki read-only SQL, redacted converter, runbook ve static regression testleri.

**Durum:** `Verification` — canlı final evidence ve insan review'ı olmadan `Done` değildir.

### E2-C1 — Captured provider-token security parity corrective kararı

**Planlanan:** E2-T3–T7 acceptance artefaktlarındaki E1 kapanış anından kalan hardcoded 7/7 provider nüfusunu, production sayısı disclosure etmeden operator-local capture ve exact parity ile değiştirmek; missing/orphan/plaintext kontrollerini zero tutmak.

**Gerçekleşen:** Management API bağlantısı HTTP 201 ile doğrulandı. E2-T3 read-only preflight çalıştı. `CONNECTED_CONNECTIONS` ve `ENCRYPTED_TOKEN_ROWS` hardcoded 7 beklentileri başarısız oldu. Diğer preflight güvenlik/schema kapıları geçti. Actual production sayıları evidence'a veya repository'ye alınmadı.

**Sapmalar:** Değişebilir production provider nüfusu nedeniyle fixed population kabulü güvenlik sözleşmesinden çıkarıldı. Bu corrective paket production data correction değildir; transaction, INSERT ve postcheck çalıştırılmadı, production değişmedi.

**Evidence:** Paylaşılabilir response yalnız captured-baseline parity sonuçlarını ve `missing_encrypted_unchanged`, `orphan_encrypted_unchanged`, `plaintext_unchanged` boolean sonuçlarını taşır. Operator-local connected/encrypted baseline source control'a alınmaz.

**Durum:** E2-T1/T2 `Done`; E2-T3–T8 `Verification` olarak korunur.

### E2-C2 — E2-T3 ordered read-back v2 corrective preparation

**Durum:** E2-T3 `Verification`; E2-T4–T8 durumları değişmedi.

**Gerçekleşen (safe/redacted):** Management API transport HTTP 201 ve updated preflight 17/17 PASS oldu. v1 transaction HTTP 201 döndü; insert/contract PASS, read-back/overall FAIL oldu. PostgreSQL same-statement snapshot semantiği nedeniyle v1 read-back tasarımı geçersizdi. v1 postcheck invalid aggregate projection nedeniyle HTTP 400 döndürdü. v1 transaction retry edilmedi. Ayrı insan-onaylı recovery sorgusu HTTP 201 ve 13/13 PASS verdi; fixture residue zero ve production no-change doğrulandı. Actual count/identity paylaşılmadı.

**Corrective hazırlık:** `e2_t3_static_v2` yeni namespace'i ve `E2_T3_TRANSACTION_V2` operation code'u kullanılır. Tek intact transaction payload'ı ordered top-level temp baseline, INSERT ve ayrı target-table read-back statement'ları ile zorunlu final `ROLLBACK` taşır. Postcheck scalar actual/expected sorgularına çevrildi. v2 eski operation'ın retry'ı değildir; yeni preflight ve ayrı insan onayı zorunludur. Bu corrective task canlı SQL çalıştırmaz; static testler live PostgreSQL acceptance yerine geçmez.

### E2-T4 corrective V2 kaydı — integer evidence ve scalar postcheck

**Durum:** E2-T3 `Done`; E2-T4 `Verification`; E2-T5–T8 durumları değişmedi.

**Canlı v1 bulguları:** v1 preflight HTTP 201 ve 16/16 PASS. v1 transaction HTTP 201; initial write, same-key upsert, final fixture row, updated contract ve duplicate-group PASS; duplicate-excess evidence contract FAIL. Final statement `ROLLBACK`; transaction retry: no. v1 postcheck HTTP 400 ve retry edilmedi.

**Recovery:** read-only recovery HTTP 201 ve recovery 11/11 PASS; fixture residue zero, Dataset V2 zero ve production no-change. Actual production counts and identities were not shared.

**Corrective kapsam:** v2 corrective preparation; `E2_T4_TRANSACTION_V2`, `e2_t4_same_key_v2` ve `e2-t4-upsert-v2`; duplicate excess explicit bigint ve postcheck tamamen scalar actual/expected bigint sözleşmesi. Bu repository taskında canlı SQL veya Management API çalıştırılmadı. E2-T4 `Verification` kalır.

### E2-C3 — E2-T6 fail-closed recovery kaydı

**Durum:** E2-T6 `Verification`; canlı PASS iddiası yoktur.

**Gerçekleşen:** İnsan onaylı E2-T6 v1 transaction bir kez gönderildi; CLI fail-closed durdu. Capsule `CONSUMED`, transaction intent ve postcheck intent kayıtlıdır; ikisi de retry edilmedi. Ayrı insan onaylı distinct read-only recovery sorgusu 19/19 PASS verdi. E2-T6 residue, total E2 residue ve persistent evidence object sıfır; Dataset V2/V1/snapshot, OAuth/token/ledger ve RLS/policy/grant korunan baseline kapıları değişmedi. Production count ve identity paylaşılmadı.

**Sapma:** v1 CLI güvenli kategorik terminal sonucu kalıcılaştırmadığı için transaction evidence failure ile original mandatory postcheck failure birbirinden sonradan ayrıştırılamadı. Recovery production no-change kanıtıdır; 16-case RLS acceptance PASS yerine geçmez. v1 transaction tekrar edilemez. Yeni canlı deneme ancak ayrı namespace/version, düzeltilmiş terminal observability, yeni preflight ve ayrı production onayıyla yapılabilir.

**Evidence:** `artifacts/dataset-v2-acceptance/e2-t6-rls/recovery-v1.json`.

### E2-C4 — E2-T6 corrective v2 terminal observability hazırlığı

**Durum:** Repository preparation; E2-T6 `Verification`, canlı işlem yapılmadı.

**Corrective kapsam:** `e2_t6_rls_v2` ayrı namespace/version; checksum-bound 21-gate preflight, intact rollback-only 16-case transaction ve 19-gate postcheck. V2, transaction evidence ve postcheck sonuçlarını `PASS`, `TRANSACTION_EVIDENCE_FAILED_POSTCHECK_PASS`, `POSTCHECK_FAILED` veya `TRANSACTION_AND_POSTCHECK_FAILED` kapalı safe-code sözleşmesiyle ayrı outcome sidecar'ında kalıcılaştırır. Sidecar repository dışındadır, `0600` modundadır ve raw hata/identity/count içermez. v1 capsule veya namespace tekrar kullanılamaz.

**Canlı sınır:** V2 preparation SQL, Management API veya production işlemi çalıştırmaz. Yeni preflight ve transaction ayrı production onaylarına tabidir.

### E2-C5 — E2-T6 v2 fail-closed recovery kaydı

**Durum:** E2-T6 `Verification`; canlı PASS iddiası yoktur.

**Gerçekleşen:** İnsan onaylı v2 transaction ve mandatory postcheck birer kez gönderildi; terminal outcome `TRANSACTION_AND_POSTCHECK_FAILED` olarak güvenli sidecar'a yazıldı. İkisi de retry edilmedi. Ayrı insan onaylı distinct read-only recovery 19/19 PASS verdi; v2/total residue ve persistent evidence object sıfır, korunan baseline ve RLS/policy/grant kapıları değişmedi.

**Karar:** Recovery production no-change kanıtıdır ve 16-case acceptance PASS yerine geçmez. E2-T7 inventory ve read-only selector'ları v1/v2 T6 namespace'lerini ayrı izler. Yeni canlı deneme hazırlanmayacaktır; transaction SQL root cause repository/static ve disposable ortamda çözülmeden production E2-T6 tekrarına izin verilmez.

**Evidence:** `artifacts/dataset-v2-acceptance/e2-t6-rls/recovery-v2.json`.

### E2-C6 — E2-T6 repository/static root-cause audit

**Durum:** `Verification`; production retry hazırlığı veya canlı PASS iddiası değildir.

**Gerçekleşen:** V2 transaction içindeki iki fixture'ın `entity_key` değerlerinin frozen canonical hierarchy üzerinden üretilmesi gereken anahtarlarla eşleşmediği repository/static olarak doğrulandı. Bu bulgu iki fixture'ı da etkiler ve transaction tasarımında kesin bir contract ihlalidir. Güvenli terminal sidecar ham database/transport hatası taşımadığından tüketilmiş transaction ile postcheck'in terminal hata nedeni geriye dönük olarak kesinleştirilemez; recovery yalnız production no-change durumunu kanıtlar.

**Karar:** Production retry yasağı korunur. Yeni bir namespace/operation hazırlanmasından önce canonical anahtarlı düzeltme ve disposable PostgreSQL ortamında transaction/postcheck reproduksiyonu zorunludur. Bu taskta SQL, Management API, production data, schema, policy, grant, ledger, environment veya deployment değişikliği yapılmadı.

**Evidence:** `artifacts/dataset-v2-acceptance/e2-t6-rls/static-root-cause-v1.json` ve `tests/e2-t6-static-root-cause.test.js`.

### E2-C7 — E2-T6 disposable PostgreSQL root-cause reproduction

**Durum:** Repository/disposable reproduction `Done`; E2-T6 production acceptance hâlâ `Verification`.

**Gerçekleşen:** PostgreSQL 16 disposable şeması historical V2 transaction'ı gerçek parser, role switching, RLS ve nested exception davranışıyla yeniden çalıştırdı. V2 final payload SQL'inde `jsonb_build_object` kapanış parantezinin ve case aggregate'in `pg_temp.e2_t6_rls_evidence` source ifadesinin eksik olduğu doğrulandı. C6'daki canonical `entity_key` ihlali de yeni V3 disposable fixture'larında düzeltildi. Corrected V3 disposable transaction 16/16 case PASS, zero unexpected allow, overall PASS ve final outer `ROLLBACK` verdi.

**Reproduction kapısı:** Redacted runner PostgreSQL 16 ile yerel/disposable ortamda çalıştırıldı; executable schema ve runner repository'de tutulur. Disposable şema yalnız sembolik iki kullanıcı ve boş korunan tablolar içerir; production credential, identity, count veya bağlantı kullanmaz.

**Karar:** Repository/static ve disposable root-cause şartı tamamlandı. Bu sonuç production acceptance değildir. Yeni production preflight/transaction operatörü hazırlanması ve çalıştırılması ayrı task, yeni namespace, review ve açık insan production onayına tabidir.

**Evidence:** `artifacts/dataset-v2-acceptance/e2-t6-rls/disposable-reproduction-v1.json`, `tests/fixtures/e2-t6-disposable-schema.sql` ve `scripts/e2-t6-disposable-reproduction.js`.

### E2-C8 — E2-T6 V3 production operator preparation

**Durum:** Repository preparation; E2-T6 production acceptance `Verification`, canlı işlem yapılmadı.

**Kapsam:** Disposable ortamda doğrulanan canonical V3 transaction; yeni `e2_t6_rls_v3` namespace'i; checksum-bound 21-gate preflight; 16-case rollback-only transaction; 19-gate postcheck; tek kullanımlık `0600` state/outcome sidecar; exact confirmation ve fail-closed terminal outcome sözleşmesi.

**Gerçekleşen:** V3 preflight/transaction/postcheck, fixture contract, evidence converter, operator/CLI ve regression testleri repository'de hazırlandı. Approved-main binding PR #44 merge commit'ine sabitlendi. Bu taskta Management API, production SQL, credential, data, schema, policy, grant, ledger, environment veya deployment değişikliği yapılmadı.

**Canlı sınır:** Preflight dahil hiçbir production isteği review ve açık insan production onayı olmadan çalıştırılamaz. Repository testleri ve disposable PASS production acceptance yerine geçmez.

### E2-C9 — E2-T6 V3 preflight fail-closed diagnostic revizyonu

**Durum:** Repository revision; production preflight retry edilmedi, E2-T6 `Verification`.

**Bulgu:** İnsan onaylı V3 read-only production preflight tek istek sonrasında genel `STOPPED_FAIL_CLOSED` ile durdu. Approval-ready state ve outcome sidecar oluşmadı; transaction/postcheck gönderilmedi ve production mutation olmadı. Genel kod query/transport-response aşaması ile 21-gate validation aşamasını ayırmadığı için kör retry yasaklandı.

**Düzeltme:** V3 preflight, raw hata/count/identity taşımayan kapalı safe-code sözleşmesiyle `PREFLIGHT_QUERY_FAILED` ve `PREFLIGHT_GATES_FAILED` aşamalarını ayırır. Her iki hata state/outcome oluşturmadan durur. Bu repository taskında Management API veya production isteği çalıştırılmadı.

**Canlı sınır:** Revize read-only diagnostic preflight ancak merge/review ve yeni açık insan production onayından sonra tek istek olarak çalıştırılabilir.
