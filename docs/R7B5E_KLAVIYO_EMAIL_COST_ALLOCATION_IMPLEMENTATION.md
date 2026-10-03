# R7-B5-E — Klaviyo Email maliyet dağıtımı implementation brief

## İş çıktısı

Kullanıcının `Estimated 30-Day Klaviyo Email Spend` değeri her kapalı provider business date için 30'a bölünür; o günün gerçek Email Campaign Message ve Flow Message satırlarına Klaviyo `recipients` oranıyla dağıtılır. Günlük toplam çoğalmaz ve hiçbir provider leaf'i uydurulmaz.

## Resmî provider doğrulaması

Kontrol tarihi: 3 Ekim 2026.

- Klaviyo Reporting API, Campaign ve Flow performansını Klaviyo UI ile eşleşen Values/Series raporlarıyla verir.
- `recipients` desteklenen resmî istatistiktir.
- Campaign group-by kimliği `campaign_id + campaign_message_id`; Flow group-by kimliği `flow_id + flow_message_id` zorunlu çiftleridir.
- Campaign endpoint rate limit'i burst `1/s`, steady `2/m`, daily `225/d` olarak belgelenmiştir.
- Query Metrics Aggregates bu iş için kullanılmaz; Klaviyo Campaign/Flow UI performansı send-date esaslı olduğundan Reporting API canonical kaynaktır.

Kaynaklar:

- https://developers.klaviyo.com/en/reference/reporting_api_overview
- https://developers.klaviyo.com/en/reference/query_campaign_values
- https://developers.klaviyo.com/en/reference/query_flow_values

## Mevcut repository gerçeği

- `src/providers/klaviyo/provider-client.js` Reporting API'den `recipients` ister.
- Aynı client canonical message input'una `delivered`, unique click/open ve journey metrics taşır; `recipients` değerini düşürür.
- `src/providers/klaviyo/mapper.js` Email/SMS leaf spend'ini `null/unsupported` bırakır.
- Effective spend history şeması ve Update/Change value davranışı repository'de hazırlanmıştır.
- Canlı K3 provisional kabulünde 1 Campaign Message + 1 Flow Message satırı vardır; leaf spend hâlâ `null/unsupported`tır.
- Production SnapshotJob, automatic retry ve backfill kapalıdır.

## Hedef akış

```text
effective spend history
→ estimated 30-day Email spend / 30
→ same account + provider business date Email rows
→ Reporting API recipients validation
→ source-currency recipient-weighted allocation
→ deterministic rounding residual
→ account-day allocated/unallocated audit
→ canonical FX exactly once
→ Dataset V2 leaf spend
→ aggregate raw facts
→ Formula Engine
```

## Uygulama dilimleri

### E1 — Recipient propagation

- Provider client'taki `statistics.recipients` non-negative finite değer olarak canonical message input'una taşınır.
- `channel=email` olmayan satırlar Email allocation'a girmez.
- Missing/invalid recipients fail-closed olur; delivered değeri recipient yerine kullanılamaz.
- Campaign ve Flow canonical leaf identity korunur.

### E2 — Server-authoritative daily cost

- Workspace, Klaviyo account, source currency ve provider business date browser'dan alınmaz.
- İlgili tarihte etkin spend-history kaydı server-side bulunur.
- Günlük source cost tam precision ile `estimated_30_day_email_spend / 30` hesaplanır.
- Aynı account-day için tek allocation contract/version kullanılır.

### E3 — Allocation ve rounding

- Uygun leaf: Email + aynı provider business date + recipients > 0.
- `row_spend = daily_email_cost × row_recipients / eligible_recipients_total`.
- Canonical leaf identity ascending sıralanır.
- Workspace currency precision'a yuvarlama sonrası residual son uygun satıra eklenir.
- Persist edilen leaf toplamı account-day günlük maliyetine tam eşittir.
- Aynı leaf veya source fact iki kez katkı veremez.

### E4 — Account-day audit

Additive server-only account-day kayıt alanı şu gerçekleri bir kez taşır:

- workspace/provider/account/business date/channel;
- source daily cost ve source currency;
- eligible recipient ve leaf toplamı;
- allocated ve unallocated source amount;
- FX rate/date/provider ve reporting-currency totals;
- allocation contract version/finality;
- reason code.

Uygun recipient yoksa leaf yazılmaz; `allocated=0`, `unallocated=daily_email_cost` ve reason `KLAVIYO_EMAIL_COST_UNALLOCATED_NO_RECIPIENTS` olur.

### E5 — Dataset V2 ve Formula Engine

- Fiziksel Dataset V2 `spend` alanı Klaviyo leaf'inde yalnız allocated spend taşır; metric support `supported` olur.
- Dataset V2'ye `ctr`, `cpc`, `roas`, `cps`, `revenue` veya `revenue_margin` kolonu eklenmez.
- Formula Engine önce aynı scope'taki ham facts ve spend'i toplar.
- `ctr`, `abandoned`, `abandoned_value` maliyetten bağımsızdır.
- `cpc`, `roas`, `cps`, `revenue`, `revenue_margin` allocated spend kullanır.
- Unsupported input veya sıfır denominator `null` üretir.

## Migration ve canlı veri kapıları

1. Repository implementation ve testleri.
2. Salt-okunur production preflight.
3. Ayrı onayla additive account-day audit migration'ı.
4. Migration postcheck: RLS/FORCE RLS, browser grant 0, service-role sınırı.
5. Ayrı onayla application deployment.
6. K3 final reconciliation sonrasında salt-okunur allocation preflight.
7. Ayrı işlem-anı onayıyla mevcut Klaviyo leaf'lerinin tek kontrollü idempotent reconciliation'ı.
8. Dataset/account-day postcheck ve no-double-count kanıtı.
9. SnapshotJob, automatic retry, historical backfill ve Campaign Variation kapsamı kapalı kalır.

## Kabul örnekleri

- Günlük 10 USD; recipients 600/300/100 → spend 6/3/1 USD.
- Recipient hacmi değişse de günlük toplam 10 USD kalır.
- Campaign + Flow leaf toplamı yuvarlama sonrası tam günlük maliyettir.
- Recipient yoksa sentetik leaf yok; account-day unallocated 10 USD'dir.
- SMS/MMS/WhatsApp pay almaz.
- Source allocation tamamlanmadan FX uygulanmaz; aynı para birimi iki kez çevrilmez.
- Duplicate canonical leaf veya ikinci account-day cost katkısı fail-closed olur.

## Bu paketin mevcut durumu

`Analyst brief PASS / implementation pending`. Bu belge kod, migration, provider çağrısı, Dataset V2 write, schedule, backfill veya production activation yapmaz. Bağlayıcı karar `contracts/r7b5-klaviyo-email-cost-allocation-v2.json`dır.
