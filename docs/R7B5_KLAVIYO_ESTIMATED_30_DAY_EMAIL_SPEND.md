# R7-B5 — Klaviyo Estimated 30-Day Email Spend kararı

## İş çıktısı

Kullanıcı Klaviyo e-posta maliyetini fatura dönemi tarihlerini hesaplamadan tek bir 30 günlük tahmin olarak yönetir. AdsTable bu tahmini günlük sabit gider hâline getirir ve e-posta hacmi nedeniyle artırmaz veya azaltmaz.

## Neden değişiyor?

Önceki `Email Monthly Plan Cost` ve gönderim-hacmiyle dağıtım yaklaşımı, kullanıcının girdiği tahmini sabit platform giderini e-posta kullanımına bağlı değişken maliyet gibi yorumladı. Klaviyo Email Reporting API gerçek e-posta spend'i sağlamaz. Bu nedenle aktif ürün sözleşmesi kullanıcı tahminini provider actual spend gibi göstermemelidir.

## Kullanıcı akışı

- İlk bağlantıda kullanıcı yalnız `Estimated 30-Day Klaviyo Email Spend` değerini girer.
- Başlangıç tarihi kullanıcıdan istenmez; server kayıt gününü kullanır.
- Bitiş tarihi kullanıcıdan istenmez.
- Kullanıcı değeri değiştirmezse aynı tutar ardışık 30 günlük pencerelerde otomatik devam eder.
- `Update spend` yeni değeri kayıt gününde başlatır ve önceki değeri bir önceki gün kapatır.
- `Change value` yanlış girilmiş tarihsel tutarı düzeltir; tarihleri değiştirmez ve yeni dönem başlatmaz.
- SMS maliyetleri dahil edilmez.

## Örnek

İlk değer 29.09 tarihinde 300 USD olarak kaydedilirse:

```text
29.09–28.10: 300 USD / 30 = 10 USD/gün
29.10–27.11: değişiklik yoksa 10 USD/gün devam eder
```

Kullanıcı 25.11 tarihinde değeri 360 USD olarak güncellerse:

```text
29.10–24.11: eski değer 300 USD, günlük 10 USD
25.11–24.12: yeni değer 360 USD, günlük 12 USD
```

## Dataset V2 ve Formula sınırı

Dataset V2 Klaviyo satırları gerçek Campaign Message ve Flow Message leaf'leridir. Günlük account Email maliyeti her mesaja tam olarak kopyalanamaz ve rastgele tek mesaja atanamaz. 2 Ekim 2026 tarihli v2 sözleşmesi önceki “message satırlarına dağıtılmaz” kararını yalnız Email maliyet dağıtımı bakımından supersede eder.

Bu nedenle:

- Günlük Email toplamı `Estimated 30-Day Klaviyo Email Spend / 30` olarak sabittir.
- Aynı provider business date içindeki gerçek Email Campaign Message ve Flow Message satırları, Klaviyo Reporting API `recipients` payıyla bu toplamı paylaşır.
- Recipient hacmi yalnız ağırlığı değiştirir; günlük maliyet toplamını değiştirmez.
- Yuvarlama sonrası leaf spend toplamı günlük account maliyetine tam eşit olur; residual canonical leaf identity sırasındaki son uygun satıra verilir.
- Uygun recipient yoksa sahte leaf oluşturulmaz; leaf spend `null` kalır ve günlük maliyet account-day katmanında bir kez `unallocated` tutulur.
- SMS, MMS ve WhatsApp Email maliyetinden pay almaz.
- Dataset V2 ham provider facts ile leaf'e tahsis edilmiş spend'i saklar; CTR, CPC, ROAS, CPS, Revenue ve Revenue Margin gibi türetilmiş KPI'ları saklamaz.
- Formula Engine önce ham facts ve allocated spend'i istenen kapsamda toplar, sonra formülleri çalıştırır.

## Veri ve migration sınırı

Mevcut `email_monthly_plan_cost` ve `monthly_plan_cost` fiziksel adları tarihsel compatibility alanlarıdır; kullanıcıya gösterilecek ürün adı değildir. Yeni history şeması, güvenli backfill, mevcut tek Dataset V2 satırının kontrollü düzeltmesi, RLS/grant postcheck ve rollback ayrı implementation onayı gerektirir. Bu karar belgesi tek başına production verisini değiştirmez.

## Acceptance özeti

- 300 USD her gün 10 USD üretir.
- Sent/delivered/open/click/conversion günlük tutarı değiştirmez.
- Güncelleme kayıt gününde başlar.
- Düzeltme yeni dönem üretmez.
- Kullanıcı tarih aralığı girmez.
- Aynı günlük maliyet birden fazla message satırında çoğalmaz.
- SMS kapsam dışıdır.

## 3 Ekim 2026 resmî API yeniden doğrulaması

Güncel Klaviyo Reporting API belgesi Campaign ve Flow performansını Klaviyo arayüzüyle 1:1 eşleşen Values/Series raporları üzerinden verir. `recipients` resmî istatistiktir; Campaign sonuçları `campaign_id + campaign_message_id`, Flow sonuçları `flow_id + flow_message_id` zorunlu kimlikleriyle gruplanabilir. Mevcut provider client `recipients` istatistiğini zaten ister; implementation eksikliği bu değerin canonical row input'una taşınmaması ve account-day allocation katmanının bulunmamasıdır.

Resmî kaynaklar:

- https://developers.klaviyo.com/en/reference/reporting_api_overview
- https://developers.klaviyo.com/en/reference/query_campaign_values
- https://developers.klaviyo.com/en/reference/query_flow_values

Bağlayıcı allocation ve formül kuralları `contracts/r7b5-klaviyo-email-cost-allocation-v2.json` dosyasındadır.
