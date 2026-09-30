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

Dataset V2 Klaviyo satırları gerçek Campaign Message ve Flow Message leaf'leridir. Sabit günlük account maliyeti her mesaj satırına yazılırsa çoğalır; rastgele tek mesaja yazılırsa yanlış sahiplik oluşur; sentetik account/cost satırı üretilirse provider hierarchy bozulur.

Bu nedenle:

- Günlük tahmini maliyet Campaign/Flow message satırlarına dağıtılmaz.
- Maliyet account + business date düzeyinde bir kez tutulur veya effective history'den bir kez türetilir.
- Backend aggregation/Funnel API ilgili günlük maliyeti toplam sonuca yalnız bir kez ekler.
- Dataset message satırlarında düzeltici aktivasyona kadar Email spend `null/unsupported` kalır.
- Cost per sent/open/click/conversion günlük sabit maliyet ile provider performans toplamları üzerinden Formula Engine tarafından türetilir.

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
