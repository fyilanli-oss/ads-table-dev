# R6-D5-K3 — Klaviyo Flow ve journey canlı kabul kontrol listesi

## Analist sonucu

Bu paket yeni Klaviyo kodu geliştirmez. Amaç, üç günlük veri olgunlaşma süresi sonunda mevcut Campaign + Flow hattının gerçekten ortak Dataset V2 sözleşmesine uyduğunu kanıtlamaktır. K2 yalnız ilk Campaign satırının fiziksel yazımını kanıtladı; Flow ile journey count/value kapsamını kanıtlamadı.

## Ne zaman çalıştırılır?

Klaviyo Flow gönderiminin kaynak saat dilimindeki iş günü kapanmadan kabul başlatılmaz. Tarih kapanmamışsa sonuç hata değildir; `WAIT_NO_WRITE` olarak kaydedilir.

## Önce salt okunur kapı

1. Canonical Klaviyo bağlantısı, tek doğrulanmış hesap ve encrypted access/refresh tokenlar korunuyor olmalıdır.
2. `Added to Cart`, `Started Checkout` ve `Placed Order` binding'leri provider tarafından yeniden doğrulanmalıdır.
3. Campaign ve Flow Reporting API sonuçları ayrı ayrı görülmelidir.
4. Journey metrikleri count ve value çiftleriyle incelenmelidir.
5. Email maliyet paydasının account-month `recipients`, satır payının row `recipients` olduğu doğrulanmalıdır; `delivered` maliyet paydası değildir.
6. Business date, source timezone, source/target currency ve FX provenance doğrulanmalıdır.
7. K2 Campaign satırında duplicate bulunmamalıdır.

## Karar sınıfları

- Flow tarihi kapanmamışsa: `WAIT_NO_WRITE`.
- Provider Flow sonucu boşsa: verified-empty kaydedilebilir; Flow persistence PASS verilmez.
- Journey stage provider sonucunda yok veya `unknown` ise: değer uydurulmaz, R6-D5 açık kalır.
- Binding, Time, FX veya canonical identity hatasında: fail-closed, Dataset yazımı yok.
- Salt okunur kapı geçerse: kullanıcıdan ayrı açık onay alınır ve yalnız bir kontrollü acceptance çalıştırılır.

## Kontrollü yazım kabulü

- Otomatik retry yapılmaz.
- Mevcut Campaign satırı aynı canonical anahtarla idempotent güncellenebilir.
- Provider'ın döndürdüğü Flow satırı Dataset V2'ye yazılmalıdır.
- Duplicate canonical grup sayısı `0` olmalıdır.
- Connection, account selection ve encrypted tokenlar korunmalıdır.
- AdsTable sentetik Campaign, Flow veya journey satırı üretmez.
- Schedule, backfill ve production activation açılmaz.

## Maliyet ve FX kabulü

Kapalı ayda kaynak para birimindeki Email allocation toplamı canonical aylık plan maliyetine eşit olmalıdır. Her satırın raporlama para birimi karşılığı kendi business date FX provenance'ıyla hesaplanır; farklı günlerin hedef para birimi tutarları tek sabit kurla yeniden üretilmez.

## Done sınırı

Flow persistence ve provider-attributed Added to Cart, Started Checkout ve Placed Order count/value kapsamı canlıda kanıtlanmadan R6-D5 `Done` değildir.
