# R6-D5-K3 — Klaviyo Flow ve journey canlı kabul kontrol listesi

## Analist sonucu

Bu paket yeni Klaviyo kodu geliştirmez. Amaç, mevcut Campaign + Flow hattının ortak Dataset V2 sözleşmesine uyduğunu canlı kanıtla doğrulamaktır. K2 yalnız ilk Campaign satırının fiziksel yazımını kanıtladı; Flow persistence ve provider-attributed journey count/value kapsamını kanıtlamadı.

Bu belge, eski recipient bazlı Email maliyet dağıtımı yaklaşımını geçersiz kılar. Campaign ve Flow yapraklarına tahmini maliyet dağıtılmaz.

## Ne zaman çalıştırılır?

Klaviyo Flow gönderiminin kaynak saat dilimindeki iş günü kapanmadan kabul başlatılmaz. Tarih kapanmamışsa sonuç hata değildir; `WAIT_NO_WRITE` olarak kaydedilir.

## Önce salt okunur kapı

1. Canonical Klaviyo bağlantısı, tek doğrulanmış hesap ve encrypted access/refresh tokenlar korunuyor olmalıdır.
2. `Added to Cart`, `Started Checkout` ve `Placed Order` binding'leri aynı provider-verified integration kaynağından doğrulanmalıdır.
3. Campaign ve Flow Reporting API sonuçları ayrı ayrı görülmelidir.
4. Journey metrikleri count ve value çiftleriyle incelenmelidir.
5. Event envanteri yalnız tanı kanıtıdır. `attributed = 0` olan sentetik veya ilişkilendirilmemiş eventler Campaign/Flow performansı olarak yazılmaz.
6. Campaign/Flow yapraklarında `spend = null` ve `spend_value = unsupported` olmalıdır.
7. Business date, source timezone, source/target currency ve FX provenance doğrulanmalıdır.
8. Aynı canonical anahtar altında duplicate bulunmamalıdır.

## Karar sınıfları

- Flow tarihi kapanmamışsa: `WAIT_NO_WRITE`.
- Provider Flow sonucu boşsa: verified-empty kaydedilebilir; Flow persistence PASS verilmez.
- Journey stage provider sonucunda yok veya `unknown` ise: değer uydurulmaz, R6-D5 açık kalır.
- Event envanteri mevcut fakat provider attribution yoksa: event sayıları Campaign/Flow satırına taşınmaz.
- Binding, Time, FX veya canonical identity hatasında: fail-closed, Dataset yazımı yok.
- Salt okunur kapı geçerse: kullanıcıdan ayrı açık onay alınır ve yalnız bir kontrollü acceptance çalıştırılır.

## Kontrollü yazım kabulü

- Otomatik retry yapılmaz.
- Provider'ın döndürdüğü Campaign ve Flow satırları canonical anahtarlarla idempotent yazılmalıdır.
- Provider'ın döndürdüğü Flow satırı Dataset V2'de fiziksel olarak görülmelidir.
- Duplicate canonical grup sayısı `0` olmalıdır.
- Connection, account selection ve encrypted tokenlar korunmalıdır.
- AdsTable sentetik Campaign, Flow veya journey satırı üretmez.
- Schedule, backfill ve production activation açılmaz.

## Email maliyeti ve FX sınırı

- Canonical kullanıcı girdisi `Estimated 30-Day Klaviyo Email Spend` değeridir.
- Günlük hesap seviyesi tahmini maliyet: `Estimated 30-Day Klaviyo Email Spend ÷ 30`.
- Bu maliyet recipient, delivered, Campaign, Flow veya Variation satırlarına paylaştırılmaz.
- Campaign/Flow yapraklarında maliyet `null/unsupported` kalır.
- SMS ve WhatsApp maliyetleri bu sözleşmenin dışındadır.
- Ayrı account-day maliyet gerçeğinin yazılması bu kontrollü Flow kabulünün kapsamında değildir.
- Account-day maliyeti ileride yazıldığında kendi business date FX provenance'ını kullanır.

## Done sınırı

Flow persistence ve provider-attributed Added to Cart, Started Checkout ve Placed Order count/value kapsamı canlıda kanıtlanmadan R6-D5 `Done` değildir.
