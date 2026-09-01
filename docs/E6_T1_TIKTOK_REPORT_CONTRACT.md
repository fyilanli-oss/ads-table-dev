# E6-T1 — TikTok production report contract

## İş kararı

TikTok üretim verisi yalnız reklam (`Ad`) seviyesindeki günlük BASIC Auction raporundan üretilecektir. Campaign ve AdGroup satırları ayrıca toplanmayacak; yalnız Ad satırının lineage bilgisi olacaktır. Böylece aynı performans üç seviyede üst üste eklenmez.

E6-T1 yalnız doğrulanmış teslimat metriklerini dondurur: `spend`, `impressions` ve `clicks`. Mevcut legacy koddaki genel `conversion` alanı purchase değildir. ATC, Checkout, Purchase ve bunların value alanları canlı advertiser cevabında ayrı ayrı doğrulanmadan canonical fact olarak desteklenmiş sayılmaz; eksik değerler sıfıra çevrilmez.

## Resmî kaynak sınırı

Contract, TikTok'un resmî `tiktok-business-api-sdk` repository'sindeki `f809c396520df2d7b201a9ccc5378d822b728ed3` commit'ine ve SDK'nın `ReportingApi.md` belgesine sabitlenmiştir. Bu kaynak synchronous endpoint'i `/open_api/v1.3/report/integrated/get/`, HTTP metodunu `GET`, `BASIC` report type'ını ve `AUCTION_AD` data level seçeneğini doğrular. SDK belgesi ayrıca TikTok portal doküman kimliği `1740302848100353` bağlantısını taşır.

Resmî SDK şeması event metric isimlerini kapalı bir enum olarak yayımlamadığı için E6-T1 bunlar hakkında tahmin yürütmez. Event count/value alanlarının seçimi E6-T2/E6-T3 sırasında gerçek advertiser reporting cevabı ve açık mapping kanıtıyla yapılacaktır.

## Kapsam ve güvenlik

- Production additive grain yalnız `AUCTION_AD` / `ad_id` olur.
- Campaign → AdGroup → Ad ilişkisi korunur; üst seviyeler ayrıca fact olmaz.
- `conversion → purchase` fallback'i kesin olarak yasaktır.
- Missing/unknown değer measured zero değildir.
- Synthetic fallback production Dataset V2'ye giremez.
- Bu task provider çağrısı, production write veya canlı activation yapmaz.
