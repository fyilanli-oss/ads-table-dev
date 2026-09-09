# E10-T5-C2-B — Creative provider capability ve data model matrisi

**Durum:** `Done — fail-closed capability kararı; provider/runtime implementasyonu yok`

## Sonuç

Creative, AdsTable'ın ortak performance fact hierarchy'sine yeni leaf olarak eklenmeyecektir. İlk dilimde provider'a özgü Creative/Asset metadata ilişkisi ayrı bir **sidecar catalog** olarak modellenir; Dataset V2, Funnel hierarchy ve mevcut Ad/Asset Group fact grain'i değişmez. Creative-level performans ilk dilimde kapalıdır.

Bu karar Creative'i kullanıcıdan saklamaz: Ad Analysis satırı capability varsa preview/metadata açar. Ancak provider'ın gerçek raporlama grain'i kanıtlanmadan Ad metriğini asset'lere kopyalamaz, bölmez veya attribution gibi sunmaz.

## Provider matrisi

| Provider branch | Creative ilişkisi | Preview/metadata kararı | Performance kararı |
|---|---|---|---|
| Meta | Ad → AdCreative / asset specification | Resmi Ad Creative yüzeyi implementation öncesi tekrar doğrulanacak adaydır. | İlk dilimde kapalı; breakdown/asset metric doğrulanmadan gösterilmez. |
| Google Standard | Ad Group Ad → assets | Resmi `ad_group_ad_asset_view` doğrulanmış aday yüzeydir. | View metric alanları mevcut olsa da conversion semantiği ve aggregation kabulü tamamlanmadan kapalıdır. |
| Google Performance Max | Asset Group → assets | Resmi `asset_group_asset` doğrulanmış aday yüzeydir. | Asset performansı adaydır; Asset Group totalıyla birlikte toplanamaz ve ilk dilimde kapalıdır. |
| TikTok | Ad → video/image/identity asset | Resmi Business API şeması ve erişim implementation öncesi yeniden doğrulanacaktır. | İlk dilimde kapalı. |
| Pinterest | Ad → Pin creative | Resmi Ads API Pin/creative ilişkisi implementation öncesi yeniden doğrulanacaktır. | İlk dilimde kapalı. |
| Klaviyo | Campaign/Flow Message → template/content | Mevcut canonical message leaf'ine bağlı preview metadata adayıdır. | Ayrı Creative performance leaf'i yok; message leaf metriği korunur. |

Google resmi field reference sayfaları asset association ile metrics alanlarını belgeliyor; bu teknik olarak sorgulanabilir olması anlamına gelir, AdsTable ürününde güvenli toplama veya conversion attribution'ın onaylandığı anlamına gelmez. Diğer provider portalları bu çalışma ortamından doğrulanamadığı için isim/scope tahmini contract yapılmadı ve fail-closed revalidation statüsünde bırakıldı.

Resmi yeniden doğrulama kaynakları:

- [Meta Marketing API — Ad Creative](https://developers.facebook.com/docs/marketing-api/reference/ad-creative/)
- [Google Ads — ad_group_ad_asset_view](https://developers.google.com/google-ads/api/fields/v22/ad_group_ad_asset_view)
- [Google Ads — asset_group_asset](https://developers.google.com/google-ads/api/fields/v22/asset_group_asset)
- [TikTok Business API documentation](https://business-api.tiktok.com/portal/docs)
- [Pinterest API v5](https://developers.pinterest.com/docs/api/v5/)
- [Klaviyo Campaign Message API](https://developers.klaviyo.com/en/reference/get_campaign_message)

## Sidecar veri modeli

Association grain'i `workspace + platform + account + analytical entity key + provider creative/asset id + effective time`dır. Bu, aynı Creative'in farklı Ad'lerde kullanılmasını ve bir Ad'in zaman içinde Creative değiştirmesini ezmeden saklar.

İzinli metadata: creative type, güvenli display label, server-mediated preview reference, status, association effective time, observed time ve capability status. Purchase, Sales, Spend, Revenue, customer PII, provider token veya kalıcı üçüncü taraf signed media URL sidecar'a yazılamaz.

Preview URL doğrudan kalıcı source-of-truth değildir. Kısa ömürlü/provider korumalı medya, backend authority ve allowlist üzerinden gerektiğinde yenilenir; browser'a provider token verilmez. Silinmiş/erişilemeyen Creative `Unavailable` kalır ve eski URL ile taklit edilmez.

## Performance ve double-count kapısı

Creative performance ancak provider bazında şu kanıtların tamamı varsa ayrı pakette açılabilir: stable asset identity, tarih grain'i, metric attribution anlamı, parent-child reconciliation, duplicate engeli, currency/time semantics ve scope gerekçesi. Parent Ad/Asset Group ile Creative/Asset satırları aynı toplama setine giremez.

Google dahil hiçbir provider için bu paket performance ingest'i açmaz. `documented candidate`, yalnız resmi şemada aday yüzey bulunduğunu belirtir; production-ready veya parity-approved demek değildir.

## Scope ve uygulama kapısı

Yeni provider scope bu belgeyle talep edilmez. Creative metadata görünür ürün çıktısına bağlanıp güncel resmi izin doğrulaması tamamlandıktan sonra development onayı istenir. Provider smoke, Shopify'daki ilk temas kapısını veya production onayını geçersiz kılamaz.

## Sıra

E10-T5-C2-A ve C2-B birlikte Ad Analysis ürün sözleşmesini tamamlar; E10-T5-C2 `Done` olur. Sıradaki ürün paketi **E10-T5-C3 Dashboard**dur. Sidecar schema/migration ve provider Creative adapter implementasyonu T5-C ürün freeze'inin parçası değildir; ayrıca planlanır ve açık veri/scope kapılarına uyar.
