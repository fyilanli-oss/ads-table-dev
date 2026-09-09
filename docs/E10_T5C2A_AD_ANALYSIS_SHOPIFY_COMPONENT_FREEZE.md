# E10-T5-C2-A — Ad Analysis Shopify component ve davranış freeze'i

**Durum:** `Done — dört iş kararı onaylandı; UI/provider/Dataset implementasyonu yapılmadı`

Ad Analysis, seçili dönemde en iyi veya en zayıf performans gösteren gerçek analytical leaf'leri bulmaya yarayan ranking ve diagnostic yüzeyidir. Funnel'ın genişletilmiş bir kopyası değildir. Standart provider'larda satır grain'i Ad; Google Performance Max'te mevcut canonical sözleşmeye uygun olarak Asset Group'tur.

## Onaylanan dört iş kararı

1. Varsayılan ranking **Sales, yüksekten düşüğe**dir.
2. Compare ranking aktif `Purchase / Sales / Revenue` metriğinin yüzde değişimine göre **en çok yükselenden en çok düşene**dir.
3. Creative ilk dilimde Ad/Asset Group performansından ayrı yalnız **preview/metadata**dır; Ad metriği Creative'e kopyalanmaz veya dağıtılmaz.
4. Creative veri erişimi ve grain'i **E10-T5-C2-B provider capability + data model** paketinde araştırılır; sonuç görülmeden Dataset V2 değiştirilmez.

## Ana liste ve sıralama

`Purchase / Sales / Revenue` bir ranking metric switch'idir. Normal görünümde seçili metric varsayılan olarak yüksekten düşüğe sıralanır; kullanıcı aynı kontrol/header üzerinden düşükten yükseğe dönebilir. Total satırı görünürse başta sabittir ve ranking'e katılmaz.

Compare açıkken varsayılan sort anahtarı yalnız aktif metric'in backend tarafından hesaplanan `percent_change` değeridir. Kullanıcı yönü tersine çevirebilir. Önceki değer sıfırsa sonsuz yüzde uydurulmaz; satır `new/not comparable` grubuna girer. Missing/unsupported değer `—` kalır ve sıralamanın sonuna gider. Görünen metin, renk veya ok frontend sort authority olamaz.

Deterministik tie-break sırası `selected metric → Sales → Purchase → Spend → stable entity key`dir. Aynı request aynı satır sırasını üretir.

## Kolon sözleşmesi

İlk ana liste `Platform / Ad or analytical leaf / Creative / Purchase / Sales / Spend / Revenue / Intent / Performance` kolonlarını taşır. Mockup'taki `Sales Value` etiketi kullanıcı dilinde `Sales` olur. `Margin`, gerçek cost/COGS/refund/fee contract'ı olmadığı için kaldırılır.

Compare görünümünde metrikler `Comparison / Current / Change` alt kolonlarını gösterebilir; ranking yalnız aktif switch metriğine göre yapılır. Total ve satır değerleri frontend'de toplanmaz veya yeniden hesaplanmaz.

## Intent ve Performance

Intent ve Performance hücreleri dekoratif ikon değil, erişilebilir satır eylemidir. Her biri resmi modal içinde onaylı metric ailesini gösterir. Aynı anda yalnız bir analiz modalı ve tek satır bağlamı açık olabilir. Compare aktifse modal aynı comparison/current/change semantiğini kullanır.

Intent ilk ailesi `Add to Cart Rate / Checkout Rate / Abandoned Rate / Purchase Rate`; Performance ilk ailesi `CTR / CPC / ROAS / CPS`dir. Değerler backend Formula/Compare sonucudur.

## Creative sınırı

Creative, ortak `Ad → Creative` fact hierarchy'si olarak kabul edilmez. Bir Ad tek creative, birden fazla asset veya dinamik/responsive kombinasyon taşıyabilir; provider'lar aynı grain'i garanti etmez. PMax leaf'i Asset Group kalır.

İlk dilimde Creative kolonu yalnız capability-aware preview/metadata durumu açar: thumbnail/media reference, provider creative/asset identity, type, label ve status gibi PII'siz tanımlayıcılar. Creative erişilemiyorsa `Unavailable`; metadata var fakat metrik Ad seviyesindeyse `Performance reported at Ad level` açıklaması gösterilir.

Ad Purchase/Sales/Spend/Revenue değeri alt creative'lere aynen kopyalanamaz, eşit/oransal dağıtılamaz ve Funnel toplamına creative satırı eklenemez. Creative metric ancak provider gerçek asset/creative-level performance sağladığını C2-B'de kanıtlar ve ayrı versionlı fact kararı onaylanırsa açılabilir.

Creative metadata bu pakette Dataset V2'ye yazılmaz. C2-B; provider bazında identity, Ad association, version/effective time, preview güvenliği, metric grain, gerekli scope, retention ve PMax/dynamic/responsive davranışını resmi kaynaklarla inceler.

## Shopify-native component eşlemesi

| Mockup alanı | Resmi component yönü | Karar |
|---|---|---|
| Sayfa/layout | `s-page`, `s-section`, `s-grid`, `s-stack` | Shopify global shell çizilmez. |
| Date/Compare/Filters | C1'deki aynı resmi toolbar componentleri | Davranış ve tek-overlay kuralı korunur. |
| Purchase/Sales/Revenue | `s-button-group` | Ranking metric seçer; veri filtresi değildir. |
| Ana liste | Önce `s-table` | Sort, hierarchy ve accessibility uygunluğu implementation öncesi doğrulanır. |
| Sort yönü | Resmi table header veya `s-button`/`s-menu` | Aktif metric ve yön görünür/erişilebilir olur. |
| Intent/Performance | `s-button` + `s-modal` | Aynı anda yalnız biri açılır. |
| Creative durumu | Uygun resmi media/image yüzeyi + `s-badge`, `s-tooltip` | Exact component C2-B/T6-A resmi doğrulamasına bağlıdır. |
| Loading/partial/error | `s-spinner`, `s-banner`, `s-badge` | Unsupported sahte sıfır değildir. |

Component/property adları implementation kilidi değildir; güncel resmi App Home ve App Bridge yüzeyi E10-T6-A'da yeniden doğrulanır. Özel global shell, toolbar, modal, button veya AdsTable CSS component framework yasaktır.

## Paket ve sıra kapısı

Bu paket yalnız E10-T5-C2-A ürün/UI freeze'idir. E10-T5-C2 parent `In progress`, sıradaki repository işi **E10-T5-C2-B Creative provider capability ve data model matrisi**dir. C2-B tamamlanmadan C2 `Done` veya C3 Dashboard `Ready` yapılmaz. Shopify/provider credential, scope, API query, Dataset migration, ingest veya production işlemi yapılmadı.
