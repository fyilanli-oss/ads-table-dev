# E10-T5-C1 — Funnel Shopify component ve davranış freeze'i

**Durum:** `Done — ürün kararları onaylandı; UI implementasyonu yapılmadı`

Bu belge altı Funnel mockup'ını davranış referansı olarak Shopify'ın güncel embedded uygulama yaklaşımına eşler. Mockup'taki Shopify üst/sol shell yeniden çizilmez. Uygulama App Bridge içindeki gerçek Shopify Admin shell'inde çalışır; AdsTable yalnız uygulama içeriğini render eder.

## Onaylanan üç iş kararı

1. Açılış görünümü **Funnel**'dır. `Funnel / Table` değişimi aynı sorgunun iki sunumudur ve kullanıcının bağlamını korur.
2. Table compare açıldığında yalnız seçili **focus metric** `Comparison / Current / Absolute change / % change` alt kolonlarına genişler; diğer metrikler current değerle kalır. Böylece bütün metrikler dört kat kolon üretmez.
3. Desktop toolbar'da aynı anda yalnız bir popover açık kalır. Mobilde yoğun tarih, comparison ve filter içeriği resmi modal/sheet davranışına geçer; desktop popover mobil iframe'e sıkıştırılmaz.

## Shopify-native component eşlemesi

| Mockup alanı | Resmi component yönü | Dondurulan davranış |
|---|---|---|
| Uygulama sayfası ve bölümler | `s-page`, `s-section`, `s-grid`, `s-stack` | Shopify shell kopyalanmaz; içerik responsive Shopify layout'u içinde akar. |
| Time Range | `s-button` + `s-popover`; `s-date-picker`, `s-date-field`, `s-choice-list` | Preset ve custom range tek overlay'dedir; Apply/Cancel açık işlemdir. |
| Comparison | `s-button` + `s-popover`; `s-choice-list`, gerektiğinde `s-date-field` | No comparison, Previous period, Previous year, weekday-matched previous year ve Custom allowlist'i kullanılır. |
| Filters | `s-button` + `s-popover`; `s-search-field`, `s-checkbox`, `s-clickable-chip` | AdsTable taxonomy'si aranır/seçilir; seçim sayısı ve removable chip görünürdür. Shopify Analytics filtreleri kopyalanmaz. |
| Funnel / Table switch | `s-button-group` | Funnel varsayılandır; switch yeni sorgu veya kayıp state yaratmaz. |
| Export / Data Sources | `s-button`, `s-menu`, `s-badge` | İkincil eylemlerdir; bağlantı durumu gerçek capability sonucudur. |
| Auto refresh / freshness | `s-switch`, `s-badge`, `s-tooltip` | Freshness backend metadata'sından gelir; dekoratif yeşil nokta başarı kanıtı değildir. |
| Loading / partial / error | `s-spinner`, `s-banner`, `s-badge` | Eksik veya unsupported değer sahte sıfıra çevrilmez. |
| Düz tablo başlangıç tercihi | `s-table` | Resmi tablo önce değerlendirilir; hierarchy/sticky/multi-header ve accessibility kabulünü karşılamazsa yalnız data-presentation alanında kontrollü özel renderer kullanılabilir. |

Component adları implementation kilidi değildir. Polaris App Home component yüzeyi sürümsüz gelişebildiği için exact component, property, responsive overlay ve App Bridge davranışı E10-T6-A'da güncel resmi kaynaklardan tekrar doğrulanır. Kaynaklar: [Polaris App Home component rehberi](https://shopify.dev/docs/api/app-home/using-polaris-components), [App Home component reference](https://shopify.dev/docs/api/app-home), [App Bridge](https://shopify.dev/docs/api/app-bridge-library).

## Veri akışı ve metrikler

### Funnel görünümü

Akış aşağıya doğru stage, sağa doğru dönem/compare'dır:

1. **Traffic:** Impression, Click, Spend, CTR, CPC
2. **Cart:** Add to Cart, Add to Cart Value
3. **Checkout:** Checkout, Checkout Value, Abandoned, Abandoned Value
4. **Outcome:** Purchase, Sales, Revenue, ROAS, CPS

Custom compare sonucunda her metrik satırı `Current / Comparison / Change` gösterir. Değişim backend çıktısıdır; denominator sıfırsa yüzde yerine `not comparable/new`, unsupported ise `—` gösterilir.

### Table görünümü

Akış aşağıya doğru gerçek provider hierarchy, sağa doğru metriklerdir. Compare kapalıyken tüm onaylı metrikler current kolonlarıdır. Compare açıkken kullanıcı bir focus metric seçer; yalnız o metrik karşılaştırma alt kolonlarına genişler. Focus değişimi tarih/filter sorgusunu bozmaz.

Desktop'ta identity kolonu görünür kalır ve metrik alanı yatay akar. Mobilde tüm geniş masaüstü tabloyu küçültmek yerine identity + seçili metrik grubu gösterilir; ayrıntı kontrollü drill-down ile açılır.

## Hierarchy ve state

Provider → Campaign/Flow → provider'ın gerçek group seviyesi → leaf akışı korunur. Olmayan Ad Group/Ad/parent üretilmez. Funnel ve Table şu state'i ortak taşır: time range, comparison, filters, currency, data sources, expanded entities ve table compare focus metric.

İlk renderda yalnız bağlı, yetkili ve capability sonucu uygun provider/entity gösterilir. Mockup sayıları demo veridir; production fallback değildir. Frontend aggregation, formül, hierarchy veya provider support uyduramaz.

## Özel görselleştirme sınırı ve kabul

`s-table` gereksinimleri karşılıyorsa resmi component kullanılır. Karşılamıyorsa özel kod yalnız expandable Funnel/tree-table **veri sunum gövdesinde** kullanılabilir; Shopify token, keyboard navigation, focus, screen-reader semantics, responsive davranış ve visual regression kabulü zorunludur. Özel global shell, toolbar, popover, modal, button, filter sistemi veya AdsTable CSS component framework kurulamaz.

Birden fazla toolbar popover'ının eşzamanlı açık olması, Shopify shell'in kopyalanması, desktop tablonun mobil iframe'e sıkıştırılması, switch sırasında state kaybı veya yabancı-site hissi acceptance failure'dır.

## Paket sınırı

Bu paket yalnız **E10-T5-C1 Funnel** ürün/component freeze'idir. UI kodu, Partner Dashboard, Shopify scope, credential, Development Store, API query, OAuth ayarı veya production işlemi içermez. E10-T5-C parent'ı; C2 Ad Analysis ve C3 Dashboard dahil kalan modüller onaylanıp merge edilmeden `Done` değildir.
