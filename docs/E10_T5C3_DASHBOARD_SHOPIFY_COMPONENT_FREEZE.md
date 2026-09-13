# E10-T5-C3 — Dashboard Shopify component ve davranış freeze'i

**Durum:** `Done — üç iş kararı onaylandı; UI implementasyonu yapılmadı`

Dashboard, seçili tamamlanmış dönemin reklam performansını tek bakışta sunan yönetici özetidir. Campaign/Ad hierarchy tablosu, Creative ve attribution overlap bu yüzeyde gösterilmez; ayrıntı ilgili modüle state koruyan drill-down ile açılır.

## Onaylanan üç iş kararı

1. Time Range yalnız `Last 7/14/30/90 Completed Days` presetleridir. Custom range yoktur; bugün dahil edilmez. Compare yalnız On/Off'tur ve hemen önceki aynı uzunluktaki completed dönem backend tarafından otomatik türetilir.
2. Funnel Overview'daki Add to Cart, Checkout, Abandoned ve Purchase stage'lerinin her birinde count ve value bulunur. Compare açıkken count ve value ayrı ayrı `Current / Comparison / Change` taşır.
3. Dashboard'daki Funnel/Table switch kaldırılır. Yerine tarih, comparison, filters, currency ve data-source context'ini koruyan `View Funnel` drill-down kullanılır.

## Dönem ve compare

Shop timezone authority'dir; browser timezone dönem üretmez. Backend current/comparison başlangıç-bitişini, gün sayısını ve completed-through bilgisini döndürür. Time Range değiştiğinde comparison otomatik yeniden hesaplanır; kullanıcı comparison takvimi veya serbest tarih giremez.

## Sales, Spend & Revenue grafiği

Grafik `Sales / Spend / Revenue` serilerini gösterir. Current çizgi solid, comparison çizgi dashed olur ve eşit uzunluktaki dönemler ordinal gün üzerinden hizalanır; tooltip iki gerçek tarihi açıklar. `Revenue = Sales - Spend` backend sonucudur. Mockup sayıları veya frontend hesabı contract değildir.

Grafik AdsTable özel data visualization alanıdır; Shopify token, accessible legend, renk dışı solid/dashed ayrımı, keyboard/screen-reader özeti ve responsive kabul zorunludur.

## KPI kartları

İlk KPI ailesi `Impressions / Clicks / CTR / CPC / ROAS / CPS`dir. Compare açıkken her kart current, comparison ve change gösterir. İyi/kötü renk semantiği metric direction metadata'sından gelir; CPC/CPS düşüşü olumlu olabilir. Unsupported/unknown `—` kalır.

## Funnel Overview

| Stage | Count | Value etiketi |
|---|---|---|
| Add to Cart | Add to Cart | Add to Cart Value |
| Checkout | Checkout | Checkout Value |
| Abandoned | Abandoned | Abandoned Value |
| Purchase | Purchase | Sales |

Compare açıkken her count ve value bağımsız current/comparison/change sonucu gösterir; tek yüzde iki farklı metriği temsil edemez. Purchase Value kullanıcıya `Sales` adıyla gösterilir. Kartlar frontend'de toplama/formül yapmaz.

## Shopify-native component yönü

Sayfa/layout `s-page`, `s-section`, `s-grid`, `s-stack`, `s-box`; dönem/filter eylemleri `s-button`, `s-popover`, `s-choice-list`, `s-switch`; metin/durum `s-text`, `s-badge`, `s-tooltip`, `s-banner`, `s-spinner`; drill-down `s-button` veya `s-clickable` yönündedir. Exact component/property uygunluğu E10-T6-A'da güncel resmi kaynakla doğrulanır.

Mockup'taki eşzamanlı üç popover yalnız anlatım kompozisyonudur; production'da C1 tek-overlay kuralı geçerlidir. Shopify global shell çizilmez ve özel Dashboard CSS component framework kurulmaz.

## Ortak context ve kapsam dışı

Grafik, KPI'lar ve Funnel Overview aynı backend time/comparison/filter/currency/data-source context'ini kullanır. `View Funnel` bu context'i taşır. Attribution overlap ayrı Attribution diagnostic yüzeyinde, Creative Ad Analysis'te kalır. Dashboard'da hierarchy table, Funnel/Table renderer switch, fake zero veya frontend formula yoktur.

## Sıra

E10-T5-C3 `Done`; sıradaki ürün paketi **E10-T5-C4 Platforms**dur. Bu paket Partner Dashboard, Development Store, OAuth/runtime, Shopify/provider scope, API query, migration veya production işlemi yapmadı.
