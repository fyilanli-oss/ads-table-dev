# E10-T5-C — Shopify'a ne verilecek, nasıl gösterilecek?

**Durum:** `Product decision required — implementation yasak`

E10-T5-C'nin amacı AdsTable'ın Shopify merchant'a hangi çıktıları vereceğini ve bunların Shopify-native Funnel/Table yüzeyinde nasıl gösterileceğini ürün seviyesinde dondurmaktır. Önceki Shopify Total Purchase/Sales/Refund presentation varsayımı geçersizdir; Shopify'dan alınacak iki overlap metriği bu kararı belirlemez.

T5-C tamamlanmadan aşağıdaki konular tek bir okunabilir output/display matrisinde kararlaştırılacaktır:

- Merchant'a açılacak provider ve hierarchy seviyeleri
- Funnel ile Table'ın varsayılan görünümü ve switch davranışı
- Gösterilecek metrikler, sıraları, adları ve provenance açıklamaları
- Time Range, Comparison ve Filters davranışları
- Overlap diagnostic sonucunun ana rapordan ayrımı ve disclosure dili
- Loading, empty, partial, stale, unsupported, error ve re-auth durumları
- Desktop/mobile bilgi yoğunluğu, drill-down ve yatay akış

UI, resmi Shopify embedded shell, App Bridge ve genel Shopify UI componentleriyle kurulacaktır. AdsTable'a özel görselleştirme yalnız Funnel/Table data presentation alanında kullanılabilir; özel CSS component framework, statik Shopify shell taklidi ve iframe/yabancı-site hissi kabul edilmez.

## Sıra kapısı

Kullanıcı Execution Plan'daki E10-T5-A, E10-T5-B ve hazırlanacak E10-T5-C output/display matrisini okuyup açıkça onaylamadan:

- E10-T5-C `Done` yapılamaz.
- E10-T6 veya sonraki E10 paketleri açılamaz.
- E11 Funnel API veya E12 embedded UI paketi/PR'ı açılamaz.
- Shopify scope, production query, storage, sync, webhook, migration veya UI implementasyonu yapılamaz.
