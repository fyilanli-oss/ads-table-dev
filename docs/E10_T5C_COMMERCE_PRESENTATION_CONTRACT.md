# E10-T5-C — Shopify'a ne verilecek, nasıl gösterilecek?

**Durum:** `In progress — C1 Funnel ve C2 Ad Analysis Done; C3 Dashboard sıradaki ürün kararı; implementation yasak`

E10-T5-C'nin amacı AdsTable'ın Shopify merchant'a hangi çıktıları vereceğini ve bunların Shopify-native Funnel/Table yüzeyinde nasıl gösterileceğini ürün seviyesinde dondurmaktır. Önceki Shopify Total Purchase/Sales/Refund presentation varsayımı geçersizdir; Shopify'dan alınacak iki overlap metriği bu kararı belirlemez.

T5-C tamamlanmadan aşağıdaki konular tek bir okunabilir output/display matrisinde kararlaştırılacaktır:

- Merchant'a açılacak provider ve hierarchy seviyeleri
- Funnel ile Table'ın varsayılan görünümü ve switch davranışı
- Gösterilecek metrikler, sıraları, adları ve provenance açıklamaları
- Time Range, Comparison ve Filters davranışları
- Overlap diagnostic sonucunun ana rapordan ayrımı ve disclosure dili
- Loading, empty, partial, stale, unsupported, error ve re-auth durumları
- Desktop/mobile bilgi yoğunluğu, drill-down ve yatay akış

## Modül sırası

1. **E10-T5-C1 Funnel — `Done`:** Funnel/Table davranışı, Shopify component eşlemesi ve üç iş kararı `docs/E10_T5C1_FUNNEL_SHOPIFY_COMPONENT_FREEZE.md` ile donduruldu.
2. **E10-T5-C2 Ad Analysis — `Done`:** C2-A UI/ranking ve C2-B Creative capability/data model kararları tamamlandı.
3. **E10-T5-C3 Dashboard — `Product decision required`**
4. **E10-T5-C4 Platforms — `Blocked by C1–C3`:** Provider OAuth yüzeyi.
5. **E10-T5-C5 Attribution — `Blocked by C1–C4`:** Overlap diagnostic yüzeyi.
6. **E10-T5-C6 Settings — `Blocked by C1–C5`:** OAuth ve diğer ayarlar.
7. **E10-T5-C7 Integrated navigation/acceptance — `Blocked by C1–C6`**

Bu sıra parent T5-C'yi tamamlamaz. Funnel kararı diğer modüllere sessizce genellenmez; her modül kendi okunabilir freeze ve onayını alır.

Ad Analysis C2-A kararı `docs/E10_T5C2A_AD_ANALYSIS_SHOPIFY_COMPONENT_FREEZE.md` ve `contracts/shopify/e10-t5c2a-ad-analysis-ui.json` içindedir. Creative ilk dilimde metadata/preview'dır; Dataset V2 fact'i veya Funnel leaf'i değildir. C2-B sonucu provider-specific metadata sidecar'dır; performance kapalıdır ve bu sınır yeni ürün/veri kararı olmadan genişletilemez.

## Provider OAuth montajı — dondurulmuş karar

- Meta, Google, TikTok, Pinterest ve Klaviyo Connect/Disconnect/Reconnect ile account selection kontrolleri Shopify embedded `Data Sources / Platforms` sayfasındadır ve resmi Shopify UI componentleriyle gösterilir.
- Başlatma isteği embedded session token ile backend'e gider; backend transaction'ı doğrulanmış shop/workspace/user/provider ve `surface=shopify_embedded` bağlamına kilitler.
- Provider consent üçüncü taraf sayfası olduğu için iframe içinde gösterilmez. Güncel resmi App Bridge dış navigasyon yöntemiyle top-level açılır; exact API sürümü implementation öncesi doğrulanır.
- Callback provider'a kayıtlı AdsTable HTTPS endpoint'indedir. State tek kullanımlık tüketilir; token exchange ve encrypted persistence backend'de kalır.
- Callback sonucu bağımsız `/dashboard` yerine Shopify Admin'deki canonical embedded uygulama URL'sine döner; embedded session/status yeniden alınır.
- Standalone kanal korunursa transaction surface ve dönüş URL'si ayrıdır; iki kanal birbirine yönlenemez.

Mevcut provider OAuth çekirdeği korunur, fakat mevcut `/dashboard?...` callback dönüşleri embedded akışta doğrudan kullanılamaz. Runtime adaptasyonu T5-C output/display onayı sonrasında ayrı contract ile yapılır; bu karar production OAuth ayarı değildir.

UI, resmi Shopify embedded shell, App Bridge ve genel Shopify UI componentleriyle kurulacaktır. AdsTable'a özel görselleştirme yalnız Funnel/Table data presentation alanında kullanılabilir; özel CSS component framework, statik Shopify shell taklidi ve iframe/yabancı-site hissi kabul edilmez.

## Sıra kapısı

Kullanıcı Execution Plan'daki E10-T5-A, E10-T5-B ve hazırlanacak E10-T5-C output/display matrisini okuyup açıkça onaylamadan:

- E10-T5-C `Done` yapılamaz.
- E10-T6 veya sonraki E10 paketleri açılamaz.
- E11 Funnel API veya E12 embedded UI paketi/PR'ı açılamaz.
- Shopify scope, production query, storage, sync, webhook, migration veya UI implementasyonu yapılamaz.
