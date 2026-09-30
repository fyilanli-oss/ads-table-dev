# R7-B6 — Settings Shopify görsel standardı

## İş çıktısı

Settings, mevcut OAuth ve veri davranışını değiştirmeden Shopify Admin ile aynı görsel dile taşınır.

## Onaylanan yüzey

- Reporting Currency ve Platforms iki ayrı Shopify-native karttır.
- Meta, Google Ads ve Klaviyo tek Platforms kartı içinde yer alır ve satırlar resmi `s-divider` ile ayrılır.
- Provider başlığı altındaki tekrar eden tanıtım cümleleri gösterilmez.
- Bağlı değilken yalnız durum ve nötr `Connect` eylemi görünür.
- Yarım kalan OAuth sonrasında nötr `Resume setup` görünür.
- Bağlı durumda `Connected` success badge ve `Disconnect` critical action görünür.
- Meta ve Google Ads, bağlı hesap sayısına ek olarak Reporting Account özetini ve nötr `Reporting account` eylemini gösterir.
- Klaviyo, 30 günlük tahmini e-posta giderini ve nötr `Update spend` / `Change value` eylemlerini gösterir.
- `Change value` yalnız kullanıcı etiketidir; mevcut correction backend davranışı ve tarihsel kaydın effective date'ini koruma kuralı değişmez.

## Değişmeyen sınırlar

Bu paket Connect/Disconnect modal metinlerini, OAuth yönlendirmesini, hesap seçimini, token saklama/yenilemeyi, Supabase şemasını, Dataset V2 yazımını veya snapshot işlerini değiştirmez.

## Navigation ikon kararı

Shopify App Bridge `s-app-nav` bağlantıları resmi sözleşmede yalnız kısa metin çocuk kabul eder; link icon property'si yoktur. Bu nedenle Funnel, Analysis ve Settings için custom SVG, emoji veya CSS ikon taklidi eklenmez. Shopify resmi destek sunduğunda ayrı doğrulamayla ele alınır.

## Kabul kapısı

Repository testleri yalnız markup ve davranış regresyonunu doğrular. Paket, gerçek Shopify Admin içinde desktop ve mobil görünümde merchant kabulü tamamlanmadan `Done` sayılmaz.
