# E10-T5-B/D — Shopify attribution overlap intake contract

## Düzeltilmiş ürün kararı

Shopify'dan alınacak ilk veri seti bir commerce totalı veya Funnel veri kaynağı değildir. Tek amaç, Shopify'ın platform bazında raporladığı attribution ile reklam provider'larının kendi raporlarını daha sonra karşılaştırıp olası çakışmayı (overlap) inceleyebilmektir.

İzin verilen tek boyut `platform`; izin verilen tek iki değer `platform_purchase_count` ve `platform_sales_value`dır. Total Purchase, Total Sales, Refund, currency, timezone, order/customer kaydı, Add to Cart ve Checkout bu intake'in parçası değildir. Müşteri PII'si alınmaz.

## Scope neden henüz seçilmedi?

Bu kontrat Shopify'a gösterilecek AdsTable çıktılarından bağımsız biçimde yeni bir order ingestion tasarlamaz. İstenen iki platform-attribution değerinin güncel resmi Shopify API'de hangi resource/field ile bulunabildiği ve hangi scope/review koşulunu gerektirdiği doğrulanmadan `read_orders` dahil hiçbir scope aday olarak dondurulmaz. Bu nedenle executable contract'ta `first_slice_scopes=[]`, resource/field değerleri boş ve scope durumu `unresolved_no_scope_request`tır.

Shopify'ın bu iki aggregate değeri desteklenen ve PII'siz bir API yüzeyinden verememesi halinde kapsam order/customer ham verisine doğru sessizce genişletilmez. Ürün kararı yeniden açılır; kullanıcı onayı olmadan alternatif attribution türetme yöntemi kurulmaz.

## Kesin intake matrisi

| Boyut / değer | Amaç | Provenance | İlk dilim durumu |
|---|---|---|---|
| Platform | Shopify raporu ile provider satırını aynı kanalda karşılaştırmak | Shopify-reported attribution | İzinli tek boyut |
| Platform Purchase Count | Olası attribution overlap analiz girdisi | Shopify-reported attribution | İzinli |
| Platform Sales Value | Olası attribution overlap analiz girdisi | Shopify-reported attribution | İzinli |

Bu değerler Shopify-observed toplam mağaza satış gerçeği olarak adlandırılmaz. Provider-reported attribution ile toplanmaz, birbirinden çıkarılmaz, kazanan kaynak seçilmez ve frontend'de uzlaştırılmış gerçek gibi gösterilmez. Eşleştirme ve overlap hesabı, ileride ayrıca onaylanacak backend sözleşmesidir.

## Açıkça kapsam dışı

- Total Purchase ve Total Sales
- Refund ve refund toplamları
- Shop currency ve timezone intake çıktıları
- Order/customer satırları ve her türlü müşteri PII'si
- Add to Cart, Checkout ve Organic çıkarımı
- `read_orders`, `read_all_orders` veya başka bir production scope talebi
- Dataset V2 veya yeni commerce tablosuna yazma
- Webhook, initial sync, migration ve production query

## Yeniden doğrulama kapısı

Shopify'a verilecek AdsTable ürün çıktıları netleştirildikten sonra yalnız bu iki platform-attribution değerinin resmi API erişilebilirliği, exact resource/field, para birimi semantiği, tarih aralığı, scope ve protected-data sınıfı güncel Shopify sürümünde doğrulanır. Sonuç bu kontratla uyumsuzsa otomatik kapsam genişletilmez; iş kararı istenir.
