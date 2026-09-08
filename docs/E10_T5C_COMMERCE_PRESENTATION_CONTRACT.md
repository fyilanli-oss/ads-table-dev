# E10-T5-C/D — Commerce presentation varsayımının geri alınması

Önceki E10-T5-C kontratının Shopify Total Purchase/Sales/Refund verisini Funnel commerce kaynağı kabul etmesi ürün yönünü aştığı için geri alınmıştır. Shopify'dan alınacak veriler, şimdilik yalnız attribution overlap incelemesi için platform bazlı Purchase Count ve Sales Value'dur.

Bu iki değer `shopify_reported_attribution` provenance'ında kalır. Provider-reported Purchase/Sales ile birleştirilmez, Shopify mağaza toplamı olarak gösterilmez ve Revenue üretmekte kullanılmaz. `Revenue = Sales - Spend` AdsTable ürün sözlüğü korunur; fakat hangi Sales'in Shopify embedded çıktısına verileceği, “Shopify'a ne vereceğiz?” ürün kontratında ayrıca kararlaştırılmadan bu intake tarafından belirlenmez.

Bu düzeltmeyle eski `commerce-presentation.js` kaldırılmış ve yerine yalnız üç alanı (`platform`, `platform_purchase_count`, `platform_sales_value`) kabul eden fail-closed `attribution-overlap-intake.js` konmuştur. Total, refund, currency, timezone veya başka alan eklenirse kontrat reddeder.

Bu repository kontratı API erişilebilirliği, scope, storage, sync, UI veya production işlemi iddia etmez.
