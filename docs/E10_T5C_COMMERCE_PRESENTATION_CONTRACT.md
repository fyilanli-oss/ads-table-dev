# E10-T5-C — Commerce provenance ve presentation contract

Shopify-observed Purchase/Sales/Refund, provider-reported attribution ve AdsTable-calculated sonuçlar ayrı nesnelerde ve açık provenance ile sunulur; Shopify Sales ile provider Sales toplanmaz veya birbirinin yerine geçirilmez.

Toplam Revenue `Shopify-observed Sales - provider-reported aggregate Spend`; entity Revenue `provider-reported Sales - provider-reported Spend` olur. Eski teknik `profit` aynı değerin ikinci UI adı değildir; ilk dilimde `profit=null` ve `margin=null` kalır. Unsupported/unknown değerler `null` kalır ve derived Revenue'yu bloke eder. Organic frontend segmenti üretilmez.

Bu API presentation contract'ı mevcut canonical storage veya Formula Engine alanını migrate etmez ve production işlemi yapmaz.
