# E9-T1 — İlk veri hazırlama kapsamı

## Onaylanan ürün kararı

- İlk hazırlama yalnız `yesterday` ve `today` günlerini kapsar.
- `yesterday` önce işlenir ve `finalized`; `today` sonra işlenir ve `provisional` kabul edilir.
- Kapsanan provider'lar Meta, Google, TikTok ve Klaviyo'dur.
- Her provider için kullanıcının seçtiği, bağlantısı ve ownership'i aktif en fazla üç hesap kapsanır.
- Pinterest ve GA4 Organic dahil park edilmiş provider'lar job/API çağrısı üretmez.
- İlk hazırlamada yesterday'dan eski tarih otomatik alınmaz.
- 14. günde politika değişmez: tarihçe bağlantı gününden itibaren günlük olarak doğal biçimde büyür; eski tarihçe ancak gelecekte ayrı ürün kararıyla alınabilir.

## Kullanıcı deneyimi

Yesterday sonucu Today beklenmeden kullanılabilir hale gelir. Bir provider/account hatası diğerlerini bloke etmez; T2 checkpoint modeli her `platform + account + date` birimini bağımsız izleyecektir. Provider henüz kabul kapısını geçmediyse sentetik veri yerine readiness nedeni üretilecektir. E9 hiçbir provider'ı primary moda geçirmez.

## Bu görevin sınırı

T1 yalnız deterministik kapsam planını dondurur. Production backfill çalıştırmaz. Cursor/checkpoint persistence E9-T2, kota/retry E9-T3 ve canonical yazım E9-T4 kapsamındadır.
