# E9-T3 — Worker lease, quota ve adaptive retry

Her checkpoint tek worker tarafından atomik RPC ile alınır. Lease 30–900 saniye arasında olmalıdır. Aktif lease ikinci worker'a verilmez; yarıda kalan `running` iş yalnız lease süresi dolduktan sonra aynı checkpoint/cursor üzerinden yeniden alınabilir.

Meta, Google, TikTok ve Klaviyo ayrı in-flight bütçesi taşır; bir provider'ın beklemesi diğerini bloke etmez. İç güvenlik bütçesi provider başına tek eşzamanlı istek ve en fazla üç denemedir. TikTok için production'da gözlenen 1 QPS sınırına uygun 1100 ms, diğer provider'lar için koruyucu 1000 ms minimum aralık kullanılır; bunlar provider kotası iddiası değil AdsTable'ın kendi üst sınırlarıdır.

Yalnız HTTP 429/500/502/503/504 geçici kabul edilir. Geçerli `Retry-After` varsa önceliklidir; yoksa bounded exponential backoff uygulanır. Tek bekleme 5 dakikayı, lokal exponential bekleme 30 saniyeyi geçmez. Deneme bütçesi bittiğinde iş tekrar edilmez ve E9-T2 güvenli hata koduyla `failed` olur. Pinterest/Organic bütçe listesinde değildir.

Bu görev lease/retry sözleşmesini hazırlar; production migration, provider çağrısı veya backfill çalıştırmaz.
