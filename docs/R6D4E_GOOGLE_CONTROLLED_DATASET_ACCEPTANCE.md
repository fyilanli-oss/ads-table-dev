# R6-D4-E — Google Ads kontrollü Dataset V2 kabulü

## Analist özeti

Bu paket, R6-D4-D aşamasında üç bağlı Google Ads hesabı için canlıda kanıtlanan salt-okunur E5 zincirinin son persistence sınırını test eder. Yeni sorgu, metrik, mapper veya veri modeli oluşturmaz.

Kullanıcı gizli operatör yüzeyindeki kritik işlemi açıkça başlatmadan provider veya Dataset erişimi yapılmaz. İşlem başladıktan sonra workspace, canonical bağlantı, seçili hesaplar, manager bağlamları ve reporting currency yalnız sunucu kaynaklarından çözülür.

## Kontrollü sıra

1. Doğrulanmış Shopify oturumundan workspace çözülür.
2. Connected canonical `google_ads` bağlantısı ve merchant reporting currency okunur.
3. Seçili 1–3 hesabın yakın tarih aralığında daha önce kabul satırı bulunmadığı doğrulanır.
4. Mevcut R6-D4-B token yaşam döngüsü kullanılır.
5. R6-D4-D workspace runner her hesabı provider'dan yeniden doğrular; Standard ve Performance Max dallarını tamamlanmış E5 sözleşmesiyle çalıştırır.
6. Time/FX uygulanmış, sentetik olmayan ve seçili hesaplara ait canonical satırlar yeniden doğrulanır.
7. Satırlar `WorkspaceCanonicalWriteBoundary` üzerinden `performance_dataset_rows_v2` tablosuna `workspace_id` ile upsert edilir.
8. `attempted` ile `persisted` eşit değilse işlem fail-closed olur.
9. Tarayıcıya yalnız redacted toplam sonuç döner.

Canonical provider adı `google_ads`, Dataset V2 platform değeri tamamlanmış E5 sözleşmesine uygun olarak `google` kalır.

## Doğrulanmış boş sonuç

Provider sonucu boşsa `attempted: 0`, `persisted: 0` döner. Sentetik satır oluşturulmaz. Bu, salt-okunur/provider zinciri ile boş persistence sınırının PASS olduğunu kanıtlar; gerçek non-empty fiziksel upsert'in gözlendiği anlamına gelmez.

## Dışarıda kalanlar

- Schedule veya dört saatlik otomasyon
- Backfill
- V1 snapshot veya Google Sheets yazımı
- GA4 aktivasyonu
- Yeni metrik, mapper veya formül
- Disconnect/revoke
- Sürekli production veri aktivasyonu

## Durum

Repository uygulaması ve testleri hazırlanacaktır. Production provider çağrısı ve Dataset V2 kabulü ayrı açık onay olmadan çalıştırılmaz.


## Canlı kabul sonucu — PASS

PR #280 merge commit `6b4d4c1abb7a9a3fe83959c176a0213abd3ffa90` production deployment sonrasında merchant kontrollü kabulü bir kez çalıştırdı. Sonuç `attempted: 0`, `persisted: 0`, `verified empty: true` oldu.

Salt-okunur Supabase son kontrolü tek canonical Google bağlantısını, üç doğrulanmış hesabı, token zarfları/access expiry ve merchant reporting currency kaydını doğruladı. Dataset V2 toplam, bağlı workspace, Google ve sentetik Google satırları ile browser grant sayısı `0` kaldı. Bu nedenle boş sonuç güvenli ve kanıtlıdır; production schedule/backfill aktivasyonu değildir. Sonraki paket R6-D4-F bağımsız Google Disconnect brief'idir.
