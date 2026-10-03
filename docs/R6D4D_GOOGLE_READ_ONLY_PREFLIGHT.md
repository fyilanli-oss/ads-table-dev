# R6-D4-D — Google Ads salt-okunur çalışma doğrulaması

## Analist özeti

Bu paket, R6-D4-C aşamasında bağlanan ve kullanıcı tarafından seçilen en az bir, en fazla üç Google Ads hesabının veri okuma zincirini sınar. Yeni bir rapor, metrik veya veri modeli kurmaz. Execution Plan'da tamamlanmış E5 Google Ads motorunu Shopify workspace yetkisiyle çağıran ince bir kontrol katmanıdır.

Başarılı sonuç şu anlama gelir:

1. İstek doğrulanmış Shopify oturumundan gelir ve workspace tarayıcıdan alınmaz.
2. Canonical bağlantıdaki her seçilmiş hesabın kendi `login_customer_id` manager bağlamı kullanılır.
3. Google Ads'ten hesap kimliği, kaynak para birimi ve saat dilimi yeniden okunur.
4. Her hesabın saat dilimine göre kapanmış önceki iş günü belirlenir.
5. E5'in mevcut Standard Ads ve Performance Max sorguları ayrı ayrı çalıştırılır.
6. Kaynak para birimi, kullanıcının daha önce seçtiği AdsTable raporlama para birimine mevcut FX sözleşmesiyle normalize edilir.
7. Sonuç boşsa bile bu durum ancak bütün provider sorguları başarılı olduktan sonra doğrulanmış boş sonuç kabul edilir.

## Veri etkisi

- Dataset V2 yazımı yoktur.
- Dataset V1, snapshot, schedule ve backfill yoktur.
- Google Sheets ve GA4 parked kalır.
- Normal Data sources ekranına yeni kullanıcı adımı eklenmez. Kontrol yalnız `?acceptance=r6d4-google` operatör parametresiyle görünür.
- Tarayıcıya hesap kimliği, token, satır içeriği veya provider hata gövdesi dönmez; yalnız toplu kabul özeti döner.
- Mevcut R6-D4-B token yaşam döngüsü geçerlidir. Access token süresi dolmuşsa veya Google bir kez yetkisiz yanıt verirse refresh token ile kontrollü yenileme yapılabilir. Bu yalnız credential envelope bakımıdır; analitik veri yazımı değildir.

## Kullanılan tamamlanmış yapılar

- E5 müşteri metadata sorgusu
- E5 Standard Ads sorgu ve mapper'ı
- E5 Performance Max sorgu ve mapper'ı
- E5 dönüşüm eşlemesi
- Mevcut Time ve FX motoru
- R6-D4-B token yenileme ve tek retry kuralı
- Canonical `workspace_provider_connections` ve merchant-selected `workspace_settings` otoritesi

## Kabul ölçütleri

- Seçili bütün hesaplar okunmuş olmalıdır.
- Her hesap için canonical `login_customer_id` kullanılmış olmalıdır.
- Provider kaynak para birimi canonical seçimle eşleşmelidir.
- Standard ve Performance Max dalları her hesap için başarıyla tamamlanmalıdır.
- Time/FX doğrulaması geçmelidir.
- Sonuç yalnız `non_empty` veya provider tarafından doğrulanmış `empty` olabilir.
- Dataset V2 satır sayısı bu aşamada değişmemelidir.

## Durum

Production salt-okunur kabulü PASS olmuştur. Üç canonical Google Ads hesabında Standard ve Performance Max dalları çalışmış, provider sonucu doğrulanmış boş (`0` satır) dönmüş, Time/FX kontrolleri geçmiş ve Dataset V2 yazımı `0` kalmıştır. Supabase son kontrolü tek connected canonical bağlantıyı, üç seçilmiş hesabı, encrypted access/refresh token zarflarını, reporting currency kaydını, workspace ve Google Dataset V2 satırlarının `0` olduğunu, sentetik Google satırının `0` ve tarayıcı rol grant'inin `0` olduğunu doğrulamıştır. Redacted kanıt `docs/security/evidence/R6D4D_GOOGLE_READ_ONLY_LIVE_ACCEPTANCE_2026-09-26.json` içindedir. Sıradaki çalışma ayrı analist brief'i ve onayla R6-D4-E kontrollü Dataset V2 kabulüdür.



## 3 Ekim 2026 corrective — provider response evidence

Önceki canlı sonuç yalnız üç hesapta canonical satır sayısının sıfır olduğunu gösterdi. Hesaplarda Standard Campaign → Ad Group → Ad veya Performance Max Campaign → Asset Group yapısının bulunup bulunmadığı, sorguların hangi alanları istediği ve Google SearchStream'in her sorguya kaç sonuç/chunk döndürdüğü görünmedi. Bu nedenle önceki reporting-completeness boş sonucu yeniden açıldı.

Google Ads'in güncel resmî test-account belgesi test hesaplarının reklam yayınlamadığını ve impressions, conversions ve cost gibi serving metriklerinin boş olduğunu açıkça belirtir. Bu provider kuralı beklenen boşluğu açıklar; fakat canlı hesap yapısı ve gerçek response kanıtının yerine geçmez.

Corrective salt-okunur kabul her seçilmiş hesap için provider kimliği göstermeden şunları raporlar:

- customer metadata;
- Standard yapı envanteri;
- Performance Max yapı envanteri;
- önceki kapanmış gün Standard/PMax performance ve conversion sorguları;
- son 31 kapanmış gün Standard/PMax performance ve conversion sorguları;
- her sorgunun exact selected field listesi, provider result count'u, SearchStream chunk count'u ve güvenli field-mask yolları.

Conversion evidence hem `metrics.conversions / conversions_value` hem de `metrics.all_conversions / all_conversions_value` alanlarını ister. Dataset V2 write, schedule ve backfill kapalıdır. Gerçek Shopify oturumundaki corrective sonuç görülmeden empty/unsupported PASS tekrar verilemez.

## 3 Ekim 2026 canlı corrective — ham yanıt kapısı açık

Gerçek Shopify oturumundaki ilk corrective çalışma `PASS — 3 account(s), 0 verified canonical row(s)` ve `33` sorgu / `33` SearchStream chunk aggregate kanıtı verdi. Ancak bu çalışma Google'ın exact HTTP/SearchStream response gövdesini göstermedi; yalnız uygulamanın ham yanıttan çıkardığı result/chunk/field-mask özetini gösterdi. Bu nedenle empty-by-account-structure kararı henüz final değildir.

Yeni kontrollü salt-okunur çalışma, customer metadata dışındaki Standard ve Performance Max structure/performance/conversion sorgularında yalnız sonuç satırı `0` ise exact response body string'ini authenticated operator çıktısına taşır. Non-empty body provider ID veya satır içeriği sızdırmamak için fail-closed gizlenir. Credential, token ve customer/login-customer ID hiçbir durumda raw evidence alanına girmez.

Dataset V2 write, schedule ve backfill kapalıdır. Gerçek Shopify oturumunda raw response body görülüp kullanıcıyla birlikte değerlendirilmeden Google reporting-completeness PASS veya unsupported kararı verilemez.

## 3 Ekim 2026 final canlı corrective — Standard Ad adı ve ham verified-empty kanıtı

Production'daki ayrı salt-okunur tekrar üç canonical Google Ads hesabında `PASS — 3 account(s), 0 verified canonical row(s)` verdi. Toplam `33` sorgunun her biri bir SearchStream chunk ile tamamlandı; üç customer metadata sorgusu birer sonuç döndürürken customer metadata dışındaki `30` Standard/PMax structure, performance ve conversion sorgusu sıfır sonuç döndürdü.

Standard structure sorgusunun exact selected alanları şunlardır:

```text
campaign.id
campaign.name
campaign.advertising_channel_type
campaign.status
ad_group.id
ad_group.name
ad_group.status
ad_group_ad.ad.id
ad_group_ad.ad.name
ad_group_ad.status
```

Üç hesabın her birindeki ham response body aynı field mask'i doğruladı:

```json
[{"fieldMask":"campaign.id,campaign.name,campaign.advertisingChannelType,campaign.status,adGroup.id,adGroup.name,adGroup.status,adGroupAd.ad.id,adGroupAd.ad.name,adGroupAd.status","requestId":"<redacted>","queryResourceConsumption":"<provider-reported>"}]
```

Body içinde `results` property bulunmadı. Bu nedenle sonuç uygulama tarafından üretilmiş bir boşluk değil, Google SearchStream'in başarılı fakat sonuçsuz cevabıdır. `adGroupAd.ad.name` hem sorgunun exact selected-field listesinde hem canlı provider `fieldMask` değerinde bulunduğu için Ad adı isteğinin Google tarafından kabul edildiği kanıtlanmıştır.

Bu sonuç Google Ads bağlantı, sorgu ve alan kapsamını doğrular; sıfır sonucun provider-side nedeni hakkında test hesabı, kampanya durumu veya başka bir varsayım fact olarak yazılmaz. Gerçek provider satırı oluşana kadar non-empty metric ve Dataset V2 acceptance açık kalır. Time/FX PASS, Dataset V2 yazımı `0`, schedule/backfill/activation kapalıdır. Redacted kanıt `docs/security/evidence/R6D4D_GOOGLE_RAW_RESPONSE_LIVE_ACCEPTANCE_2026-10-03.json` içindedir.
