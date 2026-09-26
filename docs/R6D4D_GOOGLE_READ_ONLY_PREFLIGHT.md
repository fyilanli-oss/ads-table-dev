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

Repository uygulaması hazırlanmıştır; production kabulü henüz yapılmamıştır. Canlı kontrol ve salt-okunur Supabase son kontrolü kanıtlanmadan R6-D4-D tamamlanmış sayılmaz. Canlı PASS sonrasında sıradaki çalışma ayrı analist brief'i ve onayla R6-D4-E kontrollü Dataset V2 kabulüdür.

