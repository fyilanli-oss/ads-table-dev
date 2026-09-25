# R6-D3-C — Meta production merchant kabul koşucusu

## Analist amacı

Bu koşucu yeni özellik geliştirmez. Repository'de hazırlanmış Meta OAuth ve 1–3 hesap seçimi akışının gerçek Shopify merchant oturumunda doğru çalıştığını tek geçişte kanıtlar. Kanıtlanmayan hiçbir adım `PASS` sayılmaz.

## Kullanıcı açısından beklenen sonuç

1. Kullanıcı Shopify içindeki **Data sources** ekranından Meta için **Connect** seçer.
2. Meta izin ekranı tamamlanır.
3. AdsTable, Meta'nın doğruladığı reklam hesaplarını Shopify-native seçim modalında gösterir.
4. Kullanıcı en az 1, en fazla 3 reklam hesabı seçip **Save** der.
5. Meta kartı ancak bundan sonra **Connected** olur.
6. Sayfa yenilendiğinde bağlantı ve seçilen hesap sayısı korunur.

Bu aşamada reklam performans verisi çekilmez; Dataset V2 yazılmaz; schedule veya backfill başlatılmaz.

## Başlamadan önce

- R6-D3-A, R6-D3-B ve R6-D3-C repository değişiklikleri review edilip production'a dağıtılmış olmalıdır.
- `dev.adstable.app` production alias'ı review edilen deployment'a bağlı olmalıdır.
- Kullanıcı bilgisayarda Shopify ve Meta izin ekranını tamamlayabilecek durumda olmalıdır.
- `docs/security/sql/R6D3C_META_ACCOUNT_SELECTION_PREFLIGHT.sql` salt-okunur olarak çalıştırılır.
- Preflight sonucu `pass = true` değilse OAuth başlatılmaz.
- `legacy_meta_connection_baseline` değeri kabul kaydına yazılır; legacy kayıt değiştirilmez veya silinmez.

## Merchant kabul adımları

1. Shopify Admin içinden AdsTable açılır; doğrudan `dev.adstable.app` sayfasından başlanmaz.
2. **Data sources** ekranında Meta kartının `Not connected` ve **Connect** durumda olduğu doğrulanır.
3. **Connect** seçilir ve açıklama modalından bağlantı başlatılır.
4. Meta izin ekranı tamamlanır.
5. Shopify'a dönüşte Meta hesap seçim modalının kendiliğinden açıldığı doğrulanır.
6. Modalda gerçek Meta hesaplarının gösterildiği doğrulanır; kullanıcı 1–3 hesap seçer.
7. **Save** seçilir.
8. Meta kartının `Connected` olduğu ve seçili hesap sayısını gösterdiği doğrulanır.
9. Shopify sayfası bir kez yenilenir; `Connected` durumu ve hesap sayısının korunduğu doğrulanır.

## Son kontrol

`docs/security/sql/R6D3C_META_ACCOUNT_SELECTION_POSTCHECK.sql` salt-okunur olarak çalıştırılır.

Nihai `PASS` için aşağıdaki koşulların tamamı gerekir:

- Postcheck `pass = true` döndürür.
- `legacy_meta_connection_after`, preflight'ta kaydedilen `legacy_meta_connection_baseline` ile aynıdır.
- UI adımlarının 1–9'u gerçek merchant tarafından doğrulanmıştır.
- OAuth sonrası hesap seçim modalı görülmüştür.
- 1–3 hesap seçilmiş ve reload sonrasında korunmuştur.
- Dataset V2 Meta satırı, aktif Meta schedule ve açık Meta job sayısı `0` kalmıştır.

## STOP koşulları

Aşağıdakilerden biri oluşursa kabul durdurulur ve `PASS` yazılmaz:

- Connect başlamaz veya genel hata verir.
- OAuth tamamlanmasına rağmen hesap seçim modalı açılmaz.
- Meta hesapları provider'dan alınamaz.
- Sıfır hesapla veya üçten fazla hesapla kayıt yapılabilir.
- Save sonrasında kart `Connected` olmaz.
- Reload sonrasında bağlantı ya da hesap seçimi kaybolur.
- Preflight/postcheck `pass = false` döndürür.
- Legacy Meta kayıt sayısı değişir.
- Dataset V2, schedule veya job sayılarından biri artar.

STOP halinde otomatik tekrar, veri çekimi, legacy kaydı taşıma/silme veya tahmine dayalı kod değişikliği yapılmaz. Önce gerçek hata kodu, production logu ve salt-okunur database sonucu birlikte incelenir.

## Kanıt ve gizlilik sınırı

Kalıcı kabul kaydı yalnız aşağıdakileri içerebilir:

- deployment commit ve READY/alias sonucu,
- preflight/postcheck boolean ve aggregate sayıları,
- merchant'ın doğruladığı UI adımları,
- seçilen hesap adedi,
- zaman damgası ve PASS/STOP kararı.

Token, authorization code, hesap ID'si, workspace ID'si, kullanıcı ID'si veya provider payload'ı kanıta yazılmaz. Ekran görüntüsü zorunlu değildir; yalnız kullanıcı özellikle isterse alınır.

## Durum

`Prepared locally — not deployed, not executed, production merchant acceptance pending`
