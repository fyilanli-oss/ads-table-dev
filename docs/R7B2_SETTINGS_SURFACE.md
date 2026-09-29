# R7-B2 — Shopify-native navigation ve Settings yüzeyi

## Analist özeti

R7-B2, AdsTable içindeki ayarların kullanıcı açısından tek ve anlaşılır bir yerde toplanmasını sağlar. Bu paket provider bağlantılarının iş kurallarını veya veri modelini değiştirmez; yalnız navigasyon ve görünür yönetim yüzeyini R7-B1 kararına uyarlar.

## Kullanıcı davranışı

- **Dashboard**: Şimdilik yalnız doğrulanmış Shopify workspace bağlantı durumunu gösterir. Eski Data Sources tanıtımı ve yönetim butonu kaldırılır.
- **Funnel / Analysis**: Menü altyapısında görünür; ilgili execution paketleri tamamlanana kadar açıkça rezerv yüzey olarak kalır.
- **Settings**: Reporting Currency ve aktif provider yönetiminin tek kanonik yüzeyidir.
- **Reporting Currency**:
  - İlk kurulumda kullanıcı önce seçimi gözden geçirir, sonra kalıcı seçimi ayrıca onaylar.
  - Kaydedildikten sonra Settings içinde salt okunur görünür.
  - Bu pakette değiştirme veya silme davranışı yoktur.
- **Platforms**: Yalnız Meta, Google Ads ve Klaviyo gösterilir.
- **TikTok / Pinterest**: İç sistemde Parked kararları korunur; kullanıcıya kart, durum veya metin olarak gösterilmez.

## Adres ve geriye uyumluluk

- Kanonik adres: `/shopify/app/settings`
- Eski adres: `/shopify/app/platforms`
- Eski adres aynı Settings yüzeyini açan geçici uyumluluk alias'ıdır. R7-B3 OAuth callback dönüşünü kanonik Settings adresine taşıyana kadar mevcut işlemleri bozmaz.

## Bu pakette yapılmayanlar

- OAuth callback ve işlem devam ettirme davranışı
- Connect/Disconnect modal metinlerinin provider bazında nihai revizyonu
- Meta/Google Reporting Account seçimi
- Klaviyo effective-dated Monthly Amount geçmişi
- Reporting görünürlük ve historical disconnect davranışı
- Database migration, provider token veya Dataset V2 değişikliği

Bunlar sırasıyla R7-B3–B6 paketlerinde ele alınır.

## Kabul kapıları

1. Dashboard üzerinde Data Sources tanıtımı veya currency işlemi bulunmaz.
2. Settings iki ana kartı taşır: Reporting Currency ve Platforms.
3. Currency ilk seçiminde iki açık onay adımı vardır; sonrasında salt okunurdur.
4. Yalnız Meta, Google Ads ve Klaviyo görünür.
5. TikTok/Pinterest kullanıcı HTML'inde bulunmaz.
6. Kanonik Settings ve legacy Platforms adresleri aynı yüzeyi sunar.
7. Shopify-native App Bridge/Polaris markup korunur; nested iframe ve custom style eklenmez.
8. Repository testleri, güvenlik regresyonu ve deployment başarılı olur.
9. Gerçek Shopify merchant kontrolü yapılmadan paket `Done` sayılmaz.
