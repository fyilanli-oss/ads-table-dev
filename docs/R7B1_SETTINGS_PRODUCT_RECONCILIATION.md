# R7-B1 — Settings ürün ve veri davranışı uzlaştırması

**Durum:** Contract PASS / implementation pending  
**Karar tarihi:** 29 Eylül 2026  
**Production etkisi:** Yok

## İş çıktısı

AdsTable Settings, bağlantı işlemlerinin ve workspace tercihlerinin tek kullanıcı yüzeyi olur. Bu paket yalnız ürün ve veri davranışını dondurur; UI, veritabanı, OAuth, Dataset veya production değişikliği yapmaz.

## Navigasyon

Shopify embedded uygulama navigasyonu `Dashboard / Funnel / Analysis / Settings` olur. Home gelecekteki Dashboard için ayrılır. Dashboard gelene kadar Connection Status kalabilir; Data Sources başlığı, açıklaması ve butonu Home'dan kaldırılır. Mevcut `/shopify/app/platforms` adresi OAuth dönüşleri kesintiye uğramasın diye yeni Settings yüzeyi kabul edilene kadar uyumluluk adresi olarak korunur.

Kullanıcıya yalnız Meta, Google Ads ve Klaviyo gösterilir. TikTok ve Pinterest iç sistemde Parked kalır; kart, başlık veya Unavailable durumu olarak gösterilmez.

## Reporting Currency

Reporting Currency merchant tarafından ilk kurulumda seçilir. İlk Save sonrasında geri alınamaz sonucu açıklayan ikinci modal açılır; kullanıcı açık acknowledgement vermeden kayıt oluşmaz. Onaylanan currency değiştirilemez ve Settings'te salt okunur gösterilir.

Uninstall, currency'yi değiştirmez ve otomatik veri silmez. Veri silme rızası, doğrulanmış silme ve temiz yeniden kurulum ayrı bir **Workspace Data Deletion and Clean Reinstall Lifecycle** paketidir.

## OAuth dönüşü ve kurulumun devamı

Connect Settings içinden başlar. Provider consent tamamlandığında callback Home'a değil sabit Settings hedefine döner. Browser return target authority değildir. Backend connection durumuna göre doğrulanmış hesap seçimi; Klaviyo'da bunun ardından Monthly Amount adımı otomatik açılır. Zorunlu adımlar bitmeden Connected gösterilmez. Kullanıcı akışı kapatırsa Settings'te Resume setup bulunur.

Connect/Disconnect mesajları çalışan legacy `server.js + dashboard.html` metinleri okunarak hazırlanır ve güncel provider davranışına göre revize edilir. Meta mesajı doğru Facebook hesabının reklam hesabı erişimine sahip ve Connect yapılan cihaz/tarayıcıda açık olması gerektiğini açıklar. Disconnect; erişim/refresh'in duracağını, tarihsel verinin kalacağını ve diğer provider'ların etkilenmeyeceğini belirtir.

## Meta ve Google Reporting Account

Meta ve Google OAuth bağlantısı 1–3 provider-doğrulanmış hesap taşıyabilir. Raporlarda tam bir Reporting Account kullanılır. Settings'ten yapılan değişiklik provider ownership'i yeniden doğrular; OAuth grant'ini, tokenı, bağlı hesap kümesini veya tarihsel Dataset'i silmez.

Disconnect canlı provider erişimini ve refresh'i durdurur. Tarihsel Dataset V2 ile son reporting tercihi live token state'inden bağımsız korunur.

## Klaviyo Connected Account ve Monthly Amount

Klaviyo tek doğrulanmış Connected Account taşır; ayrı Reporting Account butonu gösterilmez. Account değişikliği basit filtre değişimi değil, provider yeniden doğrulamasıdır.

Monthly Amount tek scalar değer değildir. Account bazlı effective-dated tarihçe tutulur:

- **Edit/Correct amount:** Yanlış girilmiş mevcut dönemi düzeltir; onuncu günde fark edilen hata için onuncu günde yeni fiyat dönemi oluşturmaz.
- **New amount:** Gerçek fiyat değişikliğini yalnız ayın ilk gününden başlatır ve önceki dönemi kapatır.
- Dönemler çakışamaz; currency provider tarafından doğrulanmış source currency'dir.
- Etkilenen Dataset V2 satırları varsa düzeltme yalnız ayrı kontrollü ve idempotent recalculation ile yansıtılır.

## Rapor görünürlüğü

Hiç bağlanmamış provider Dashboard/Funnel/Analysis içinde görünmez. Disconnect edilmiş fakat tarihsel Dataset verisi bulunan provider görünmeye devam eder; `Disconnected` ve `Data through/Freshness` bilgisi gösterilir. Disconnect tarihsel analytics'i silmez.

## Uygulama sırası

1. R7-B2 — Navigation ve Settings yüzeyi.
2. R7-B3 — OAuth Settings dönüşü, otomatik resume ve modal metinleri.
3. R7-B4 — Meta/Google Reporting Account.
4. R7-B5 — Klaviyo effective-dated Monthly Amount.
5. R7-B6 — Rapor görünürlüğü ve historical disconnect.
6. R7-B7 — Bütünleşik gerçek cihaz kabulü.

Her alt paket başlamadan analist brief'i, etkilenen veri davranışı, kabul sonucu ve rollback sınırı açıklanır. Kanıtlanmamış aşama Done sayılmaz.

## Bu pakette yapılmayanlar

UI/runtime kodu, migration, provider çağrısı, OAuth/token değişikliği, Dataset V2 yazımı veya yeniden hesaplama, deployment, production aktivasyonu ve veri silme yapılmamıştır.
