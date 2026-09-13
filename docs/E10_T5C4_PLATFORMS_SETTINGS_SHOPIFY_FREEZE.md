# E10-T5-C4 + C6 — Platforms ve Settings Shopify ürün freeze'i

**Durum:** `Done — kullanıcı iş akışı onaylandı; UI/runtime implementasyonu yapılmadı`  
**Karar tarihi:** 2026-09-09  
**Production etkisi:** Yok

Bu karar mevcut `public/dashboard.html` davranışını iş akışı referansı olarak korur, fakat mevcut özel CSS/HTML görünümünü Shopify embedded arayüzüne taşımaz. Uygulama yalnız güncel resmi Shopify App Home/App Bridge ve Polaris web component sözleşmesi E10-T6-A'da yeniden doğrulandıktan sonra yapılır. Shopify Admin shell'i kopyalanmaz; özel modal, button, select, navigation veya settings component framework kurulmaz.

## Onaylanan ilk kullanım sırası

1. Shopify merchant uygulamayı kurduktan sonra zorunlu **Currency selection** ekranını tamamlar.
2. Currency server-side workspace ayarı olarak kaydedilmeden Platforms açılmaz; browser değeri authority değildir.
3. Merchant daha sonra Shopify embedded **Platforms** sayfasına geçer.
4. İlk dilimde aktif bağlantı kartları yalnız `Meta`, `Google`, `TikTok` ve `Klaviyo`dur. Pinterest önceki ürün kararı uyarınca `Parked` kalır; GA4 Organic ve Google Sheets bu provider-connect yüzeyinde sunulmaz.

## Platforms

### Meta, Google ve TikTok

- `Connect` önce platformu ve salt-okuma/veri erişimi etkisini açıklayan Shopify-native modal açar. Modal içindeki ikinci `Connect` onayından sonra provider OAuth başlar.
- Consent ekranı iframe içinde açılmaz. Onaylı embedded transaction ve güncel resmi App Bridge top-level navigation sözleşmesi kullanılır.
- Callback sonrası gerçek ve server-side ownership doğrulamalı reklam hesapları listelenir. Merchant bunlardan **tek aktif reklam hesabı** seçerek akışı tamamlar.
- OAuth/token başarılı olsa bile aktif hesap seçilmeden durum `Account selection required` olur; `Connected` sayılmaz ve Funnel'a veri kaynağı olmaz.
- `Disconnect` Shopify-native warning modal açar. İkinci `Disconnect` onayı yeni provider erişimini ve refresh'i durdurur. Tarihsel analytics verisi otomatik silinmez; deletion ayrı privacy akışıdır.

### Klaviyo

- Connect açıklaması, OAuth, server-side doğrulanmış hesap seçimi ve Disconnect davranışı diğer aktif platformlarla aynıdır.
- Hesap seçiminden sonra mevcut iş sözleşmesindeki **Email Monthly Plan Cost** adımı zorunlu olarak açılır. Tutar ve para birimi kaydedilmeden Klaviyo bağlantı kurulumu tamamlanmış sayılmaz.
- Bu tutar tahminî reklam harcaması değildir; kullanıcı tarafından girilen aylık sabit e-posta plan maliyetidir. Funnel hesaplamasına girişi backend provenance/formül sözleşmesine bağlıdır.

## Platforms durum ve hata sözleşmesi

İzinli kullanıcı durumları `Not connected`, `Connecting`, `Account selection required`, `Connected`, `Reauthorization required`, `Temporarily unavailable` ve `Parked`dır. Ham provider error, HTML, OAuth code, token, shop/workspace/user identity veya credential kullanıcıya/loga taşınmaz. Yeniden yetkilendirme gereken bağlantıda eylem `Reconnect` olur ve Connect ile aynı açıklama/OAuth/hesap-seçim zincirini izler.

## Settings

Settings içinde üç Shopify-native bölüm bulunur:

1. **Currency:** İlk kullanımda seçilen workspace para birimini gösterir ve değiştirir. Değişiklik etkisi kaydetme öncesi açıklanır; rapor dönüşümü backend authority'sinde kalır.
2. **Klaviyo Email Monthly Plan Cost:** Klaviyo hesap seçiminden sonra girilen sabit tutar ve para birimini gösterir/değiştirir. `Estimated Monthly Spend` etiketi yasaktır.
3. **Ad Accounts:** OAuth/discovery ile ownership'i doğrulanmış Meta, Google ve TikTok reklam hesaplarını listeler. Aynı anda yalnız **bir** hesap aktif seçilebilir. Merchant istediği zaman başka bir hesaba geçebilir; Funnel yalnız güncel tek aktif hesabı gösterir. Seçim değiştirmek tokenı, connection'ı veya tarihsel veriyi silmez.

Ad Accounts listesi browser'ın gönderdiği serbest account ID/name listesinden kurulmaz. Backend kullanıcının workspace'ine ait bağlı ve erişilebilir hesap allowlist'ini döndürür; save isteği bu allowlist ve ownership üzerinde yeniden doğrulanır. Erişimi kaybolmuş hesap seçili kalamaz ve sahte fallback account üretilmez.

## Shopify-native component ve sayfa sınırı

- Sayfa ve gruplama: güncel resmi `s-page`, `s-section`, `s-stack`, `s-grid`, `s-box` eşdeğerleri.
- Eylem ve durum: `s-button`, `s-button-group`, `s-badge`, `s-banner`, `s-spinner`, `s-text` eşdeğerleri.
- Açıklama/uyarı: App Home modal web component'i veya güncel resmi App Bridge Modal API.
- Currency ve Klaviyo girdileri: güncel resmi select/text/number/form componentleri; label, help text, validation ve save state zorunludur.
- Tek aktif Ad Account: güncel resmi single-choice/radio component'i; checkbox veya multi-select değildir.
- Embedded navigation App Bridge üzerinden kurulur. Ayrı AdsTable global navigation, bağımsız login/dashboard dönüşü veya Shopify Admin taklidi yasaktır.

Exact component adları, property'leri, API sürümü, dış OAuth navigation yöntemi, focus return, destructive modal semantics ve responsive davranış E10-T6-A'da güncel resmi Shopify kaynaklarıyla tekrar doğrulanır. Bir component deprecated veya mevcut değilse özel taklit yapılmaz; güncel resmi karşılığı seçilip bu contract revize edilir.

## Erişilebilirlik ve responsive kabulü

Modal açıldığında focus modal içine taşınır, kapanınca tetikleyiciye döner; Escape/cancel destructive işlem yapmaz. Platform ve account durumu yalnız renkle anlatılmaz. Mobil embedded görünümde kartlar/alanlar tek kolona akar, primary action görünür kalır ve account seçimi yatay taşma gerektirmez.

## Kapsam dışı

Bu freeze Shopify UI implementasyonu, mevcut dashboard markup'ını refactor etme, OAuth route değişikliği, Partner Dashboard/Development Store işlemi, scope/redirect kaydı, provider çağrısı, migration, veri yazımı, deployment veya production işlemi yapmaz.

## Sıra kararı

Kullanıcı Platforms ve Settings davranışlarını birlikte verdiği için E10-T5-C4 ve E10-T5-C6 ürün kararları aynı freeze içinde kapatılmıştır. Bu bilinçli sıra güncellemesi C5 Attribution kapısını atlamaz: sıradaki uygulanabilir ürün paketi **E10-T5-C5 Attribution**dır. C7 ancak C5 de tamamlandıktan sonra açılır; E10-T6+ parent T5-C/C7 onayından önce başlamaz.
