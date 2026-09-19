# E10-T6-C2I-V9 — Real-device Shopify-native Acceptance

**Durum:** `PASS — corrective accepted with redacted real-device evidence`
**İlk yürütme onayı:** 2026-09-13
**Kabul tarihi:** 2026-09-20
**Provider teması:** Yasak

## Amaç ve mevcut sonuç

V9 gerçek cihaz kabulü 19 Eylül 2026 tarihinde insan gözlemiyle yürütüldü. App Home ve Data Sources/Platforms Shopify Admin içinde açıldı; insan hiçbir provider `Connect` düğmesine basmadı. Buna rağmen ürün yüzeyi acceptance `PASS` değildir.

Gözlenen başarısızlıklar:

- App Home, `Manage data sources` eylemine ek olarak ikinci ve durumdan habersiz bir Klaviyo `Connect` kartı gösterdi.
- Data Sources kayıtlı Klaviyo bağlantısını `Connected` gösterirken App Home aynı bağlantıyı `Connect` gösterdi.
- Provider bölümleri aynı bağlantı cümlesini tekrar etti.
- Ürün freeze'inde `Parked` olan Pinterest aktif `Connect` eylemiyle gösterildi.
- Data Sources ilk render'ı Klaviyo account endpoint'ini otomatik çağırdı; bu endpoint provider Account API'sine gidebildiği ve token yenileyebildiği için insan Connect'e basmasa dahi V9 provider-teması sınırı korunmadı.

Ham insan görüntüleri shop/provider kimliği içerdiği için repository'ye eklenmedi. Bu görüntüler `PASS` kanıtı olarak kullanılmaz; yalnız `FAIL` teşhisini oluşturdu. Corrective tamamlanmadan yeni capture/redaction işi istenmez.

Bu paket yalnız Shopify-native render'ı gözlemler. `Connect` düğmesine basılmaz; OAuth, provider consent, callback, account discovery, token exchange, provider API isteği, deployment veya Production mutation yapılmaz.

## Corrective acceptance sonucu — 20 Eylül 2026

V9 product UI corrective Production'a yayınlandıktan sonra gerçek cihazda App Home ve Data Sources/Platforms yeniden gözlendi. İnsan yalnız `Manage data sources` iç navigasyonunu kullandığını; hiçbir provider `Connect` düğmesine basmadığını ve OAuth/izin ekranı açılmadığını açıkça doğruladı.

- App Home yalnız store bağlantı durumu ve `Manage data sources` eylemini gösterdi; mükerrer Klaviyo kartı yoktu.
- Data Sources provider açıklamalarını ayrıştırılmış biçimde gösterdi.
- Klaviyo hesap kimliği göstermeden `Connected · 5.00 USD/month` durumunu gösterdi ve Klaviyo `Connect` eylemini gizledi.
- Pinterest `Parked` ve pasif `Connect` eylemiyle gösterildi.
- Görüntüler repository'ye eklenmeden önce mağaza/avatar kimliği maskelendi; ham JPG dosyaları repository'ye alınmadı.
- İki metadata-free PNG, SHA-256 değerleri ve insan privacy/interaction attestation'ı `artifacts/e10-shopify/e10-t6-c2i-v9/evidence.json` içinde validator tarafından doğrulandı.

Bu sonuç V9'u `PASS` yapar. Sıradaki uygulanabilir paket V10-A Klaviyo Connect açıklama modalıdır; V10-A bu kanıt paketinin review/merge kapısından önce başlatılmaz.

## Corrective kapsamı

1. App Home yalnız store bağlantı durumu ve Data Sources girişini gösterir; provider-specific Klaviyo kartı göstermez.
2. Data Sources provider açıklamaları platform amacını kısa ve tekrarsız anlatır.
3. Pinterest `Parked` gösterilir ve aktif provider OAuth boundary'si taşımaz.
4. Normal Data Sources açılışı yalnız doğrulanmış Shopify session üzerinden yerel connection status okur; Klaviyo API'sine, token refresh'e veya account discovery'ye gitmez.
5. Kayıtlı Klaviyo bağlantısı hesap kimliği göstermeden `Connected` ile aylık plan maliyeti/currency bilgisini gösterir ve çelişkili `Connect` eylemini gizler.
6. Bu corrective Connect modalı, OAuth, account selection, cost mutation veya Disconnect uygulamaz; bunlar V10 kapılarında kalır.

## PASS sonrası insan capture prosedürü

1. Gerçek cihazda Shopify Admin'e giriş yap ve AdsTable uygulamasını Shopify Admin içinden aç.
2. App Home tamamen render olduktan sonra görüntüyü al. `Home`/`Data sources` app navigation'ı, Shopify-native page/section/banner/action bileşenleri ve `Manage data sources` eylemi görünmelidir.
3. Yalnız `Manage data sources` iç navigasyonunu kullanarak Data Sources/Platforms ekranına geç. Bu eylem provider Connect değildir.
4. Platforms ekranında provider bölümleri ve Connect eylemlerinin render olduğunu gösteren ikinci görüntüyü al. Hiçbir provider `Connect` düğmesine basma.
5. Repository'ye eklemeden **önce** görüntülerden mağaza adı/domaini, avatar, e-posta, kişi adı, Shopify hesap/organizasyon kimliği, provider account kimliği ve browser chrome içindeki kimlik belirteçlerini kaldır.
6. Redaction sonrasında görüntülerin UI kararını değerlendirmeye yetecek içeriği koruduğunu gözle kontrol et. Tamamen kapatılmış veya hangi yüzey olduğu anlaşılamayan görüntü kabul edilmez.
7. Redacted dosyalar teslim edildiğinde her dosyanın SHA-256 değeri evidence manifestine yazılır ve insan incelemesiyle privacy attestation tamamlanır.

Pending contract'taki listeler gözlenmiş `PASS` değerleri değildir; kabul sırasında kanıtlanması gereken gereksinimlerdir. İki görüntü yalnız PNG olarak, sırasıyla `artifacts/e10-shopify/e10-t6-c2i-v9/app-home-redacted.png` ve `artifacts/e10-shopify/e10-t6-c2i-v9/platforms-redacted.png` yollarına eklenir. Evidence JSON'u `node scripts/e10-t6-c2i-v9-evidence.js <evidence.json>` ile doğrulanır. Validator exact dosya/route sırasını, SHA-256 değerlerini, farklı görüntüleri, güvenli boyutları, PNG yapısını ve PNG text/EXIF metadata yokluğunu fail-closed doğrular. İnsan görsel/privacy attestation'ının yerini almaz.

Evidence tesliminde `contracts/shopify/e10-t6-c2i-v9-evidence.template.json` kopyalanır; template dosyasının kendisi değiştirilmez. `REVIEW_REQUIRED`, placeholder timestamp/hash ve bütün gözlemsel attestation'ların `false` başlangıç değeri bilinçlidir. Reviewer yalnız gerçekten gözlediği kapıları `true` yapar, çıktı manifestini iki redacted PNG ile aynı artifact dizinine koyar ve validator'ı çalıştırır. Template hiçbir koşulda acceptance evidence veya `PASS` sayılamaz.

## Görsel kabul matrisi

### App Home

- Shopify Admin içinde embedded olarak görünür.
- Resmi App Bridge/Polaris App Home bileşenleri doğal Shopify görünümünde render olur.
- App navigation ve `Manage data sources` eylemi görünür ve anlaşılırdır.
- Özel Shopify Admin taklidi, özel component CSS'i veya kullanıcıya dönük teknik release işareti yoktur.

### Data Sources / Platforms

- Aynı Shopify embedded uygulama navigasyonu içinde görünür.
- Provider eylemleri resmi Shopify button görünümündedir ve durum yalnız renkle anlatılmaz.
- Nested provider iframe, açılmış modal, consent sayfası veya provider sonucu görünmez.
- Bu aşamada yalnız render gözlemlenir; Connect davranışı V10-A ve sonrası için ayrıdır.

## Resmi Shopify kaynak sınırı

- App Home, uygulama yüzeyinin Shopify Admin içinde iframe olarak çalıştığını ve Polaris web components kullandığını tanımlar: <https://shopify.dev/docs/api/app-home>
- App navigation, uygulamanın Shopify Admin navigation menüsüne link sağlaması için resmi `s-app-nav` bileşenini tanımlar: <https://shopify.dev/docs/api/app-home/latest/app-bridge-web-components/app-nav>
- Page ve Button bileşenleri, sayfa yapısı ve kullanıcı eylemleri için resmi App Home bileşenleridir: <https://shopify.dev/docs/api/app-home/latest/web-components/structure/page> ve <https://shopify.dev/docs/api/app-home/latest/web-components/actions/button>

Görüntüdeki görsel benzerlik kaynak sözleşmesinin yerini tutmaz; source/test kapıları ile insan render kanıtı birlikte geçmelidir.

## PASS / FAIL kararı

`PASS` yalnız aşağıdakilerin tamamında verilir:

- iki zorunlu redacted görüntü teslim edilmiştir;
- her görüntünün SHA-256 değeri manifestte kayıtlıdır;
- bütün görsel kapılar insan tarafından doğrulanmıştır;
- privacy attestation tamamlanmıştır;
- Connect/provider/OAuth/API temaslarının hiçbirinin yapılmadığı doğrulanmıştır.

Eksik görüntü, yetersiz redaction, eski/custom yüzey, teknik release marker, nested iframe veya yanlışlıkla Connect'e basılması `PASS` değildir. Hata türü redacted olarak kaydedilir ve V9 corrective açılır; V10 execution başlamaz.

## Paket sırası

- **Tamamlanan paket:** V9 execution authorization, corrective, insan capture, redaction ve hash/attestation kabulü.
- **Sıradaki paket:** V10-A Klaviyo Connect modalı.
- **Sonraki paketler:** V10-B ayrı provider consent kararı; V10-C verified Klaviyo account selection; V10-D Email Monthly Plan Cost; V10-E Disconnect; V10-F ayrıca onaylı read-only Production API smoke.

V9 `PASS` kanıtı bu paketle hazırlanmıştır. V10-A ancak bu evidence paketinin review/merge kapısından sonra ayrı kapsam olarak başlatılır.
