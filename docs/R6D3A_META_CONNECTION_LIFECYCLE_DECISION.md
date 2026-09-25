# R6-D3-A — Meta bağlantı ve token yaşam döngüsü kararı

## İş çıktısı ve iş değeri

AdsTable, eski ve süresi geçmiş Meta bağlantısını yeni Shopify workspace bağlantısı gibi kullanmayacak. Merchant temiz OAuth ile bağlanacak; Meta token'ı doğrulanacak ve kullanıcı provider tarafından doğrulanmış 1–3 reklam hesabını seçmeden bağlantı `Connected` görünmeyecek.

Bu karar, eski kaydın yanlışlıkla yeniden canlandırılmasını, saatler/aylar sonra habersiz veri kesilmesini ve hesap seçilmeden bağlantının tamamlanmış görünmesini engeller.

## Amaç

R6-D3 Meta canlı kabulünden önce legacy bağlantının kaderini, Meta token ömrünü, canonical durum geçişlerini ve R6-D3-B uygulama sınırını kanıtlanabilir biçimde dondurmak.

## Mevcut durum

- Execution Plan içindeki E4 Meta veri motoru, mapping, hierarchy, Time/FX, Dataset V2 writer, retry ve parity işleri tamamlanmıştır; yeniden yapılmayacaktır.
- Eski `server.js + dashboard.html` deneyiminde OAuth sonrasında hesap seçimi açılır ve server en fazla üç hesabı kabul eder.
- Canlı Supabase salt-okunur envanterinde legacy Meta bağlantısı durum olarak `connected` görünür; encrypted access token vardır fakat expiry geçmiştedir ve refresh token yoktur.
- Canonical `workspace_provider_connections` içinde Meta bağlantısı yoktur.
- Mevcut embedded callback authorization code'u access token'a çevirir; long-lived exchange ile app/scope/expiry doğrulamasını henüz tamamlamaz.

## Planlanan durum

1. Legacy Meta bağlantısı ve tarihsel kayıtları korunur; canonical workspace bağlantısına taşınmaz, token'ı kullanılmaz ve kayıt silinmez.
2. Legacy schedule pasif kalır; otomatik job, backfill veya V1 yazımı başlamaz.
3. Merchant Meta'yı Shopify embedded yüzeyinden temiz OAuth ile bağlar.
4. Server authorization code'u token'a çevirir, Meta'nın desteklenen long-lived token modelini uygular ve canonical kayıttan önce token geçerliliğini, app bağını, gerekli scope'ları ve expiry'yi doğrular.
5. Meta için var olmayan bir refresh token mekanizması uydurulmaz. Yenileme desteklenmiyor veya token geçersizse bağlantı kontrollü `Reconnect Meta` durumuna geçer.
6. Callback yalnız `pending_account_selection` üretir. Kullanıcı provider'dan yeniden doğrulanmış en az 1, en fazla 3 reklam hesabı seçince bağlantı `Connected` olur.

## Kapsam

- Legacy/canonical ayrım kararı.
- Meta long-lived access-token stratejisi.
- Token app, validity, scope ve expiry doğrulama şartları.
- `not_connected → oauth_in_progress → token_validated → pending_account_selection → connected` durum akışı.
- Kontrollü `reauthorization_required / Reconnect Meta` davranışı.
- 1–3 provider-doğrulanmış hesap otoritesi.
- R6-D3-B uygulama paketinin kesin sınırı.

## Kapsam dışı

- Meta Insights/performance API çağrısı.
- Dataset V2 yazımı.
- Schedule veya backfill aktivasyonu.
- E4 mapper, hierarchy, Time/FX, writer ya da formula işlerinin yeniden geliştirilmesi.
- Legacy token kullanımı veya legacy kayıt silme.
- Disconnect/revoke uygulaması; bu R7-B kapısında ele alınır.
- Production Meta OAuth veya provider teması.

## Bağımlılıklar

- R4 canonical workspace provider connection authority: `Done`.
- R7-A reporting currency ve 1–3 hesap seçim temeli: merchant acceptance `PASS`.
- E4 Meta veri motoru: `Done`; yalnız R6 workspace runner composition aşamasında yeniden kullanılacak.
- R6-D2 Klaviyo canlı kabulü: `PASS`; Meta'yı otomatik aktive etmez.

## Uygulama adımları

1. Bu kararın Execution Plan ve versionlı executable contract ile dondurulması.
2. R6-D3-B'de server-side long-lived exchange ve token doğrulamasının uygulanması.
3. Callback'in `pending_account_selection` durumunda durduğunun ve 1–3 doğrulanmış hesap seçimi olmadan `Connected` olamadığının test edilmesi.
4. Ayrı review sonrasında, production provider teması olmadan deployment hazırlığının doğrulanması.
5. Production OAuth ve canlı kabul için ayrıca açık onay alınması.

## Kabul kriterleri

- Legacy Meta kaydı canonical workspace'e taşınmaz ve silinmez.
- Legacy access token yeni bağlantıda kullanılmaz; legacy schedule pasif kalır.
- Canonical token yalnız encrypted envelope ve bilinen gelecekteki expiry ile saklanabilir.
- Token doğru Meta app'e ait değilse, geçersizse veya gerekli scope eksikse canonical bağlantı güncellenmez.
- Refresh token alanı/akışı Meta için başarı varsayımı olarak kullanılmaz.
- Callback `Connected` üretmez.
- Bağlantı ancak server'ın provider'dan yeniden doğruladığı 1–3 hesap seçildikten sonra `Connected` olur.
- Token yeniden yetkilendirme gerektiriyorsa kullanıcı kontrollü `Reconnect Meta` görür.
- Dataset V2, schedule, backfill ve E4 yeniden geliştirme bu pakette bulunmaz.

## Test planı

- Versionlı JSON contract parse testi.
- Legacy migration/use/delete kararlarının negatif assertion'ları.
- Long-lived exchange, app/scope/expiry doğrulama assertion'ları.
- Callback ve 1–3 hesap seçim state-transition assertion'ları.
- Kapsam dışı Dataset/schedule/provider-contact assertion'ları.
- `git diff --check`.

## Rollback planı

Bu paket yalnız plan, karar ve contract değiştirir. Rollback ilgili commit'in geri alınmasıdır. Production bağlantı, provider grant, database veya Dataset V2 verisi değişmediği için veri rollback'i gerekmez.

## Gözlemlenebilirlik

R6-D3-B ve sonrası, token veya hesap ID'si loglamadan şu redacted durumları ayıracaktır: token exchange başarısız, app mismatch, scope eksik, expiry geçersiz, account selection gerekli, reauthorization gerekli ve connected.

## Güvenlik ve veri etkisi

- Browser workspace, app veya account authority sağlayamaz.
- Token plaintext saklanmaz veya loglanmaz.
- Legacy token decrypt/use/migrate edilmez.
- Stale connection version yeni bağlantının üzerine yazamaz.
- Bu karar aşamasında Supabase mutation, provider teması ve Dataset V2 yazımı yoktur.

## Planlanan

Meta bağlantı yaşam döngüsü ve R6-D3-B sınırını implementation öncesinde dondurmak.

## Gerçekleşen

Karar Execution Plan, `contracts/r6d3a-meta-connection-lifecycle-v1.json` ve contract testiyle repository düzeyinde işlendi. Runtime veya production değişikliği yapılmadı.

## Sapmalar

Yok.

## Evidence

- Legacy/canonical Meta salt-okunur envanter sonucu: legacy connected `1`, encrypted access `1`, expired expiry `1`, encrypted refresh `0`, active schedule `0`, open job `0`; canonical Meta `0`.
- `contracts/r6d3a-meta-connection-lifecycle-v1.json`
- `tests/r6d3a-meta-connection-lifecycle.test.js`

## Durum

`Done — contract only / R6-D3-B implementation gate`

## R6-D3-B kesin kapsamı

R6-D3-B yalnız şu işleri yapacaktır:

1. Meta authorization code sonrası server-side long-lived token exchange.
2. Canonical save öncesi token validity, configured app, required scope ve expiry doğrulaması.
3. Encrypted access token ve canonical expiry kaydı.
4. Callback sonrası `pending_account_selection` durumu.
5. Mevcut 1–3 provider-doğrulanmış hesap seçim sözleşmesinin korunması.
6. Yeniden yetkilendirme gerektiğinde kontrollü `Reconnect Meta` durumu.

R6-D3-B; veri çekme, Dataset V2 yazma, schedule/backfill, Disconnect/revoke veya production OAuth yapmayacaktır.
