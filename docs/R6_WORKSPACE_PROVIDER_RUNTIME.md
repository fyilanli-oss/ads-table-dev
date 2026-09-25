# R6 — Workspace provider runtime ve Dataset V2 aktivasyonu

## Analist özeti

R6'nın amacı Meta, Google Ads ve Klaviyo verisini artık eski kullanıcı kaydı üzerinden değil AdsTable workspace'i üzerinden işletmektir. Ticaret kanalı Shopify bugün workspace'i doğrulayan giriş kapısıdır; provider bağlantısının sahibi Shopify değil AdsTable workspace'idir.

R6 tek adımda production'a açılamaz. Gerçek provider kabulü için önce kullanıcının reporting currency seçmesi ve provider OAuth sonrasında canonical bağlantının oluşması gerekir. Bunlar R7 ekran akışında gerçekleşecektir. Bu nedenle R6 ile R7 arasındaki önceki dairesel bağımlılık alt kapılara ayrılmıştır.

## R6-A canlı preflight — 23 Eylül 2026

- Canonical workspace: `1`.
- Workspace settings ve seçilmiş reporting currency: `0`.
- Canonical provider connection: `0`; connected: `0`.
- Dataset V2: `0` satır; workspace-bound: `0`.
- Dataset V2 `workspace_id` foreign key ve workspace unique index: mevcut.
- Dataset V2 `user_id`: hâlâ zorunlu.
- Aktif legacy Meta/Google/Klaviyo schedule: `0`; açık job: `0`.
- Legacy write-freeze trigger: `6`.
- Eski embedded Klaviyo: `1 revoked`.
- Provider çağrısı, veritabanı yazısı veya migration uygulanmadı.

Sonuç `PASS_PREPARATION_ONLY`'dir. Şema ve çalışma sınırı hazırlığına başlanabilir; gerçek provider çalıştırılamaz.

## Revize uygulama sırası

1. **R6-A — Envanter ve contract:** Bu doküman, versionlı contract ve salt okunur SQL kapısı hazırlanır.
2. **R6-B — Ortak workspace runtime:** Server tarafından çözülen workspace, canonical connection ve reporting currency tek çalışma bağlamında birleştirilir. Provider çağrısı kapalı kalır. Kod sınırı hazırlanmış ve test edilmiştir; production composition root'a kaydedilmemiştir.
3. **R6-C — Aktivasyon migration hazırlığı:** Dataset V2 `user_id` alanının workspace yazarı için nullable yapılması hazırlanır ve boş tabloda dry verification yapılır. Canlı uygulama ayrıca onaylanır.
4. **R7-A — Currency ve Connect foundation:** Kullanıcı reporting currency seçer; Meta, Google Ads veya Klaviyo bağlantısı canonical tabloda hesap doğrulaması tamamlandıktan sonra `connected` olur.
5. **R6-D — Gerçek provider kabulü:** Her provider workspace→Time→FX→Dataset V2 hattında gerçek ve sentetik olmayan sonuçla doğrulanır. Klaviyo için non-empty sonuç veya provider'ın gerçekten boş döndüğünü kanıtlayan sonuç gerekir.
6. **R3-C — Tenant sıkılaştırması:** R6 writer kabulünden sonra `workspace_id NOT NULL` ve kalan workspace tenant enforcement tamamlanır.
7. **R7-B — Son kullanıcı durumu:** Connected ve warning modal kullanan Disconnect deneyimi tamamlanır.

Bu sıra R6'nın R7'yi, R7'nin de R6'yı sonsuza kadar beklemesini engeller. R7-A yalnız bağlantı ve currency foundation'ını açar; Dataset V2 yazarı R6-D kabulünden önce production primary olmaz.

## Ortak çalışma kuralı

Her provider çalışması şu sırayı izler:

1. Workspace server tarafında doğrulanır.
2. `workspace_provider_connections` içinde aynı workspace ve provider için `connected` kayıt çözülür.
3. Kullanıcının seçtiği `workspace_settings.reporting_currency` okunur.
4. Provider hesabının gerçek source currency ve timezone bilgisi doğrulanır.
5. Time ve FX normalization uygulanır.
6. Satır yalnız `workspace_id` tenant anahtarıyla Dataset V2'ye yazılır.

## R6-B repository sonucu

- `workspace-settings-store` yalnız `merchant_selected` reporting currency kabul eder.
- `workspace-provider-runtime` yalnız Meta, Google Ads ve Klaviyo'yu kabul eder.
- Çalışma sırası authority → canonical connected connection → reporting currency → provider runner → workspace Dataset V2 olarak sabitlenmiştir.
- Request içindeki `workspace_id`, `user_id` veya `shop_id` tenant iddiası reddedilir.
- Sonuç nesnesi access/refresh token dışarı vermez.
- Provider runner'lar henüz kaydedilmediği için production çalışması `PROVIDER_RUNTIME_NOT_READY` ile kapalıdır.
- Bu kod production route, cron veya job'a bağlanmadı; provider teması ve database write yapılmadı.

Shopify currency, provider currency veya sabit bir varsayılan reporting currency olamaz. Source ve target currency farklıysa FX verisi bulunmadan `1` oranı kullanılamaz.

## R6-C canlı kabul — PASS

- Resmî Supabase CLI `migration new` komutuyla hazırlanan migration, production ledger ile aynı sürümü taşıyan `20260923154503_r6_dataset_v2_workspace_activation.sql` olarak kanonikleştirildi.
- Migration yalnız Dataset V2 `user_id` alanını nullable yapar. Böylece workspace-first yazıcı, zorunlu bir legacy kullanıcı üretmeden çalışabilir.
- `workspace_id` bu aşamada nullable kalır; `NOT NULL` sıkılaştırması R6-D gerçek provider kabulünden sonra R3-C'de yapılır.
- Eski user-scoped unique index, query indexleri ve authenticated SELECT policy kaldırılmaz.
- Migration, `workspace_id` bulunmayan mevcut satır görürse fail-closed olur.
- Preflight mevcut sıfır satırlı baseline'ı, postcheck beklenen staged şemayı, rollback ise null `user_id` satırı oluşmadan geri dönüş şartını doğrular.
- Açık production onayı sonrasında preflight `PASS_PREPARATION_ONLY`, migration apply başarılı ve postcheck `PASS` oldu. Dataset V2 satır sayısı `0` kaldı.
- Security ve Performance Advisor sonucunda bu değişiklikten doğan yeni WARN/ERROR yoktur. Boş Dataset V2 workspace indexleri için görülen unused-index kayıtları beklenen INFO düzeyindedir.
- Provider runner hâlâ production'a kayıtlı değildir; OAuth, provider teması ve Dataset V2 yazımı kapalı kalır.
- R6-C tamamlandı; sıradaki kapı R7-A reporting currency ve canonical Connect foundation'dır.

## R7-A sonrası R6-D iş aynası

R7-A Klaviyo merchant acceptance canlıda tamamlandı. Kullanıcı bağlantıyı kontrollü olarak kaldırdıktan sonra R6-D2 hazırlığı için yeniden bağladı; canonical Klaviyo bağlantısı merchant tarafından `connected` olarak doğrulandı. Bu nedenle OAuth deneyiminin başarılı olması tek başına R6-D provider runtime kabulü veya Dataset V2 aktivasyonu sayılmaz.

R6-D aşağıdaki sabit sırayla yürütülür:

1. **R6-D1 — Ortak fail-closed kabul koşucusu:** Workspace authority, canonical connected connection, merchant-selected reporting currency, seçilmiş provider hesabı, Time/FX, sentetik olmayan canonical sonuç ve Dataset V2 yazısı tek kontrollü akışta birleştirilir. Bu paket yalnız repository hazırlığıdır; provider teması, production deployment veya Dataset V2 yazısı yapmaz.
2. **R6-D2 — Klaviyo canlı kabulü:** Kullanıcı Klaviyo'yu yeniden bağladıktan ve ayrı production onayı verdikten sonra tek verified account için gerçek provider sonucu alınır. Non-empty gerçek satırlar workspace-bound Dataset V2'ye yazılır. Provider gerçekten boş dönerse `empty_provider_result=true` kanıtlanır ve sahte satır yazılmaz.
3. **R6-D3 — Meta canlı kabulü:** Ayrı bağlantı ve onayla, kullanıcının seçtiği 1–3 verified reklam hesabının her biri workspace/hesap sahipliği, timezone, source currency, Time/FX ve gerçek Dataset V2 sonucu bakımından doğrulanır.
4. **R6-D4 — Google Ads canlı kabulü:** Ayrı bağlantı ve onayla, seçilen 1–3 verified customer için Standard ve PMax kapsamı, timezone, source currency, Time/FX ve gerçek Dataset V2 sonucu doğrulanır.
5. **R6-D5 — Kontrollü aktivasyon kararı:** Her provider yalnız kendi canlı kabulü PASS olduktan sonra açılabilir. Bir provider'ın PASS sonucu diğerini açmaz. Üç provider sonucu Execution Plan'a işlendiğinde R6-D kapanır; ardından R3-C ve R7-B kapıları değerlendirilir.

Her canlı kabul öncesinde salt-okunur preflight zorunludur. Provider teması, canonical connection metadata yazısı, kalıcı Dataset V2 yazısı, production deployment ve runtime aktivasyonu repository hazırlığından ayrı açık onay ister.

### R6-D1 repository sonucu — PASS

Ortak runtime artık Dataset V2 yazısından önce şu kapıları uygular: canonical bağlantıda seçilmiş bütün hesapların kapsanması; provider/platform ve hesap eşleşmesi; provider source currency ile merchant reporting currency ayrımı; timezone ve business date varlığı; sentetik satır reddi; provider tarafından doğrulanmış `empty` veya `non_empty` sonucu. Sonuç yalnız redacted adet ve durum taşır, token/hesap kimliği/metrik taşımaz ve `production_activation=false` kalır. Provider runner production composition root'a kayıtlı değildir.

### R6-D2 Klaviyo repository hazırlığı — PASS / canlı kapı kapalı

- Klaviyo mapper workspace modunda `user_id` üretmez; doğrulanmış `workspace_id` kullanır.
- Canonical bağlantıdaki tek hesap ID/currency, Klaviyo Accounts API sonucuyla tekrar eşleştirilir; timezone provider hesabından alınır.
- Campaign ve Flow günlük raporları ile aynı ayın month-to-date gönderim toplamları ayrı alınır. Email plan maliyeti yalnız canonical connection kaydından kullanım payıyla dağıtılır.
- `text_message_spend` Klaviyo sözleşmesinde USD'dir. Account currency USD değilse bu değer başka currency gibi etiketlenmez; `unsupported/null` kalır.
- Conversion metric ID global environment değerinden alınmaz ve isim benzerliğiyle tahmin edilmez. Klaviyo Metrics API'nin bütün sayfaları okunur; yalnız provider'ın tam `Placed Order` adıyla döndürdüğü adaylar ve integration provenance kullanıcıya sunulur. Seçim workspace + Klaviyo account + connection version ile canonical connection kaydına bağlanır.
- OAuth scope'a `flows:read` eklenmiştir. Mevcut bağlantı disconnected olduğu için bu scope ancak sonraki açık reconnect grant'inde alınabilir.
- Empty kabul yalnız Accounts, Campaign reporting ve Flow reporting istekleri başarıyla döndükten ve iki günlük branch de boş olduktan sonra `verified_empty=true` olabilir; sahte satır üretilmez.
- Provider runner production job/cron'a bağlı değildir. C1 read-only kabul yüzeyi production'a deploy edilmiştir; provider discovery ve Dataset V2 yazısı yapılmamıştır.

### R6-D2-C1 metric binding corrective — migration ve application deployment PASS

PR #250 ile production'a alınan ilk Shopify-session-bound read-only preflight, provider çağrısından önce `KLAVIYO_PREFLIGHT_NOT_CONFIGURED` ile fail-closed durdu. Canlı PASS verilmedi. Kök neden production composition'ın global `KLAVIYO_PLACED_ORDER_METRIC_ID` beklemesiydi. Tek global ID çok-workspace ve gelecekteki commerce adapter'ları için canonical değildir.

C1 repository düzeltmesi şu sınırı kurar:

- `20260925072801_add_workspace_provider_conversion_metric.sql` yalnız nullable metric kimliği, adı, integration provenance ve doğrulama zamanını canonical connection'a ekler; mevcut satıra veri yazmaz.
- Read-only `R6D2C1_KLAVIYO_METRIC_BINDING_PREFLIGHT.sql` ve `POSTCHECK.sql` şema/RLS/grant/binding sıfır durumunu doğrular; rollback mevcut bir metric bağı varsa kendini engeller.
- Metric discovery mevcut canonical Klaviyo token'ı ile salt okunur çalışır, sayfalamayı provider origin'i dışına çıkarmaz ve sıfır/çoklu adayda kendiliğinden seçim yapmaz.
- Açık metric seçimi provider'da yeniden doğrulanır; optimistic `connection_version` ve aynı `active_account_id` sağlanmadan yazılamaz.
- Disconnect, reconnect veya yeniden account seçimi metric bağını temizler.
- Read-only preflight yalnız canonical metric bağı mevcutsa Campaign/Flow raporlarına geçer; aksi halde `KLAVIYO_PREFLIGHT_METRIC_REQUIRED` ile durur.
- Normal Data Sources yüzeyi değişmez. Seçim yalnız `acceptance=r6d2-klaviyo` kabul yüzeyindedir.

Focused C1 ve Shopify regresyonu PASS'tir. Migration production'a uygulanmış ve PR #251 merge commit `b482ed7bd6506225ec9b95ca0cd9c5eef243ee50` production'da `READY` olmuştur. `dev.adstable.app` alias'ı aynı deployment'a bağlı, deploy sonrası runtime error taraması temizdir. Provider discovery, canonical metric yazısı ve Dataset V2 yazısı yapılmamıştır.

Production salt-okunur migration preflight 2026-09-25 tarihinde `PASS` verdi: canonical tablo mevcut, RLS ve FORCE RLS açık, browser grant `0`, hedef kolon/constraint `0/0`, canonical connection `1` ve connected Klaviyo `1`. Sorgu mutation veya provider teması yapmadı. Redacted kanıt `docs/security/evidence/R6D2C1_KLAVIYO_METRIC_BINDING_PREFLIGHT_LIVE.json` içindedir. Bu sonuç migration uygulama onayı değildir.

Production migration açık onayla `20260925072801_add_workspace_provider_conversion_metric` ledger sürümüyle uygulandı. Zorunlu postcheck `PASS`: hedef kolon `5`, validated constraint `1`, RLS/FORCE RLS açık, browser grant `0`, bound metric `0`, invalid binding `0`. Migration mevcut connection verisini değiştirmedi; provider teması ve Dataset V2 yazısı yapılmadı. Advisor taraması C1'e ait yeni bir bulgu üretmedi; mevcut proje genelindeki uyarılar ayrı backlog kapsamındadır. Redacted kanıt `docs/security/evidence/R6D2C1_KLAVIYO_METRIC_BINDING_MIGRATION_LIVE.json` içindedir.

### R6-D2-C2 Klaviyo satış kaynağı ürün sözleşmesi — PASS / C3 approval gate

- Merchant teknik metric ID görmez; ürün dili **Klaviyo sales source** ve güvenli integration/store provenance kullanır.
- Tek doğrulanmış `Placed Order` adayı varsa ayrı seçim ekranı açılmaz. Email Monthly Plan Cost adımının final onayında salt-okunur satış kaynağı özeti gösterilir ve aynı `Save and connect` eylemi kullanılır.
- Birden fazla doğrulanmış aday varsa aynı Shopify-native setup modalında yalnız güvenli integration/store adıyla tek seçim yapılır. Sıfır adayda bağlantı `Connected` olmaz ve sistem kaynak tahmin etmez.
- Seçim normal akışta connection binding başına bir kez istenir. Yalnız reconnect, Klaviyo account değişimi, provider bağının geçersizleşmesi veya Settings'teki açık değişiklik yeniden seçim gerektirir. Aylık plan maliyeti değişikliği satış kaynağını sıfırlamaz.
- C2 yalnız ürün/UX sözleşmesidir. Provider çağrısı, database write, Dataset V2 write, yeni adapter, mapping veya formula yoktur. Meta ve Google davranışı değişmez; TikTok ve Pinterest parked kalır.

Sıradaki kapı **R6-D2-C3 salt-okunur satış kaynağı keşfi** için analist brief, review ve ayrı açık onaydır.

Resmî sözleşme referansları: Klaviyo [Get Accounts](https://developers.klaviyo.com/en/reference/get_accounts), [Reporting API overview](https://developers.klaviyo.com/en/reference/reporting_api_overview), [Get Campaigns](https://developers.klaviyo.com/en/reference/get_campaigns) ve [Get Flows](https://developers.klaviyo.com/en/reference/get_flows).

### R6-D2-C6-D connected token lifecycle corrective — live PASS / verified empty / rows `0`

C6-C ikinci kontrollü deneme `KLAVIYO_DATASET_ACCEPTANCE_FAILED_PROVIDER_ACCOUNT` ile Account API aşamasında durdu; Dataset V2 satırı `0` kaldı. Production canonical Klaviyo kaydı encrypted refresh token ve `accounts:read` scope taşırken access-token expiry alanı boştu. Connected runtime kısa ömürlü access token için refresh uygulamıyordu.

C6-D, OAuth grant'ini kalıcı ürün bağlantısı olarak yönetir: `expires_in` canonical expiry alanına yazılır; expiry bilinmiyor/yaklaşıyorsa token provider'da yenilenir; dönen en güncel access/refresh token encrypted envelope olarak aynı workspace connection'a optimistic version ile kaydedilir. Beklenmeyen `401` yalnız bir refresh ve bir tekrar üretir. `invalid_grant` veya ikinci auth hatası yeniden yetkilendirme ister. Paralel istekler aynı process'te tek refresh paylaşır. Metric seçimi, mapping, Dataset writer ve E4/E5/E7 kapsamı değişmez.

Repository ilgili kapsam regresyonu `120/120 PASS` verdi. Production deployment, gerçek token yenilemesi, provider teması veya Dataset V2 yeniden denemesi yapılmadı; bunlar ayrı kabul kapılarıdır.

PR #259 merge commit `9039180442768a2dab9b80c09d24217d8249588f` post-merge Security/Full Regression PASS ve otomatik Vercel Production deployment SUCCESS ile canlıya alındı. Açık production onayıyla salt-okunur preflight tek kez çalıştı; Account, Campaign, Flow, Time ve FX kapıları PASS verdi. Ardından tek kontrollü C6 isteği `attempted 0`, `persisted 0`, `verified empty true` sonucu verdi. Provider doğrulanmış boş sonuç döndürdüğü için sentetik satır üretilmedi.

Supabase salt-okunur postcheck Dataset V2 toplam/Klaviyo satırını `0/0`; connected canonical Klaviyo, access-token expiry, encrypted refresh token ve metric binding sayılarını `1/1/1/1` doğruladı. R6-D2 Klaviyo canlı kabulü PASS ile kapanmıştır. Bu sonuç Meta veya Google Ads'i aktive etmez; sıradaki ayrı kapı R6-D3 Meta analist brief'i ve açık production onayıdır. Redacted kanıt `docs/security/evidence/R6D2C6D_KLAVIYO_LIVE_ACCEPTANCE_2026-09-25.json` içindedir.

### R6-D3-A Meta bağlantı ve token yaşam döngüsü — contract PASS / R6-D3-B implementation gate

Canlı salt-okunur envanter, legacy Meta bağlantısının durum olarak `connected` ve encrypted access token sahibi olmasına karşın expiry'sinin geçmiş olduğunu, refresh token taşımadığını ve active schedule/open job sayısının `0/0` kaldığını gösterdi. Canonical workspace Meta bağlantısı `0`dır. Bu nedenle legacy kayıt tarihsel kanıt olarak korunur; silinmez, canonical workspace'e taşınmaz ve access token'ı yeniden kullanılmaz.

Yeni Meta bağlantısı temiz Shopify embedded OAuth ile başlar. Authorization code server-side token'a çevrildikten sonra supported long-lived access-token exchange uygulanır; token validity, configured Meta app, required scope ve gelecekteki expiry canonical save öncesinde doğrulanır. Meta'nın mevcut OAuth sözleşmesinde bulunmayan bir refresh token akışı uydurulmaz. Yenileme desteklenmiyor veya doğrulama geçmiyorsa kullanıcı kontrollü `Reconnect Meta` durumuna alınır.

Callback yalnız `pending_account_selection` üretir. Bağlantı, server Meta'dan hesap listesini yeniden aldıktan ve kullanıcı en az `1`, en fazla `3` doğrulanmış reklam hesabını seçtikten sonra `Connected` olur. R6-D3-A yalnız plan ve sözleşme kararıdır; provider teması, production OAuth, Dataset V2 yazısı, schedule/backfill, Disconnect/revoke ve tamamlanmış E4 veri motorunun yeniden geliştirilmesi yoktur. Versionlı karar `contracts/r6d3a-meta-connection-lifecycle-v1.json`, analist belgesi `docs/R6D3A_META_CONNECTION_LIFECYCLE_DECISION.md` içindedir.

### R6-D3-B Meta token doğrulama — repository PASS / R6-D3-C account-selection acceptance gate

Embedded Meta callback'te server-side short-lived → long-lived user token exchange ve token debugger doğrulaması uygulanmıştır. `is_valid`, configured app ID, `ads_read` ve gelecekteki expiry koşullarından biri geçmezse canonical connection store çağrılmaz. Meta için refresh token beklenmez; yalnız doğrulanmış long-lived access token, debugger kaynaklı scope ve expiry mevcut encrypted store sınırına gönderilir.

Validation kaynaklı güvenli hatalar Meta'ya bağlı `Reconnect Meta` durumu üretir; token, app secret ve provider response kullanıcıya veya loga çıkmaz. Başarılı callback `pending_account_selection` olarak kalır ve 1–3 provider-doğrulanmış hesap seçilmeden `Connected` olmaz. R6-D3-B production'a deploy edilmemiş, provider'a çağrı yapmamış ve Dataset V2/schedule/backfill açmamıştır. Versionlı sonuç `contracts/r6d3b-meta-token-validation-v1.json`, analist kaydı `docs/R6D3B_META_TOKEN_VALIDATION.md` içindedir.

### R6-D3-C Meta hesap seçimi — production merchant acceptance PASS / data runtime gate

R7-A2'de tamamlanan canonical hesap seçimi runtime'ı yeniden incelendi ve R6-D3-C için yeniden geliştirilmesine gerek olmadığı doğrulandı. Eski `server.js + dashboard.html` akışının aksine browser tam hesap nesnesi veya tenant bilgisi sağlayamaz; yalnız seçilen ID'leri gönderir. Server save sırasında Meta `/me/adaccounts` listesini yeniden alır ve yalnız provider-doğrulanmış 1–3 hesabı optimistic connection version sınırıyla canonical kayda yazar.

Shopify-native `s-choice-list` çoklu seçim ve `s-modal` programatik açma kullanımı güncel resmi Shopify App Home sözleşmesiyle doğrulandı. Supabase `selected_accounts` alanı pending durumda boş, connected Meta durumunda 1–3 hesap olacak şekilde validated constraint ile korunmaktadır. Paket-spesifik uçtan uca repository testleri session authority, provider re-fetch, browser alanlarının reddi, 1–3 sınırı, duplicate/yabancı hesap reddi ve `Connected` geçişini doğrular.

Repository hazırlığı PR #261 ile production'a dağıtılmıştır. Gerçek Shopify merchant oturumunda temiz Meta OAuth tamamlanmış, hesap seçim modalı açılmış, bir provider-doğrulanmış hesap kaydedilmiş ve reload sonrasında `Connected · 1 account` korunmuştur. Salt-okunur postcheck canonical Meta kaydını geçerli ve browser grant sayısını `0` olarak doğrulamıştır; legacy Meta kaydı `1 → 1`, Dataset V2/schedule/job sayıları `0` kalmıştır. Versionlı sonuç `contracts/r6d3c-meta-account-selection-acceptance-v1.json`, analist kaydı `docs/R6D3C_META_ACCOUNT_SELECTION_ACCEPTANCE.md`, redacted aggregate kanıt `docs/security/evidence/R6D3C_META_ACCOUNT_SELECTION_LIVE_ACCEPTANCE_2026-09-25.json` içindedir.

R6-D3-C production kabulü yalnız bağlantı ve hesap seçimi kapsamıyla verilmiştir. Meta Insights/performance okuma, Time/FX ve kontrollü Dataset V2 sonucu ayrıca analiz edilip kanıtlanmadan R6-D3 bütünü `Done` sayılmaz; production veri hareketi halen kapalıdır.

### R6-D3-D Meta workspace salt-okunur preflight — production read-only acceptance PASS

Tamamlanmış E4 Meta Client, Campaign→AdSet→Ad mapper, sabit metrik sözleşmesi ve Time/FX katmanı korunmuştur. Yeni ince workspace runner yalnız Shopify session'dan çözülen authority, canonical `connected` Meta bağlantısı ve merchant-selected reporting currency ile bu katmanları compose eder. Seçili 1–3 hesabın tamamı Meta Account API'den yeniden doğrulanır; her hesap kendi currency ve timezone bilgisiyle önceki kapanmış business date için günlük Ad Insights okur.

Sonuç yalnız aggregate hesap/satır sayısı, verified empty/non-empty, Time/FX ve yazma durumunu dışarı verir. Token, hesap/entity kimliği ve ham metrik response'a çıkmaz. Dataset V2/V1 yazısı, schedule, backfill ve production activation yapılmaz. Normal Data Sources görünümü değişmez; salt-okunur kabul yüzeyi yalnız `?acceptance=r6d3-meta` operatör parametresiyle açılır.

PR #263 production'a dağıtıldıktan sonra gerçek merchant oturumundaki kabul `PASS — 1 account(s), 0 verified row(s), Time and FX checks succeeded. Dataset V2 writes: 0.` verdi. Bu, provider tarafından doğrulanmış boş sonuçtur; hata veya sentetik satır değildir. Supabase postcheck canonical Meta bağlantısını `connected · 1 account`, pending/invalid `0`; Dataset V2 Meta/schedule/job ve browser grant sayılarını `0` olarak doğruladı. Versionlı sonuç `contracts/r6d3d-meta-read-only-preflight-v1.json`, analist kaydı `docs/R6D3D_META_READ_ONLY_PREFLIGHT.md`, redacted kanıt `docs/security/evidence/R6D3D_META_READ_ONLY_LIVE_ACCEPTANCE_2026-09-25.json` içindedir.

## Fail-closed kurallar

- Currency yoksa provider çalışmaz.
- Canonical `connected` kayıt yoksa provider çalışmaz.
- Hesap kimliği canonical kayıtla eşleşmiyorsa provider çalışmaz.
- Başka workspace'e ait satır yazılamaz veya okunamaz.
- V2 hatası V1'e sessiz dönüş yapmaz.
- TikTok ve Pinterest production runtime'a kaydedilmez.
- Browser service-role veya workspace kimliği sağlayamaz.

## Bu pakette yapılmayanlar

- Production migration uygulanmadı.
- Provider API çağrısı yapılmadı.
- OAuth veya Connect açılmadı.
- Dataset V2'ye veri yazılmadı.
- Meta, Google Ads veya Klaviyo primary runtime yapılmadı.
- TikTok/Pinterest park durumu değiştirilmedi.
