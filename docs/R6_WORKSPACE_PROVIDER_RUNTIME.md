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
- Kod production route/job/cron'a bağlı değildir. Deployment, provider çağrısı ve Dataset V2 yazısı yapılmamıştır.

### R6-D2-C1 metric binding corrective — repository hazır / production kapalı

PR #250 ile production'a alınan ilk Shopify-session-bound read-only preflight, provider çağrısından önce `KLAVIYO_PREFLIGHT_NOT_CONFIGURED` ile fail-closed durdu. Canlı PASS verilmedi. Kök neden production composition'ın global `KLAVIYO_PLACED_ORDER_METRIC_ID` beklemesiydi. Tek global ID çok-workspace ve gelecekteki commerce adapter'ları için canonical değildir.

C1 repository düzeltmesi şu sınırı kurar:

- `20260925072801_add_workspace_provider_conversion_metric.sql` yalnız nullable metric kimliği, adı, integration provenance ve doğrulama zamanını canonical connection'a ekler; mevcut satıra veri yazmaz.
- Read-only `R6D2C1_KLAVIYO_METRIC_BINDING_PREFLIGHT.sql` ve `POSTCHECK.sql` şema/RLS/grant/binding sıfır durumunu doğrular; rollback mevcut bir metric bağı varsa kendini engeller.
- Metric discovery mevcut canonical Klaviyo token'ı ile salt okunur çalışır, sayfalamayı provider origin'i dışına çıkarmaz ve sıfır/çoklu adayda kendiliğinden seçim yapmaz.
- Açık metric seçimi provider'da yeniden doğrulanır; optimistic `connection_version` ve aynı `active_account_id` sağlanmadan yazılamaz.
- Disconnect, reconnect veya yeniden account seçimi metric bağını temizler.
- Read-only preflight yalnız canonical metric bağı mevcutsa Campaign/Flow raporlarına geçer; aksi halde `KLAVIYO_PREFLIGHT_METRIC_REQUIRED` ile durur.
- Normal Data Sources yüzeyi değişmez. Seçim yalnız `acceptance=r6d2-klaviyo` kabul yüzeyindedir.

Focused C1 + Shopify regresyonu 101/101 PASS'tir. Tam yerel suite 1049 testin 1002'sini geçirmiş, 46 tarihsel checksum/Windows spawn-CRLF testi başarısız olmuş ve 1 test skip kalmıştır; bu nedenle full regression PASS iddiası yoktur. Migration uygulanmadı, C1 deploy edilmedi, yeni provider çağrısı yapılmadı ve Dataset V2 yazısı yapılmadı.

Production salt-okunur migration preflight 2026-09-25 tarihinde `PASS` verdi: canonical tablo mevcut, RLS ve FORCE RLS açık, browser grant `0`, hedef kolon/constraint `0/0`, canonical connection `1` ve connected Klaviyo `1`. Sorgu mutation veya provider teması yapmadı. Redacted kanıt `docs/security/evidence/R6D2C1_KLAVIYO_METRIC_BINDING_PREFLIGHT_LIVE.json` içindedir. Bu sonuç migration uygulama onayı değildir.

Production migration açık onayla `20260925072801_add_workspace_provider_conversion_metric` ledger sürümüyle uygulandı. Zorunlu postcheck `PASS`: hedef kolon `5`, validated constraint `1`, RLS/FORCE RLS açık, browser grant `0`, bound metric `0`, invalid binding `0`. Migration mevcut connection verisini değiştirmedi; provider teması ve Dataset V2 yazısı yapılmadı. Advisor taraması C1'e ait yeni bir bulgu üretmedi; mevcut proje genelindeki uyarılar ayrı backlog kapsamındadır. Redacted kanıt `docs/security/evidence/R6D2C1_KLAVIYO_METRIC_BINDING_MIGRATION_LIVE.json` içindedir. Sıradaki kapı application deployment review ve ayrı açık onaydır.

Resmî sözleşme referansları: Klaviyo [Get Accounts](https://developers.klaviyo.com/en/reference/get_accounts), [Reporting API overview](https://developers.klaviyo.com/en/reference/reporting_api_overview), [Get Campaigns](https://developers.klaviyo.com/en/reference/get_campaigns) ve [Get Flows](https://developers.klaviyo.com/en/reference/get_flows).

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
