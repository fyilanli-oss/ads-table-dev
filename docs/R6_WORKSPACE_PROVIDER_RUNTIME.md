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

## R6-C repository hazırlığı

- Resmî Supabase CLI `migration new` komutuyla `20260923153220_r6_dataset_v2_workspace_activation.sql` oluşturuldu.
- Migration yalnız Dataset V2 `user_id` alanını nullable yapar. Böylece workspace-first yazıcı, zorunlu bir legacy kullanıcı üretmeden çalışabilir.
- `workspace_id` bu aşamada nullable kalır; `NOT NULL` sıkılaştırması R6-D gerçek provider kabulünden sonra R3-C'de yapılır.
- Eski user-scoped unique index, query indexleri ve authenticated SELECT policy kaldırılmaz.
- Migration, `workspace_id` bulunmayan mevcut satır görürse fail-closed olur.
- Preflight mevcut sıfır satırlı baseline'ı, postcheck beklenen staged şemayı, rollback ise null `user_id` satırı oluşmadan geri dönüş şartını doğrular.
- Bu hazırlık production migration onayı değildir. Canlı uygulama, postcheck ve advisor kontrolü için ayrıca açık onay gerekir.
- Provider runner hâlâ production'a kayıtlı değildir; OAuth, provider teması ve Dataset V2 yazımı kapalı kalır.

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

