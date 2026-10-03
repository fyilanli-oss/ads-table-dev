# A3 — Shopify Lifecycle, Privacy ve Data Deletion Auditi

**Tarih:** 3 Ekim 2026  
**Kapsam:** E10-T4, E10-T4-B, E10 install/uninstall/reinstall lifecycle ve R7 içindeki Delete My Data bağı  
**Yöntem:** Salt-okunur; Execution Plan → executable contract → route/runtime → canlı Supabase şeması → güncel resmî Shopify/Supabase kaynakları  
**Yetki sınırı:** Webhook kaydı, Shopify app deploy, migration, veri/token silme, provider revoke veya production değişikliği yapılmadı.

## Analist özeti

Karar katmanı doğru yöndedir:

- Uninstall ile Delete My Data aynı olay sayılmıyor.
- app/uninstalled erişimi hemen durdurmalı, fakat veri silindi dememeli.
- shop/redact yaklaşık 48 saat sonra workspace verisini onaylı manifestle silmeli.
- Merchant uninstall olmadan iki aşamalı Delete My Data isteyebilmeli.
- 48 saat içindeki reinstall duplicate workspace üretmemeli.
- Terminal silme sonrasındaki clean reinstall yeni workspace generation üretmeli.

Fakat bu kararların hiçbiri canlı operasyon zincirine dönüşmemiştir. Canonical app configuration içinde zorunlu compliance webhook subscription'ları yoktur; HTTP webhook/HMAC/durable claim hattı yoktur; canlı DB'de webhook claim, deletion request, deletion manifest execution veya redacted deletion evidence tabloları bulunmamaktadır.

Daha kritik olarak legacy kullanıcı yüzeyinde “Delete My Data” eylemi görünür durumdadır. Bu akış yalnız subscriptions satırını deleted yapıp hard_delete_at tarihini 90 gün sonrasına yazar. Repository'de bu tarihte verileri gerçekten silecek worker bulunmamıştır. Workspace, provider credential, Dataset, snapshot, billing ve kullanıcı verileri bu eylemle silinmez.

A3 sonucu: **Karar sözleşmesi güçlüdür; gerçek privacy lifecycle unsupported durumdadır ve public/review öncesi P0 blokerdir.**

## Resmî Shopify gerçeği

Güncel Shopify dokümantasyonuna göre App Store'da dağıtılan uygulamalar şu compliance topic'lere subscribe olmalıdır:

- customers/data_request
- customers/redact
- shop/redact

Shopify, shop/redact olayını uninstall'dan yaklaşık 48 saat sonra gönderir. Uygulama bu süre içinde yeniden kurulursa shop/redact gönderilmez. Bu davranış AdsTable E10-T4-B kararındaki iki reinstall yolunu doğrular.

Resmî kaynaklar:

- [Shopify — Privacy law compliance](https://shopify.dev/docs/apps/build/compliance/privacy-law-compliance)
- [Shopify — Webhook topics](https://shopify.dev/docs/api/webhooks/latest)

## Repository ve canlı şema gerçeği

### App configuration

Canonical shopify.app.toml yalnız app URL, embedded ayarı, boş scope listesi ve redirect URL taşımaktadır. Webhooks bölümü, compliance_topics ve app/uninstalled subscription'ı yoktur.

### Runtime

src/shopify/privacy-lifecycle.js yalnız bir planner/executor iskeletidir. Gerçek webhook route'u, HMAC doğrulama, durable event store ve production operation portları composition'a bağlı değildir.

Planner ile E10-T4-B contract arasında ayrıca iki fark vardır:

- Contract replay key'i shop_id + topic + webhook_event_id iken executor claim yalnız shop_id + event_id kullanır.
- shop_redact planner eylemi “delete_shop_personal_data” ile sınırlıdır; versionlı workspace deletion manifestindeki Dataset V2, provider credentials, schedules/jobs, settings, billing/entitlement ve binding kategorilerini yürütmez.

### Canlı Supabase

Canlı şema şu lifecycle işaretlerini taşır:

- workspaces status: active, suspended, archived
- shopify_installations status: active, revoked, uninstalled, redacted
- shopify_installations install_generation alanı vardır

Ancak bulunmayan operasyon nesneleri:

- durable webhook claim/event tablosu
- workspace deletion request tablosu
- deletion manifest run/step tablosu
- redacted deletion evidence tablosu
- terminal deletion ve clean reinstall generation authority'si
- workspace-scoped canonical SnapshotJob shutdown kaydı

Canlıda bir active workspace ve generation=1 olan bir active Shopify installation vardır. Bu yalnız mevcut development state kanıtıdır; lifecycle kabulü değildir.

Workspace ilişkilerinde hem CASCADE hem RESTRICT foreign key'ler vardır. Özellikle Dataset V2, canonical provider connection ve bazı binding'ler workspace silinmesini RESTRICT eder. Bu güvenli bir varsayılan olabilir, fakat exact tablo sıralı manifest olmadan silme işlemi yarıda kalır.

### Managed reinstall davranışı

complete_shopify_managed_install RPC'si aynı shop_id conflict'inde mevcut satırı active yapar, token envelope'larını değiştirir ve aynı workspace_id'yi korur. install_generation artırılmaz.

Bu davranış 48 saat içinde, deletion başlamamış reinstall için kullanılabilir. Fakat terminal silme sonrasında eski installation satırı kalmışsa aynı workspace'i yeniden canlandırır. Contract'ın “new workspace generation, old Dataset/token/binding restore yok” şartı ayrı terminal deletion/reinstall authority olmadan kanıtlanamaz.

## Legacy Delete My Data gerçeği

public/dashboard.html kullanıcıya “Delete My Data” ve “Hard delete is scheduled after 90 days” metnini gösterir.

POST /api/account/request-delete:

- subscriptions satırına kısa ömürlü confirmation token yazar;
- tokenı ve confirmation URL'ini browser response'unda döndürür.

GET /api/account/confirm-delete:

- subscription status değerini deleted yapar;
- deleted_at ve hard_delete_at yazar;
- kullanıcıyı login sayfasına yönlendirir.

Bulunmayan davranışlar:

- workspace access ve provider operations için workspace-scoped durdurma;
- Shopify/provider credential silme veya güvenli revoke;
- Dataset V2, snapshots, settings, billing ve binding manifesti;
- durable async deletion run;
- 90 gün sonunda hard delete worker;
- redacted completion evidence;
- Supabase Auth session/JWT kapatma.

Supabase'in güncel dokümantasyonuna göre auth user silinse bile önceden verilmiş JWT, exp süresine kadar geçerli kalabilir. Bu nedenle subscription status kontrolü önemli olsa da tek başına privacy deletion değildir.

Resmî kaynak: [Supabase — User Management](https://supabase.com/docs/guides/auth/managing-user-data)

## Bulgular

### AF-A3-001 — P0 — Zorunlu compliance webhook ve app/uninstalled hattı yok

Failure: Shopify compliance talepleri uygulamaya ulaşamaz; uninstall erişimi durdurmaz; App Store review veya canlı privacy yükümlülüğü başarısız olur.

Kanıt: shopify.app.toml içinde webhook subscription yok; repository'de production webhook route/HMAC/persistent claim yok.

Sınır: Audit app config deploy etmez. Remediation exact config, HMAC, durable claim, retry ve development-store acceptance içermelidir.

### AF-A3-002 — P0 — Görünür Delete My Data gerçek veri silme işlemi değildir

Failure: Kullanıcı verilerinin silindiğini düşünürken yalnız hesabın subscription erişimi kapanır. Veri ve credential'lar kalır.

Kanıt: UI “Delete My Data” ve 90 günlük hard delete sözü verir; route yalnız subscriptions satırını günceller; hard-delete worker bulunmadı.

Bilinmeyen: Legacy dashboard'ın production kullanıcılarına güncel erişilebilirliği bu audit içinde bağımsız doğrulanmadı. Bu bilinmeyen P0'ı kapatmaz; route ve görünür yüzey repository'de aktiftir.

### AF-A3-003 — P0 — Uninstall sonrası immediate access-stop operasyonu yok

Failure: Merchant uninstall ettikten sonra provider tokenları ve arka plan işleri yeni çağrı üretmeye devam edebilir.

Kanıt: planner eylemleri mock port düzeyinde; webhook registration/route/event store/operation composition yok. Live canonical hourly job zaten A2'de eksik, fakat provider operation authority ayrıca kapanmıyor.

### AF-A3-004 — P0 — Terminal deletion sonrası clean reinstall authority'si kanıtlanamaz

Failure: Silinmiş workspace generation yeniden canlanabilir veya yeni/eskisi aynı shop altında örtüşebilir; eski Dataset/token/binding'e yeniden erişim doğabilir.

Kanıt: managed install upsert aynı shop_id için aynı workspace_id'yi active yapıyor ve install_generation artırmıyor. Terminal deletion state/run/evidence yok.

### AF-A3-005 — P1 — Exact versionlı deletion manifesti ve dependency order yok

Failure: RESTRICT foreign key'ler nedeniyle silme yarıda kalabilir; bazı tablolar silinip bazıları kalabilir.

Kanıt: Contract yalnız kategori listesi taşır. Canlı authority envanterinde workspace, shop, user ve provider account kimliği taşıyan çok sayıda canonical ve legacy tablo vardır; table-by-table delete/anonymize/retain sınıflaması yoktur.

### AF-A3-006 — P1 — Planner, frozen contract'ı tam uygulamıyor

Failure: Aynı event ID farklı topic'lerde yanlış replay sayılabilir; shop_redact yalnız personal data operation adıyla sınırlı kalır; tam workspace manifesti çalışmaz.

Kanıt: Contract replay key shop_id + topic + webhook_event_id; executor claim shop_id + event_id. Eylem setleri contract kategorileriyle eşleşmiyor.

## Paket durum gerçeği

| Paket | A3 sınıflaması | Gerekçe |
|---|---|---|
| E10-T4 foundation | partially_verified | verified-event planner, ordered mock executor ve negatif testler var |
| E10-T4 operational lifecycle | unsupported | webhook registration, route, HMAC, persistence ve operations yok |
| E10-T4-B | verified_as_decision_only | karar doğru; contract açıkça implementation pending |
| R7 Delete My Data bağı | unsupported | Shopify Settings ürünü ve workspace deletion operation yok |
| Install | partially_verified | active binding/token persistence canlı; deletion-aware generation yok |
| Uninstall | unsupported | immediate stop zinciri yok |
| Reinstall <48h | partially_verified_by_shape_only | same shop/workspace reuse mümkün; deletion-not-started guard yok |
| Clean reinstall after deletion | unsupported | new generation ve old authority non-restore kanıtı yok |

## Güvenli remediation bağımlılık sırası önerisi

Bu audit implementation izni vermez. A6'da deduplicate edilmek üzere gerekli sıra:

1. Exact data inventory ve delete/anonymize/legal-retain manifesti.
2. Durable lifecycle event/deletion request/run/evidence şeması.
3. HMAC-verified compliance ve app/uninstalled route'ları.
4. İlk durable claim sonrası hızlı 2xx; retry-safe asynchronous worker.
5. Immediate workspace/provider/job access-stop.
6. Manifest executor, partial-failure resume ve completion evidence.
7. Reinstall-before-48h guard ile terminal clean-reinstall generation ayrımı.
8. Shopify Settings iki aşamalı Delete My Data yüzeyi.
9. Legacy “Delete My Data” sözünün kaldırılması veya gerçek manifest hattına bağlanması.
10. Development Store webhook/replay/uninstall/reinstall acceptance ve ayrı production onayı.

## Sonuç

E10-T4-B kararının implementation pending tutulması doğrudur; sorun belge değildir. Sorun, repository'de kullanıcıya silme sözü veren eski akış bulunurken zorunlu Shopify privacy lifecycle'ın henüz operasyonel olmamasıdır.

A3 bunu dört P0 ve iki P1 bulgu olarak kayda almıştır. Hiçbir webhook, veri, token, DB veya deployment değiştirilmemiştir.
