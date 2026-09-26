# AdsTable — V4 Execution Plan

**Sürüm:** V4.1 — Workspace-first execution amendment
**Tarih:** 17 Ağustos 2026; son karar revizyonu 20 Eylül 2026
**Belge türü:** Güncel uygulama, kabul ve takip planı  
**Durum:** Mutabık kalınan execution baseline  

**Kalıcı yürütme kaydı:** GitHub issue [#36 — AdsTable Execution Control](https://github.com/fyilanli-oss/ads-table-dev/issues/36). Bu belge kapsam, bağımlılık ve kabul kaynağıdır; PR/merge/CI ve insan onayı gerektiren production kapılarının güncel koordinasyon durumu kalıcı issue üzerinde tutulur.

## 0. Belge hiyerarşisi ve kullanım kuralı

Bu belge, aşağıdaki iki belgeyi değiştirmez:

1. **Final analiz raporu:** Tarihli mevcut durum ve karar baseline'ıdır; değiştirilmeden korunur.
2. **V3 Implementation Plan:** Canonical contract, platform semantiği, hierarchy, metric support, time, FX, formula, Dataset V2 ve Funnel API için teknik referanstır.

**V4 Execution Plan**, bu iki kaynağı uygulanabilir epic, task, kabul kapısı ve kanıt yapısına dönüştürür. Günlük ilerleme bu belge üzerinden takip edilir. Teknik contract çatışmasında V3; öncelik, güvenlik, geçiş ve production kabul sıralamasında Final Rapor; iş takibinde V4 esas alınır. Bir çatışma görülürse sessizce yorumlanmaz, karar kaydı açılır.

Repository ve CI yürütme günlüğü bu belgenin geçmişe dönük baseline niteliğini bozmayacak şekilde kalıcı Execution Control issue'sunda tutulur. Issue güncellemesi bu belgedeki kabul kriterlerinin yerine geçmez; task durumu yalnız repository, CI, canlı evidence ve gerekli insan kabulü birlikte sağlandığında ilerletilir.

### 0.1 Planlanan ve gerçekleşen ayrımı

- **Planlanan:** Başlangıçta onaylanan kapsam, bağımlılık, kabul, test ve rollback'tir.
- **Gerçekleşen:** Yalnız repository, canlı sistem ve evidence ile doğrulanmış sonuçtur.
- **Sapma:** Planlanandan farklı yapılan veya yapılmayan her şey, gerekçesi ve etkisiyle kaydedilir.
- Bir task kodu yazıldığı için değil, kabul kriterleri ve evidence tamamlandığı için `Done` olur.
- Plan geçmişe dönük olarak gerçekleşene uydurulmaz; değişiklikler decision log ile versionlanır.

### 0.2 Durum sözlüğü

| Durum | Anlamı |
|---|---|
| `Not started` | İşe başlanmadı. |
| `Ready` | Bağımlılıklar tamam, başlanabilir. |
| `In progress` | Uygulama devam ediyor. |
| `Blocked` | Belgelenmiş dış bağımlılık nedeniyle ilerleyemiyor. |
| `Verification` | Kod tamam; kabul, test ve evidence doğrulanıyor. |
| `Done` | Bütün kabul, test, rollback ve evidence koşulları sağlandı. |
| `Deferred` | Açık karar ve gerekçeyle ileri tarihe taşındı. |
| `Parked` | Mevcut artefaktlar korunur; yeni bağlantı, provider teması, runtime activation veya ürün yüzeyi ayrı açık karara kadar ilerlemez. |

## 1. Değiştirilemez execution prensipleri

1. Proje baştan yazılmayacaktır.
2. Çalışan V1 snapshot/dashboard hattı kontrollü geçiş boyunca korunacaktır.
3. Yeni canonical analytics kaynağı Dataset V2 olacaktır.
4. Business math'in sahibi backend Formula/Compare/Intent katmanlarıdır.
5. Funnel UI presentation-only olacaktır.
6. Güvenlik ve ownership kontrolleri provider ingest'ten önce tamamlanacaktır.
7. Provider geçişleri feature flag, dual-write, parity ve rollback ile yapılacaktır.
8. Backfill resumable, idempotent ve ölçülebilir olacaktır.
9. Legacy retirement en son yapılacaktır.
10. Production GO yalnız ölçülebilir kabul kapılarıyla verilecektir.

### 1.1 Monolit büyütmeme kuralı

V4 başladıktan sonra yeni Funnel, provider adapter, OAuth, analytics ve job business logic'i doğrudan kök `server.js` veya inline `public/dashboard.html` içine eklenemez.

### 1.2 Dokunurken çıkarma kuralı

Her epic dokunduğu eski davranışı hedef modüle taşır; fakat ilgisiz alanlarda big-bang refactor yapmaz. Taşıma characterization testleriyle başlar, delegation/feature flag ile devreye alınır ve parity kanıtlanmadan eski uygulama silinmez.

### 1.3 Zorunlu task aynası

Her task aşağıdaki alanları eksiksiz taşımalıdır:

```markdown
## [EPIC]-[TASK] İş başlığı

### Amaç
### Mevcut durum
### Planlanan durum
### Kapsam
### Kapsam dışı
### Bağımlılıklar
### Uygulama adımları
### Kabul kriterleri
### Test planı
### Rollback planı
### Gözlemlenebilirlik
### Güvenlik ve veri etkisi
### Planlanan
### Gerçekleşen
### Sapmalar
### Evidence
### Durum
```

Alanlardan biri uygulanabilir değilse silinmez; `Uygulanamaz — gerekçe` yazılır.


### 1.4 Analist diliyle koordinasyon kuralı

Her paket koordinasyon özetinde önce tek cümlelik **iş çıktısı** ve ölçülebilir **iş değeri** yazılır. Teknik araç, PR, hata kodu ve operasyon kapıları ana çıktı gibi sunulmaz; yalnız gerektiğinde teknik evidence olarak eklenir. İş çıktısı kullanıcı, gelir, operasyon, güvenlik veya süreklilik açısından anlamlı değilse paket ilerletilmez; kapsam/öncelik için iş kararı istenir. Bir paketin içindeki güvenlik adımları yeni paket veya alt task gibi raporlanmaz.

### 1.5 Onaylı workspace-first execution revizyonu — 20 Eylül 2026

Bu bölüm, Shopify Embedded kararından önceki standalone kullanıcı/OAuth modeli ile E10 sonrasında eklenen Shopify workspace modelinin birlikte incelenmesi üzerine verilmiş ileriye dönük execution kararıdır. Tarihsel task/evidence kayıtları silinmez; ancak aşağıdaki kararlarla çelişen eski “aktif provider”, `user_id` tenant authority, ikinci OAuth deposu veya Shopify currency varsayımları yeni iş için kullanılamaz. Çelişki sessizce yorumlanmaz; bu revizyon esas alınır ve ilgili executable contract/migration/runtime paketi kendi kabul kapısında güncellenir.

#### Değiştirilemez hedef mimari

- AdsTable canonical tenant anahtarı `workspace_id` olacaktır. `user_id`, `shopify_user_id`, `shop_id`, email veya provider account ID tenant authority değildir.
- Shopify ilk commerce installation adapter'ıdır; analytics, reporting currency ve provider bağlantıları Shopify tablosuna değil AdsTable workspace'ine aittir. Ortak çekirdek ileride WooCommerce gibi ikinci bir commerce adapter'ını yeni Dataset veya ikinci provider OAuth sistemi kurmadan kabul edebilmelidir.
- İlk aktif provider dilimi yalnız **Meta, Google Ads ve Klaviyo**'dur. **TikTok ve Pinterest `Parked`** durumundadır. TikTok için tamamlanmış adapter, güvenlik ve evidence artefaktları korunur; yeni Shopify Connect, OAuth/reconnect, refresh/shadow, provider teması ve production primary activation yapılmaz. Yeniden açılış ayrı açık kullanıcı kararı ve güncel resmi revalidation gerektirir.
- Workspace reporting currency kullanıcı tarafından ilk kurulumda seçilir, server-authoritative workspace ayarı olarak saklanır ve kaydedilmeden Data Sources açılmaz. Shopify store/presentment currency okunmaz veya default yapılmaz.
- Provider source currency ve workspace reporting currency ayrıdır. Örneğin Klaviyo plan maliyeti `USD`, workspace reporting currency `TRY` olabilir; FX canonical satırda gerçek rate/date/provider provenance ile uygulanır.
- Yeni bağlantıların tek authority ve token deposu workspace-scoped canonical provider connection store'dur. Standalone `platform_connections` / `platform_connection_tokens` yeni OAuth, reconnect veya refresh kabul etmez; yalnız migration/rollback süresince legacy kaynak olarak korunur.
- Dataset V2 yeni canonical analytics source-of-truth olmaya devam eder fakat tenant identity `user_id` yerine `workspace_id` olur. V1 ve snapshot verileri E9/E13/E14 kapıları tamamlanmadan silinmez veya körlemesine V2'ye kopyalanmaz.
- Canlıda aynı Klaviyo account'un standalone ve Shopify workspace modellerinde bulunması duplicate authority olarak kabul edilir. Konsolidasyon tamamlanana kadar yeni Klaviyo OAuth/reconnect ve iki hattan refresh yasaktır; provider revoke çağrısı yeni canonical grant'i de etkileyebileceği için ayrı doğrulama/onay olmadan yapılmaz.

#### Revize execution paketleri ve sıra kapısı

| Sıra | Revizyon paketi | Mevcut Epic bağı | Durum ve zorunlu çıktı |
|---|---|---|---|
| R0 | Geçici duplicate/OAuth güvenlik kapısı | E7 + E10-T6 | `Ready` — yeni duplicate OAuth/refresh/revoke yok; veri silinmez. |
| R1 | Workspace authority kararını contract'lara işleme | E10-T2 + E2/E3 | `Done` — canonical tenant `workspace_id`; versionlı contract, authority envanteri ve R2–R7 migration/release sırası donduruldu. R2 ayrı açık onay bekler. |
| R2 | Platformdan bağımsız workspace ve currency şeması | E10-T2 + E10-T5-C4/C6/C7 | `Done` — R2-A ledger uzlaştırması ve foundation migration ayrı açık onaylarla tamamlandı; canlı postcheck `PASS`, currency kullanıcı seçimine kadar boş. |
| R3 | Dataset V2 workspace tenant dönüşümü | E2 + E3 | `In progress / R3-A+B+C1 Done; first-write tenant enforcement live PASS` — Dataset V2 canlıda `0` satırdır. Kullanıcı kararıyla `workspace_id NOT NULL`, legacy user index/policy retirement ve browser-direct erişim kapısı ilk Dataset V2 satırından önceye alındı. Onaylı production migration `20260925103312` olarak uygulandı; postcheck `PASS`, Dataset V2 yine `0` satırdır, provider teması ve Dataset write yapılmadı. |
| R4 | Workspace provider connection birleşimi | E10-T6 | `Done / R4-A+B+C` — kanonik boş tablo canlıda; standalone OAuth/refresh/write hattı fail-closed donduruldu; tarihsel kayıtlar korundu. |
| R5 | Mevcut Klaviyo account konsolidasyonu | E7 + E10-T6 | `Done / controlled clean reset completed` — no-refresh doğrulama `409` ile fail-closed durduğu için eski grant kanoniğe taşınmadı. Shopify modalındaki işlem-anı onayıyla revoke tamamlandı; embedded satır `revoked`, canonical Klaviyo `0` kaldı ve tarihsel alanlar korundu. |
| R6 | Embedded provider runtime → canonical V2 | E4 + E5 + E7 | `In progress / R6-D2 Klaviyo live PASS; R6-D3 Meta full lifecycle live PASS; R6-D4-C Google Ads connection/account selection live PASS; R6-D4-D repository PASS / production acceptance pending` — Google Sheets ve GA4 parked kalır. Üç bağlı Google Ads hesabı için tamamlanmış E5 Standard/PMax ve Time/FX motorunu yeniden kullanan, Dataset V2 yazmayan salt-okunur kabul katmanı hazırlanmıştır; canlı provider sonucu ve Supabase son kontrolü henüz kanıtlanmamıştır. |
| R7 | Currency-first ve Shopify-native Connect/Disconnect UX | E10-T5 + E10-T6-C2I-V10 | `R7-A merchant acceptance PASS; provider-specific Disconnect acceptance moved into each R6-D provider gate; R7-B final consistency after R6-D` — Klaviyo için tam yaşam döngüsü canlıda doğrulandı. Meta/Google için Connect → Connected → Disconnect → temiz Reconnect zinciri ilgili provider'ın R6-D kabulü kapanmadan tamamlanır. R7-B bütün aktif provider'ların son ortak deneyim ve tutarlılık kapısıdır; eksik Disconnect uygulamalarının ilk teslim noktası değildir. |
| R8 | V1 tarihsel geçiş ve resumable backfill | E9 | `Blocked by R3/R6` — doğrulanmış legacy binding, re-fetch veya canonical validation; fake/synthetic/ambiguous satır yok. |
| R9 | Production read cutover | E13 | `Blocked by R6–R8` — provider bazlı canary, V2 read, SLO/parity/restore/rollback ve insan GO. |
| R10 | Standalone OAuth ve V1 legacy retirement | E14 | `Blocked by R9 stabilization` — consumer-zero, read-disable observation, ayrı retirement migration ve restore noktası. |
| R11 | WooCommerce readiness contract | E10 tenant model future extension | `Deferred` — WooCommerce implementation yok; ortak workspace/currency/connection/V2 çekirdeğinin Shopify'a kilitlenmediğini kanıtlayan contract. |

#### Paketler için ortak uygulama ve kabul kuralları

- **R0:** Eski ve embedded Klaviyo akışları aynı anda refresh üretemez. Hiçbir token/kayıt silinmez; Klaviyo `/oauth/revoke` çağrılmaz. Rollback, yalnız konfigürasyon kapısını eski güvenli duruma döndürür.
- **R1 — Done:** `user_id` kullanan canonical envelope, Dataset V2, repository, query, job, backfill, ownership ve RLS noktaları; `workspace_id` kullanan Shopify installation/OAuth noktalarıyla birlikte envanterlendi. `contracts/r1-workspace-authority-v1.json` versionlı authority kararıdır; `docs/R1_WORKSPACE_AUTHORITY_DECISION.md` exact R2–R7 migration/release sırasını ve kabul kapılarını kaydeder. Kod/DB/provider mutation yapılmadı. R2 ayrı açık insan onayı almadan başlamaz.
- **R2 — Done:** Additive `workspaces` ve `workspace_settings` migration'ı, versionlı currency contract'ı, salt okunur preflight/postcheck ve fail-closed rollback hazırlandı. 20 Eylül 2026 ilk canlı preflight'ta bulunan embedded migration ledger farkı, ayrı açık onayla R2-A kapsamında yalnız `20260911130000` ve `20260911150000` sürümleri işlenerek kapatıldı; E9/backfill sahte `applied` yapılmadı. İkinci ayrı açık onayla `20260920090105_create_workspace_currency_foundation` canlıya uygulandı. Postcheck `PASS`: bir doğrulanmış Shopify workspace'i canonical registry'ye seed edildi, `workspace_settings` boş bırakıldı, foreign key doğrulandı, RLS + force RLS açık, browser rolleri kapalı ve `service_role` yalnız explicit CRUD yetkili. OAuth/provider/Dataset V2 adetleri değişmedi. Shopify currency hiçbir alana kaynak olmadı. Advisor taramasında R2 kaynaklı yeni WARN/performance bulgusu yoktur; server-only tablolardaki policiesiz RLS bilgi kaydı beklenen deny-by-default modelidir. R3 ayrı kapsam ve onay kapısıyla başlar.
- **R3 — In progress / R3-A Done; R3-B runtime verification:** 23 Eylül 2026 canlı preflight Dataset V2'nin `0` satır içerdiğini, canonical workspace'in bulunduğunu, `workspace_id` kolonunun ve `backfill_checkpoints` tablosunun bulunmadığını doğruladı. Açık production onayıyla additive `20260923083734_add_dataset_v2_workspace_tenant` migration'ı uygulandı; nullable `workspace_id`, doğrulanmış workspace foreign key'i, canonical unique index ve üç query indexi oluşturuldu. Postcheck `PASS`; Dataset V2 yine `0` satır, workspace-bound satır `0`; eski `user_id`, unique index, authenticated SELECT policy ve grant'ler korundu. Canonical contract V2, workspace-scoped repository/Query Service/backfill sınırı ile negatif cross-workspace testleri hazırdır. `NOT NULL`, eski unique/index/policy retirement, backfill ve provider runtime cutover yapılmadı. `backfill_checkpoints` canlıda hiç oluşmadığı için eski user-scoped E9 migration karantinada kalır; fiziksel workspace checkpoint tablosu R8'de lease/control semantiğiyle oluşturulacaktır. Sonraki kapı R3-B runtime doğrulaması; R3-C final enforcement ayrı onay ister.
- **R3 — In progress / R3-A+B+C1 Done; first-write tenant enforcement live PASS:** R3-A/R3-B workspace authority ve R6-C nullable `user_id` sonucu korunur. C6-A ilk canlı write denemesi fail-closed `503` verdi ve Dataset V2 `0` kaldı. Kullanıcı kararıyla final tenant enforcement'ın güvenli alt paketi ilk satırdan önceye alındı: `workspace_id NOT NULL`, workspace FK/unique index korunması, dört legacy user indexinin ve eski authenticated user policy/grant'inin retirement'ı. `user_id` silinmez; nullable non-authoritative actor olarak kalır. Canlı tabloda bağımlı view/function/trigger/cron ve satır yoktur. Açık production onayıyla `20260925103312_r3c1_dataset_workspace_first_write_enforcement` uygulandı ve postcheck `PASS` verdi: Dataset V2 `0` satır kaldı, `workspace_id` zorunlu oldu, legacy user index/policy/grant kaldırıldı, workspace indexleri/FK ve service-role erişimi korundu. C6-B teşhis deployment'ından sonra ayrı açık onayla yapılan C6-C ikinci deneme `KLAVIYO_DATASET_ACCEPTANCE_FAILED_PROVIDER_ACCOUNT` ile fail-closed kaldı; Dataset V2 hâlâ `0` satırdır. R3-C1 sırasında provider teması veya Dataset write yapılmadı.
- **R4:** Canonical connection store `workspace_id + provider` başına tek state taşır; OAuth callback tek başına `Connected` değildir; active account server-side ownership doğrulaması ister. Token yalnız encrypted envelope'dur. Eski ve yeni OAuth runtime aynı anda authoritative olamaz.
- **R4-A+B — Tamamlandı / R4-C write-freeze approval gate:** 23 Eylül 2026 canlı preflight `PASS` sonrasında açık production onayıyla `20260923091731_create_workspace_provider_connections` uygulandı. `workspace_id + provider` keyed kanonik tablo `0` satırla oluşturuldu; primary key, doğrulanmış workspace foreign key'i, lifecycle indexi, constraint'ler, force RLS, browser deny ve explicit service-role CRUD postcheck'i `PASS` verdi. Mevcut sayılar değişmedi: `1` Shopify-scoped connected Klaviyo, `8` legacy connection, `7` encrypted legacy token, `0` plaintext token. Embedded ve legacy Klaviyo aynı anda bulunduğu için hiçbir kayıt/token otomatik kopyalanmadı. OAuth route, runtime, provider grant/revoke, legacy write davranışı ve deployment değiştirilmedi. Advisor kontrolünde R4-B kaynaklı yeni WARN yoktur; server-only tablodaki policiesiz RLS ve boş tablonun kullanılmamış lifecycle indexi bilgi düzeyinde beklenen sonuçtur. R4-C standalone write freeze, R5 konsolidasyonundan önce ayrı açık onay ister.
- **R4-C — Tamamlandı / R5 approval gate:** Açık onayla `20260923093756_r4c_freeze_legacy_provider_writes` uygulandı. Standalone Meta/Google/Klaviyo/TikTok/Pinterest OAuth transaction insert'leri ile legacy connection, encrypted token, ownership, schedule ve job write'ları database trigger'larıyla fail-closed donduruldu; Shopify embedded OAuth transaction'ları korunur. Canlı preflight, postcheck ve gerçek negatif yazma denemesi `PASS` verdi. Aktif `1` Google, `1` Klaviyo ve parked `1` TikTok schedule durduruldu. Dokuz gündür açık kalan `1` Google queued ve `1` TikTok running automation job silinmeden `failed` yapıldı; Klaviyo açık job sayısı zaten `0` idi. Legacy `8` connection ve `7` encrypted token değişmedi; plaintext token `0`, kanonik connection `0` kaldı. Uygulama katmanında standalone route, save ve refresh-job guard'ları ile cron provider filtresi hazırlandı; deployment yapılmadı. Provider revoke/decrypt/re-encrypt/veri taşıma yapılmadı. R5 ayrı açık onay ister.
- **R5:** Eşleşen Klaviyo account ID destekleyici kanıttır, tek başına tenant sahipliği değildir. Embedded token yalnız ayrı açık provider-temas onayıyla Account API'de doğrulanır. Başarılı doğrulama sonrasında eski connection local `migrated/disabled` olur; historical V1/snapshot korunur; rollback süresi bitmeden token envelope temizliği yapılmaz.
- **R5-A — Tamamlandı / R5-B verification gate:** Canlı redacted envanterde `2` legacy Klaviyo satırı, `1` Shopify embedded connected Klaviyo ve `0` canonical Klaviyo bulundu. Legacy satırlardan yalnız biri embedded account ID ile eşleşir; ikinci satırda seçilmiş account yoktur. OAuth kayıtlarında legacy `user_id` ile `workspace_id` birlikte bulunmadığı ve üyelik/binding tablosu olmadığı için otomatik tenant eşlemesi reddedildi. Açık R5 onayıyla additive `20260923132407_create_legacy_user_workspace_bindings` migration'ı uygulandı ve tablo bilinçli olarak `0` satır bırakıldı. Tek legacy user için birden fazla aktif workspace eşlemesi unique index ile engellenir; RLS + force RLS açık, browser rolleri kapalı, service role explicit CRUD yetkilidir. Provider teması, account taşıma, canonical insert, legacy disable, revoke veya token silme yapılmadı. R5-B, embedded token'ın Klaviyo Account API'de ayrı açık provider-temas onayıyla doğrulanmasını ve matching legacy aday için insan attestation kaydını bekler.
- **R5-A human binding + R5-B repository hazırlığı:** Kullanıcı, embedded account ID ile eşleşen eski AdsTable Klaviyo hesabının bu workspace'e ait olduğunu açıkça beyan etti. Yalnız bu legacy aday için `1` aktif `human_attested` binding yazıldı; seçilmiş hesabı olmayan ikinci legacy satır unbound kaldı. Mevcut accounts endpoint'inin `401` halinde token refresh yazısı yapabildiği görülerek salt-okunur onay kapsamında çalıştırılması reddedildi. Bunun yerine Shopify session-bound `GET /api/shopify/providers/klaviyo/accounts/verify` ve R5 operatör parametresi hazırlandı: tek Account API GET çağrısı, refresh/write yok, `401` halinde fail-closed, account/token ifşası yok. Testler `16/16` Klaviyo ve `3/3` R5 PASS. Kod henüz deploy edilmediği için provider teması ve R5-C taşıması yapılmadı.
- **R5-B canlı sonuç + R5-C kontrollü temiz reset kararı:** PR #231 merge commit `bab09110eb532e318d893c24535ce33953df63c2` production'a alındı ve `dev.adstable.app` alias'ı bu READY deployment'a bağlandı. Kullanıcının açık provider-temas onayıyla session-bound no-refresh doğrulama tam bir kez çalıştırıldı; `/api/shopify/providers/klaviyo/accounts/verify` `409` döndürdü. Supabase salt-okunur kontrolü embedded kaydın `connected`, aktif hesaplı, `USD` currency'li ve şifreli access/refresh token zarflı olduğunu doğruladı; refresh, retry, revoke, canonical insert veya token silme yapılmadı. Eski grant doğrulanamadığı için R5 v1 kanonik taşıma yolu kapatıldı ve sonuç `reauthorization_required` kabul edildi. Kullanıcı temiz başlangıç kararı verdi: R5-C, yalnız işlem anındaki ayrı açık onayla refresh token'ı bir kez revoke edecek; provider başarısından önce yerel durumu değiştirmeyecek; embedded satırı `revoked` yaparken hesap/maliyet/currency/token zarfları ve tarihsel verileri koruyacak; aynı işlemde OAuth başlatmayacaktır. Hazırlık sözleşmesi `contracts/r5-klaviyo-consolidation-v2.json` ve `docs/R5C_KLAVIYO_CONTROLLED_CLEAN_RESET.md` içindedir. Canlı revoke ve Supabase mutation henüz yapılmamıştır.
- **R5-C — Tamamlandı / R6 approval gate:** PR #232 merge commit `cb51f91f257622c9e6715fd2ae960cf9a0b596d4` ile hazırlık, PR #233 merge commit `8f183720fe1e64975ce8adabbeb8318f55257259` ile Shopify-native execution gate production'a alındı. Son deployment `READY`, target `production`, alias `dev.adstable.app` ve alias hatası `null` olarak doğrulandı. Merchant resmi `s-modal` içindeki **Remove connection** eylemiyle işlem-anı onayı verdi; session-bound `POST /api/shopify/providers/klaviyo/accounts/reset` production logunda bir kez `200` döndü. Salt-okunur Supabase postcheck `PASS`: tek embedded Klaviyo satırı `revoked`, `connected` satır `0`, canonical Klaviyo `0`; aktif hesap, aylık maliyet, source currency ve encrypted access/refresh zarfları tarihçe olarak korundu. Aktif human-attested binding `1`, Klaviyo schedule/job `0`, legacy freeze trigger `6`, plaintext legacy token `0` kaldı. Reset yeni OAuth, refresh, canonical insert, Dataset V2 yazımı veya token silme başlatmadı; Connect R6/R7 kabulüne kadar kapalıdır. R5 tamamlandı; sonraki çalışma ayrı kapsam ve onay kapısıyla R6'dır.
- **R6-A — Tamamlandı / R6-B workspace runtime gate:** 23 Eylül 2026 salt-okunur canlı preflight `PASS_PREPARATION_ONLY` verdi: `1` workspace, `0` workspace settings/reporting currency, `0` canonical connection, `0` Dataset V2 satırı, `0` aktif legacy schedule/job, `6` legacy freeze trigger ve `1` revoked embedded Klaviyo. Workspace foreign key ve unique index yerinde; Dataset V2 `user_id` hâlâ `NOT NULL`. Provider teması, migration veya database write yapılmadı. Versionlı contract `contracts/r6-workspace-provider-runtime-v1.json`, analist kararı `docs/R6_WORKSPACE_PROVIDER_RUNTIME.md` ve salt-okunur kapı `docs/security/sql/R6_WORKSPACE_RUNTIME_PREFLIGHT.sql` içindedir. R6 canlı kabulü reporting currency ve canonical connection istediği, bunları fication, ayrı capture/restore insan onayları, environment-only credential ve managed primitives doğrulanmış disposable Supabase target.

**Uygulama adımları:** Contract’ı doğrula; fixed schema-only capture planını review et; gelecekte sanitize capture al; validator ve insan review’dan geçir; cutoff ve migration classification’ı kesinleştir; target preflight yap; ayrı restore operatorünü ancak accepted baseline sonrasında hazırla; restore ve read-only acceptance evidence’ını review et.

**Kabul kriterleri:** Exact inventory/checksum; sıfır row/secret/managed DDL; final migration classification ve cutoff; managed primitive preflight; normalized object parity; sıfır application row; human-reviewed redacted evidence ve gerçek fresh-project restore.

**Test planı:** Node artifact/validator/converter unit testleri, tek-statement read-only SQL static kontrolleri, previous E2 regresyonları, full `npm test` ve security suite.

**Rollback:** Bu preparation yalnız repository değişikliğidir ve commit revert ile geri alınır. Gelecekte disposable target failure’ı production’a yönlendirilmez; teardown ayrı onay gerektirir.

**Gözlemlenebilirlik:** Redacted PASS/FAIL, counts ve SHA-256 evidence; raw SQL, project ref, URI, identity veya credential yok.

**Güvenlik/veri etkisi:** Production bağlantısı ve data/schema/ledger/privilege/deployment etkisi yok; actual capture ve restore yok.

**Planlanan:** Scope review, ardından ayrı onaylı capture/classification/cutoff/restore/acceptance zinciri.

**Gerçekleşen:** Scope contract ve capture operator/validator/inventory/acceptance preparation hazır. Actual schema capture yapılmadı; baseline SQL üretilmedi; cutoff kesinleşmedi; target provision edilmedi; restore çalıştırılmadı; fresh restore doğrulanmadı; production değişmedi.

E2-T8-A source inventory için açık insan onaylı tek production request çalıştırıldı ve `SOURCE_INVENTORY_QUERY_FAILED` ile fail-closed oldu. State/inventory kapsülü oluşmadı, schema capture çalışmadı ve otomatik retry yapılmadı. Corrective revizyon, sonraki insan onaylı denemede credential, query, service, timeout ve transport sınıflarını secret-free ayıracak şekilde hazırlanır; E2-T8-A `Verification` kalır.

İnsan onaylı corrective request `SOURCE_INVENTORY_MANAGEMENT_TRANSPORT_FAILED` ile fail-closed oldu. Credential-free ağ probe'u ortam proxy'si üzerinden Management API'ye ulaşırken Node fetch'in proxy sınırı olmadan `ENETUNREACH` verdiğini doğruladı. State/inventory kapsülü, schema capture ve production mutation oluşmadı; retry yapılmadı. Redacted sonuç `source-inventory-attempts.json` içinde tutulur; proxy-aware Node 24 CLI yeni insan onayı olmadan production request göndermez.

Proxy-aware üçüncü insan onaylı request Management API transport'unu geçti ve `SOURCE_INVENTORY_CONTRACT_FAILED` ile fail-closed oldu. Kapsül, schema capture, mutation ve otomatik retry yine oluşmadı. Corrective validator revizyonu raw object/identity göstermeden empty, row-shape, identity, ownership, fingerprint, duplicate ve application-empty sınıflarını ayırır; yeni request tekrar açık insan onayı gerektirir.

Classified dördüncü insan onaylı request `SOURCE_INVENTORY_OWNERSHIP_UNCLASSIFIED` ile fail-closed oldu. Production mutation, kapsül, schema capture veya retry oluşmadı. İş değeri değerlendirmesi sonrasında yeni diagnostic/capture operasyonları durduruldu ve E2-T8 defer/E2 closeout iş kararına taşındı.

**Sapmalar:** Actual baseline olmadan restore operatorü hazırlanmadı. Altı migration bilinçli olarak `pending_capture_checksum` ve replay-disabled kaldı.

**Evidence:** `artifacts/dataset-v2-acceptance/e2-t8-restore/`, `docs/security/E2_T8_RESTORE_READINESS_RUNBOOK.md`, `docs/security/sql/E2_T8_*.sql`, `security/e2-t8-restore-contract.js`, `scripts/e2-t8-*.js`, `tests/e2-t8-restore-readiness-artifacts.test.js`.

**Durum:** `Deferred` — açık insan iş kararıyla durduruldu; E2 kapanışını bloke etmez.

### Kabul kriterleri

- Canlı DDL migration sözleşmesiyle uyumludur veya drift kapatılmıştır.
- Aynı canonical key ikinci yazımda duplicate değil upsert üretir.
- Geçersiz canonical satırlar DB tarafından reddedilir.
- User yalnız kendi satırını okur; anon okuyamaz; authenticated istemci yazamaz.
- Service-role backend write/read çalışır.
- Test verisi temizlenir ve legacy tablolar değişmez.
- Dataset V2 mapper hiçbir provider için ayrı persistence shape veya alan kaybı üretmez.

### Test planı

- HTTPS Management/Data API ve güvenli SQL introspection evidence.
- Gerçek repository integration testi.
- Constraint table-driven negatif testleri.
- İki izole kullanıcıyla RLS testi.
- Exact count öncesi/sonrası ve cleanup testi.
- Migration static testleri.

### Rollback planı

- V2 henüz production read source yapılmaz.
- Destructive migration uygulanmaz; corrective migration ileri yönlüdür.
- Acceptance fixture'ları namespaced run ID ile silinir.
- V1/snapshot hattı değişmeden kalır.

### Bağımlılıklar

- E1 güvenli identity/ownership temeli.
- Supabase HTTPS Management API erişimi.
- İzole test kullanıcıları ve service-role test harness'i.

### Evidence

`artifacts/dataset-v2-acceptance/<run-id>/` altında schema, constraint/index, RLS, round-trip, upsert, rejection, cleanup ve legacy-no-change kanıtları.

### E2-T1/T2 task aynası — 2026-08-24 metadata acceptance

**Amaç:** Canlı Dataset V2 column, constraint, index, RLS, policy ve grant sözleşmesini yalnız read-only metadata ile repository migration'larına karşı doğrulamak.

**Mevcut durum:** Ledger reconciliation tamamlandı ve ledger 37 kayıtta. Dataset V2 tablosu canlıda mevcut fakat satır sayısı sıfır.

**Planlanan durum:** Redacted, deterministic ve executable testlerle korunan E2-T1/T2 evidence paketinin review ve merge edilmesi.

**Kapsam:** Beş allowlist SELECT/WITH SELECT amacıyla column, constraint/index, semantic fingerprint, RLS/policy/grant ve ledger/safe-state doğrulaması.

**Kapsam dışı:** Dataset write, fixture, round-trip, upsert, rejection, iki kullanıcı RLS matrisi, cleanup, V1/snapshot mutation ve runtime/UI değişikliği. E2-T3–T7 açık kalır.

**Bağımlılıklar:** E1 güvenlik postcondition'ları, tamamlanan ledger reconciliation ve açık E2-T8 restore-readiness takibi, Management API read-only erişimi ve repository baseline commit'i.

**Uygulama adımları:** GitHub main ve migration checksum doğrulandı; canlı metadata beş read-only query amacıyla yeniden okundu; repository/live contract karşılaştırıldı; redacted evidence ve contract testi üretildi.

**Kabul kriterleri:** 47 kolon; PK + user FK + 19 check; beş fiziksel index; sıfır invalid/unvalidated object; enabled/non-forced RLS; exact authenticated SELECT policy; beklenen role grant'leri; ledger 37; Dataset V2 row count sıfır.

**Test planı:** Dedicated evidence contract testi, full test, security regression, JavaScript syntax, diff/secret/PII kontrolleri.

**Rollback:** Database değişmedi. Repository rollback gerekirse yalnız evidence/plan commit'i revert edilir.

**Gözlemlenebilirlik:** Object adı, checksum/fingerprint, count, boolean, PASS/FAIL, evidence version ve repository commit ile sınırlı.

**Güvenlik ve veri etkisi:** Canlı sorgular read-only; data/schema/ledger/privilege/deployment etkisi yok; credential veya row data evidence'a alınmadı.

**Planlanan:** E2-T1/T2 metadata sözleşmesinin canlı kabul evidence'ı.

**Gerçekleşen:** E2-T1 ve E2-T2 metadata kontrolleri PASS. Ledger reconciliation ve production root/login smoke daha önce tamamlandı. Dataset V2 satır sayısı sıfır olduğundan persistence acceptance yapılmadı.

**Sapmalar:** Yok. E2-T3–T7 özellikle uygulanmadı.

**Evidence:** `artifacts/dataset-v2-acceptance/20260824-metadata-acceptance/` ve `tests/e2-dataset-v2-metadata-evidence.test.js`.

**Durum:** `Done` — E2-T1/T2 evidence PR review ve merge süreci tamamlandı.

### E2-T3A task aynası — canonical round-trip hazırlığı

**Amaç:** Tek bir namespaced Meta paid canonical fixture'ını Dataset V2 fiziksel sözleşmesine map eden, transaction içinde insert/read-back yapan, yedi canonical bloğu kayıpsız karşılaştıran ve zorunlu rollback ile kalıcı veri bırakmayan acceptance paketini hazırlamak.

**Mevcut durum:** E2-T1/T2 metadata evidence merge edildi; Dataset V2 canlı metadata sözleşmesi kabul edildi ve canlı satır sayısı son doğrulamada sıfırdı. E2-T3 canlı write/read operation henüz çalıştırılmadı.

**Planlanan durum:** Ayrı insan onayından sonra exact preflight, tek insert/read/rollback transaction ve read-only postcheck çalıştırılarak redacted round-trip evidence üretilmesi.

**Kapsam:** Meta paid fixture; canonical→physical ve physical→canonical mapper; unsupported/null, supported zero ve positive metric semantiği; identity dışındaki yedi blok; internal eligible-user seçimi; Dataset V2/V1/snapshot/OAuth/token count parity; fail-closed evidence dönüştürme.

**Kapsam dışı:** Canlı operation, ikinci insert/upsert, rejection matrisi, RLS kullanıcı matrisi, commit/cleanup, V1 veya snapshot mutation, runtime/UI, migration/schema/grant/policy, OAuth/token, deployment ve environment işlemleri.

**Bağımlılıklar:** Merge edilmiş E2-T1/T2 evidence, ledger 37 baseline'ı, mevcut canonical validator/entity hierarchy ve Dataset V2 mapper sözleşmesi; canlı operation için ayrıca insan onayı ve uygun auth/public user.

**Uygulama adımları:** Deterministik canonical ve physical fixture üretildi; read-only preflight/postcheck, tek transaction rollback operation, redacted evidence converter, runbook ve executable contract testi eklendi; production credential veya canlı bağlantı kullanılmadı.

**Kabul kriterleri:** Local canonical/physical round-trip exact; tek Dataset V2 insert ve read-back guard'ları; `COMMIT` yok ve zorunlu `ROLLBACK`; korunan relation'larda mutation yok; identity/credential sızıntısı yok; canlı operation ve postcheck tamamlanmadan durum `Done` değil.

**Test planı:** Dedicated E2-T3 artifact testi, full test, security regression, JavaScript syntax, diff ve secret/PII pattern kontrolleri.

**Rollback:** Hazırlık database'i değiştirmez. Repository rollback yalnız E2-T3 artefakt/plan commit'inin revert edilmesidir; gelecekteki canlı operation'ın zorunlu normal sonu transaction rollback'tir.

**Gözlemlenebilirlik:** Run ID, operation status, count, boolean, canonical alan adı, redacted değer ve PASS/FAIL ile sınırlıdır; gerçek identity ve raw production row yasaktır.

**Güvenlik ve veri etkisi:** Bu hazırlıkta data/schema/ledger/privilege/runtime/deployment etkisi yoktur. Hazırlanan operation yalnız Dataset V2'de tek geçici satır oluşturabilir ve aynı transaction içinde rollback eder.

**Planlanan:** Kontrollü production E2-T3 operation ve postcheck evidence'ı.

**Gerçekleşen:** Repository paketi ve local exact mapper/round-trip doğrulaması hazırlandı; canlı SQL çalıştırılmadı ve production user seçilmedi.

**Sapmalar:** Yok. E2-T4–T7 `Not started`, E2-T8 `Verification` olarak açık kalır.

**Evidence:** `artifacts/dataset-v2-acceptance/e2-t3-roundtrip/`, `docs/security/sql/E2_T3_ROUNDTRIP_*.sql`, `docs/security/E2_T3_ROUNDTRIP_RUNBOOK.md`, `scripts/e2-t3-roundtrip-evidence.js`, `tests/e2-t3-roundtrip-artifacts.test.js`.

**Durum:** `Verification` — canlı operation ve postcheck review edilmeden E2-T3 `Done` değildir.

### E2-T4 task aynası — same-key PostgreSQL upsert hazırlığı

**Amaç:** Migration-defined canonical unique key'i paylaşan initial ve updated Meta paid fixture yazımlarının gerçek PostgreSQL `ON CONFLICT DO UPDATE` ile tek satırda sonuçlanmasını, mutable değerlerin güncellenmesini ve zorunlu rollback ile kalıcı veri bırakılmamasını kanıtlayacak acceptance paketini hazırlamak.

**Mevcut durum:** E2-T1/T2 `Done`; E2-T3 repository paketi merge edildi fakat credential bulunmadığından canlı E2-T3 kabulü çalıştırılmadı ve `Verification` kaldı. E2-T4 canlı acceptance henüz çalıştırılmadı.

**Planlanan durum:** Ayrı insan onaylı bir operasyonda read-only preflight, initial insert, exact same-key PostgreSQL upsert, aggregate/redacted evidence, koşulsuz rollback ve read-only postcheck uygulanması.

**Kapsam:** `e2_t4_same_key_v1` namespaced Meta paid A/B fixture'ları; exact canonical conflict target; initial/upsert/final count ve duplicate guard'ları; mutable metric update; identity/hierarchy ve unsupported-null/supported-zero parity; V1/snapshot/OAuth/token no-change; fail-closed evidence.

**Kapsam dışı:** Bu taskta canlı SQL, E2-T3 canlı kabulü, E2-T5 rejection, E2-T6 RLS matrisi, E2-T7 cleanup, runtime/UI, migration, schema, ledger, RLS/policy/privilege, environment, deployment ve gateway işlemleri.

**Bağımlılıklar:** Güncel main, Dataset V2 migration canonical unique index'i, canonical validator/hierarchy/mapper ve ilerideki canlı operation için Management API credential ile ayrı insan onayı.

**Uygulama adımları:** A/B canonical fixture ve updated physical expectation üretildi; read-only preflight/postcheck, rollback-only transaction, evidence converter, runbook ve executable static/contract test eklendi; test zinciri ve security manifest güncellendi.

**Kabul kriterleri:** Conflict target migration ile exact; initial/upsert operation count `1`; final fixture count `1`; duplicate/excess `0`; B mutable değerleri mevcut; identity/hierarchy ve null/zero semantiği korunmuş; korunan relation mutation'ı ve identity/credential sızıntısı yok; final statement `ROLLBACK`; canlı kabul olmadan `Done` yok.

**Test planı:** E2-T4 artifact testi; E2-T3 ve metadata regression testleri; full ve security suite; JavaScript syntax; SQL statement/conflict/mutation kontrolleri; diff ve secret/PII taraması.

**Rollback planı:** Repository preparation database'i değiştirmez. Gelecekteki controlled operation'ın koşulsuz normal sonu `ROLLBACK`tır; residue halinde ad hoc cleanup yetkilendirilmez. Repository rollback yalnız E2-T4 commit revert'idir.

**Gözlemlenebilirlik:** Namespaced fixture alanları, count, boolean, güvenli expected/actual fixture değeri ve PASS/FAIL ile sınırlıdır; production identity, UUID, credential ve raw production row yasaktır.

**Güvenlik ve veri etkisi:** Bu taskta data/schema/ledger/privilege/deployment etkisi yoktur; Management API kullanılmadı ve canlı SQL çalıştırılmadı.

**Planlanan:** Kontrollü rollback-only E2-T4 canlı preflight, same-key upsert ve postcheck evidence'ı.

**Gerçekleşen:** Repository preparation tamamlandı. Canlı preflight, initial insert, same-key upsert ve postcheck çalıştırılmadı. Management API erişimi bu taskta kullanılmadı. Data/schema/ledger/privilege/deployment değişikliği yapılmadı.

**Sapmalar:** Yok. E2-T3 `Verification`; E2-T5–T7 `Not started`; E2-T8 `Verification` kalır.

**Evidence:** `artifacts/dataset-v2-acceptance/e2-t4-upsert/`, `docs/security/sql/E2_T4_UPSERT_*.sql`, `docs/security/E2_T4_UPSERT_RUNBOOK.md`, `scripts/e2-t4-upsert-evidence.js`, `tests/e2-t4-upsert-artifacts.test.js`.

**Durum:** `Verification` — repository preparation canlı acceptance yerine geçmez.

### E2-T5 task aynası — rollback-only Dataset V2 rejection matrisi hazırlığı

**Amaç:** Dataset V2 migration CHECK ve NOT NULL sözleşmelerinin 35 invalid canonical vaka için PostgreSQL seviyesinde fail-closed reddini, güvenli diagnostics ve zorunlu outer rollback ile kanıtlayacak preparation paketini hazırlamak.

**Mevcut durum:** E2-T1–T5 `Done`; E2-T5 V2 canlı acceptance tamamlandı ve approval capsule tüketildi.

**Planlanan durum:** Tamamlandı — exact read-only preflight, tek intact rollback-only transaction, redacted evidence conversion ve read-only scalar postcheck kabul edildi.

**Kapsam:** `e2_t5_rejection_v2`; 32 CHECK ve üç NOT NULL vaka; valid canonical baseline; migration-derived closed constraint sets; static inserts; nested exception subtransactions; safe SQLSTATE/constraint/column diagnostics; `pg_temp` evidence; Dataset V2/V1/snapshot/OAuth/token/ledger parity.

**Kapsam dışı:** Migration/schema/ledger/RLS/policy/grant/privilege değişikliği, persistent DDL, cleanup, runtime/UI, environment, deployment ve E2-T6/T7 uygulaması. Canlı operation yalnız onaylı rollback-only E2-T5 V2 acceptance ile sınırlıydı.

**Eski kapsam dışı kaydı:** Management API ve canlı SQL preparation aşamasında kapsam dışıydı; kabul aşamasında ayrı production onayıyla kullanıldı.

**Korunan sınırlar:** E2-T3/T4 tekrar edilmedi; E2-T6/T7 uygulanmadı; schema, ledger, RLS, policy, grant, privilege, runtime, UI, environment ve deployment değiştirilmedi.

**Bağımlılıklar:** Onaylı main `135c9e880dd6db22059175977a3c2850ebe079fa`; Dataset V2 create ve Klaviyo corrective migration checksum'ları; canonical validator/hierarchy/repository sözleşmeleri; tamamlanan ayrı insan onayı, environment-only credential ve bütün preflight stop gate'leri.

**Uygulama adımları:** Repository paketi merge edildi; full regression 320/320 PASS oldu; read-only preflight 18/18 PASS verdi; repository dışı approval capsule oluşturuldu; açık production onayıyla 35 ayrı exception bloğu taşıyan transaction bir kez gönderildi; final `ROLLBACK` uygulandı; mandatory postcheck 15/15 PASS verdi; capsule tüketildi.

**Kabul kriterleri:** Tam 35 unique vaka; SQLSTATE exact; CHECK actual constraint case-specific closed allowlist üyesi ve non-empty; NOT NULL exact column; yanlış/missing/extra/duplicate/accepted/residue/parity sonucu FAIL; tek final response; `COMMIT` yok; final `ROLLBACK`; canlı evidence review tamamlandı.

**Test planı:** Dedicated E2-T5 artifact/converter testi; E2-T3/T4, metadata ve ledger regression'ları; full/security suite; JavaScript syntax, SQL statement/mutation/diagnostic, diff ve secret/PII kontrolleri.

**Rollback planı:** Repository preparation database'i değiştirmez ve commit revert edilebilir. Gelecekteki operation'ın tek yetkili normal sonu outer `ROLLBACK`tır. Unexpected accept outer transaction içinde kalıp fail sayılır ve rollback edilir. Residue halinde retry veya ad hoc cleanup yoktur.

**Gözlemlenebilirlik:** Yalnız case code, expected/actual SQLSTATE, closed expected constraints, actual constraint, expected/actual column ve boolean/count parity alanları; SQLERRM/message/detail/hint/context, raw SQL, production identity/value ve credential yasaktır.

**Güvenlik ve veri etkisi:** Management API yalnız onaylı preflight, rollback-only transaction ve read-only postcheck için kullanıldı. Transaction request 1, retry 0; postcheck request 1, retry 0. Kalıcı data, schema, ledger, privilege veya deployment değişikliği oluşmadı; production count, identity, credential ve raw row commit edilmedi.

**Planlanan:** Tamamlandı — insan onaylı controlled E2-T5 V2 acceptance ve redacted evidence review.

**Gerçekleşen:** Preflight 18/18 PASS; 35-vaka rejection transaction PASS; mandatory postcheck 15/15 PASS; final state `CONSUMED`. Transaction ve postcheck retry edilmedi. Fixture residue ve unexpected acceptance sıfır; korunan parity kapıları PASS.

**Sapmalar:** İlk tasarım exact tek constraint hedefledi; cross-field overlap nedeniyle uygulanabilir değildi. 35 vaka ve schema değişmeden korundu; SQLSTATE exact kaldı; case-specific closed `expected_constraints` kabul edildi. Constraint order kullanılmadı ve allowlist canlı sonuçtan öğrenilmedi.

**Evidence:** `artifacts/dataset-v2-acceptance/e2-t5-rejection/live-acceptance-v2.json`, `artifacts/dataset-v2-acceptance/e2-t5-rejection/`, `docs/security/sql/E2_T5_REJECTION_*.sql`, `docs/security/E2_T5_REJECTION_RUNBOOK.md`, `scripts/e2-t5-rejection-evidence.js`, `tests/e2-t5-rejection-artifacts.test.js`.

**Durum:** `Done` — canlı V2 rejection acceptance, mandatory postcheck, redacted evidence review ve production no-change kabul edildi.

## 7. E3 — Backend modularization foundation

**Durum:** `Done` — E3-T1–E3-T10 uygulama, PR, CI ve post-merge `main` doğrulama kapıları tamamlandı.

### Hedef yapı

```text
src/
  app.js
  config/
  middleware/
  auth/
  oauth/
  routes/
  services/
  repositories/
  providers/
  jobs/
  funnel/
```

### Planlanan işler

- **E3-T1 — `Done` — Characterization baseline:** Kritik V1 route/status/response davranışlarını sabitle.
- **E3-T2 — `Done` — Composition root:** App oluşturma, dependency kurma ve `listen()` işlemini ayır.
- **E3-T3 — `Done` — Config boundary:** Environment doğrulama ve typed config sınırı kur.
- **E3-T4 — `Done` — Shared clients + static entrypoint incident corrective:** Supabase/provider client creation'ı merkezi dependency yap.
- **E3-T5 — `Done` — Middleware boundary:** Auth, access, ownership, error, request ID ve logging'i ayır.
- **E3-T6 — `Done` — Route registration:** İnce route→validation→authorization→service→repository akışını kur.
- **E3-T7 — `Done` — OAuth extraction:** E1'de güvenli hale gelen OAuth'u modüle taşı.
- **E3-T8 — `Done` — Job boundary:** Refresh/snapshot orchestration için test edilebilir job sınırı kur.
- **E3-T9 — `Done` — Architecture guard:** Yeni business logic'in kök monolite eklenmesini CI kontrolüyle engelle.
- **E3-T10 — `Done` — Canonical boundary guard:** Provider-specific DTO'nun canonical validator'ı atlayarak repository, Formula Engine veya Funnel API sınırına geçmesini engelle.

### E3 ilerleme raporlama kuralı

- Her koordinasyon özetinde E3 için son tamamlanan iş, devam eden iş ve sıradaki aday ayrı maddeler halinde yazılır.
- Bir task bölünmeden tek PR'da ilerlerse `E3-T1`, `E3-T2`, `E3-T3` kimlikleri korunur.
- Bir task birden fazla kontrollü parçaya ayrılırsa alt işler `E3-T1-A`, `E3-T1-B` biçiminde adlandırılır; parent `E3-T1`, bütün zorunlu alt işler tamamlanmadan `Done` olmaz.
- Tek PR birden fazla taskı gerçekten bütün kabul kriterleriyle kapatırsa özet `E3-T1 + E3-T2 + E3-T3 Done` biçiminde yazılır; yalnız hazırlanan fakat kabulü tamamlanmayan tasklar `Verification` olarak ayrıca gösterilir.
- Güncel E3 özeti: E3-T1–E3-T10 ve parent E3 `Done`. Son tamamlanan iş E3-T10; sıradaki uygulanabilir aday E4-T1 Meta provider fixture ve mevcut fetch characterization. E2 ana production kabul hattı değişmez.

### Kabul kriterleri

- App port dinlemeden testte oluşturulabilir.
- Yeni provider/Funnel rotası kök `server.js` içine business logic eklemeden kaydedilebilir.
- Auth ve ownership'in tek canonical uygulaması vardır.
- Handler'lar dependency injection ile test edilebilir.
- Standart error contract ve request correlation vardır.
- Kritik V1 smoke/characterization testleri değişmeden geçer.
- `server.js` sorumluluk ve satır sayısı yeni epic'lerle artmaz.
- Bütün adapter'lar aynı canonical validator ve repository portunu kullanır; provider'a özel paralel analytics pipeline yoktur.

### Test planı

- App boot ve graceful shutdown.
- Route registration ve missing dependency.
- Auth/ownership negatif testleri.
- Error normalization.
- V1 critical route characterization/smoke.
- Import-cycle ve architecture boundary kontrolü.

### Rollback planı

- Her extraction küçük ve bağımsız değişikliktir.
- Route-level delegation/feature flag eski handler'a dönebilir.
- Parity sağlanmadan eski uygulama silinmez.
- DB schema değişikliği bu epic'e dahil edilmez.

### Bağımlılıklar

- E0 mimari kararları.
- E1 güvenli auth/OAuth davranışı.
- Kritik endpoint envanteri.

### Evidence

- Before/after responsibility map.
- Characterization sonuçları.
- Architecture guard çıktısı.
- Route parity raporu.

## 8. E4 — Meta referans vertical slice

**Durum:** `Done` — E4-T1–T8 ve zero-data canlı production kabulü tamamlandı; gerçek reklam satırı ilk oluştuğunda mevcut evidence ile izlenecek.

### Planlanan işler

- **E4-T1 — `Done`:** Meta provider fixture ve mevcut fetch characterization; Paid Funnel `ad_click = link_click`, `clicks` yalnız delivery/parity evidence olarak onaylandı.
- **E4-T2 — `Done`:** Client/mapper/capabilities/adapter modülleri `src/providers/meta` altında kabul edildi.
- **E4-T2A — `Done`:** `Campaign → AdSet → Ad` root/parent/leaf lineage ve deterministic entity key mapping kabul edildi.
- **E4-T2B — `Done`:** Meta output yedi bloklu canonical envelope'a normalize ediliyor; provider DTO adapter sınırının dışına çıkmıyor.
- **E4-T3 — `Done`:** ATC/Checkout/Purchase count/value mapping ve provenance kabul edildi.
- **E4-T4 — `Done`:** Account timezone/currency doğrulaması ve Time/FX service binding kabul edildi.
- **E4-T5 — `Done`:** Canonical validation ve Dataset V2 idempotent write boundary kabul edildi.
- **E4-T6 — `Done`:** Refresh job retry/idempotency/telemetry sözleşmesi kabul edildi.
- **E4-T7 — `Done`:** Kullanıcı/account allowlist ile V1+V2 shadow dual-write boundary kabul edildi.
- **E4-T8 — `Done`:** Provider→canonical→FX→V2→Formula sentetik expected totals parity kabul edildi.
- **E4 canlı kabul — `Done`:** Doğru Meta hesap business date sorgusu, geçerli provider response, zero-row/fake-free Dataset V2 sonucu ve kalıcı redacted evidence insan iş kararıyla kabul edildi.

#### E4-T1 task aynası — Meta alan ve mevcut fetch karakterizasyonu

**Amaç:** Meta API alanlarını AdsTable canonical iş gerçekleriyle eşleştirmeden önce mevcut davranışı ve açık iş kararlarını executable fixture ile sabitlemek.

**Mevcut durum:** Meta account discovery ve Campaign/AdSet/Ad Insights fetch çalışıyor; provider-specific mapping, derived KPI ve sıfır fallback davranışı kök `server.js` içinde.

**Planlanan durum:** Account/time/currency, lineage, raw metric, conversion alias, evidence-only alan ve Dataset V2'ye girmeyecek derived alan kararları review edilmiş baseline olur.

**Kapsam:** Sentetik provider fixture, mevcut fetch/action priority characterization, alan karar matrisi ve executable contract testleri.

**Kapsam dışı:** Meta production API çağrısı, runtime adapter, Dataset V2 write, dual-write, feature flag, deployment veya UI değişikliği.

**Bağımlılıklar:** E1, E2 ve E3 `Done`; V4 canonical envelope ve Meta hierarchy freeze.

**Uygulama adımları:** Mevcut sorgu alanlarını sabitle; Meta alias örneklerini fixture'a bağla; canonical/evidence/forbidden alanları ayır; `ad_click` iş kararını review'a sun.

**Kabul kriterleri:** Fixture secret-free ve sentetik; Campaign→AdSet→Ad eksiksiz; standard/omni alias'lar toplanmıyor; derived KPI'lar V2 fact sayılmıyor; açık click kararı belgeli.

**Test planı:** Fixture schema/redaction, mevcut source characterization, alias double-count negatif kontrolü, plan/status ve karar matrisi testleri; full/security/architecture/canonical suite.

**Rollback planı:** Runtime etkisi yoktur; commit revert fixture, doküman, test ve plan durumunu kaldırır.

**Gözlemlenebilirlik:** Yalnız sentetik fixture ve statik karakterizasyon; production kimliği, count veya credential yok.

**Güvenlik ve veri etkisi:** Production request/write yok; secret, PII ve gerçek hesap kimliği yok.

**Planlanan:** Meta alan sözleşmesinin uygulanmadan önce review edilmesi.

**Gerçekleşen:** Sentetik fixture, mevcut davranış baseline'ı ve alan karar matrisi hazırlandı; runtime değiştirilmedi.

**Sapmalar:** Yok. `ad_click = link_click` açık insan iş kararıyla onaylandı; `clicks` canonical toplama girmez.

**Evidence:** `artifacts/e4-meta/e4-t1-provider-fixture.json`, `docs/E4_T1_META_CHARACTERIZATION.md`, `tests/e4-t1-meta-characterization.test.js`.

**Durum:** `Done` — fixture, characterization, `ad_click` iş kararı, PR/CI ve review tamamlandı.

#### E4-T2 + E4-T2A + E4-T2B task aynası — Meta adapter sınırı

**Amaç:** Meta ham API cevabını, provider alanlarını sistemin geri kalanına sızdırmadan AdsTable ortak veri diline çevirmek.

**Mevcut durum:** E4-T1 alan sözleşmesi `Done`; mevcut Meta fetch/mapping kök `server.js` içinde ve V1 snapshot'a özel.

**Planlanan durum:** Bağımsız Meta client, capability ve mapper; Ad leaf lineage, deterministik key ve yedi bloklu canonical çıktı üretir.

**Kapsam:** `src/providers/meta` client/capabilities/mapper/adapter, sentetik fixture mapping'i, canonical/hierarchy ve negatif contract testleri.

**Kapsam dışı:** Production Meta request, Time/FX servis binding, Dataset V2 write, job, dual-write, feature flag, deployment ve UI.

**Bağımlılıklar:** E4-T1 `Done`; E3 canonical boundary; V4 envelope/hierarchy freeze.

**Uygulama adımları:** Fixed Ad-level client kur; capability kararlarını kodla; provider DTO'yu canonical row'a map et; canonical/hierarchy validator ve deterministic key ile doğrula; adapter dışına yalnız canonical sonuç çıkar.

**Kabul kriterleri:** Token URL'ye girmez; daily Ad-level fetch; Campaign/AdSet/Ad eksiksiz; `link_click` canonical click; alias double-count yok; eksik metrik unknown/null; gerçek zero supported; derived KPI sızıntısı yok; E4-T4 öncesi cross-currency çıktı fail-closed olur.

**Test planı:** Client request/auth, capability, seven-block envelope, deterministic key, alias, zero/null/support, eksik lineage ve adapter output testleri; full/security/architecture/canonical suite.

**Rollback planı:** Runtime delegation yoktur; commit revert yeni modülleri ve testleri kaldırır, V1 davranışı değişmez.

**Gözlemlenebilirlik:** Adapter version ve redacted action-type provenance; raw provider payload dışarı çıkmaz.

**Güvenlik ve veri etkisi:** Production request/write yok; access token yalnız Authorization header contract'ında; fixture sentetik.

**Planlanan:** Meta provider DTO → ortak canonical envelope sınırı.

**Gerçekleşen:** Client/capabilities/mapper/adapter ve executable testler hazır; runtime henüz bağlanmadı.

**Sapmalar:** Time/FX değerleri mapper context'inden alınır; gerçek servis binding'i planlandığı gibi E4-T4 kapsamındadır.

**Evidence:** `src/providers/meta/`, `tests/e4-t2-meta-adapter.test.js`, E4-T1 sentetik fixture.

**Durum:** `Done` — client/capability/mapper/adapter, lineage, canonical envelope, PR/CI ve insan review tamamlandı.

#### E4-T3 task aynası — Meta conversion provenance

**Amaç:** AdsTable ATC, Checkout ve Purchase count/value değerlerinin Meta'da hangi exact action kaydından geldiğini açıklanabilir yapmak.

**Mevcut durum:** E4-T2 canonical mapping `Done`; action priority değeri doğru seçiyor fakat seçilen source field/action type/fallback kararını metrik bazında taşımıyordu.

**Planlanan durum:** On canonical metriğin her biri value taşımayan, review edilebilir source provenance'a sahip olur; standard/omni ve count/value kararları ayrı izlenir.

**Kapsam:** Action selection provenance, row confidence (`real|fallback|partial`) ve standard/fallback/missing/mixed count-value testleri.

**Kapsam dışı:** Meta production request, runtime binding, Time/FX, Dataset V2 write, job, dual-write, deployment ve UI.

**Bağımlılıklar:** E4-T1 + E4-T2 + E4-T2A + E4-T2B `Done`.

**Uygulama adımları:** Her metriğin source field/action type/fallback bilgisini üret; raw değerleri provenance'a kopyalama; fallback/partial confidence belirle; count/value kaynaklarını bağımsız test et.

**Kabul kriterleri:** Standard action kazanır; omni yalnız fallback; alias toplanmaz; missing unknown/null; provenance raw value taşımaz; count/value source kararları ayrı; confidence deterministik.

**Test planı:** Standard, omni-only, missing, mixed count/value ve no-value-leak testleri; full/security/architecture/canonical suite.

**Rollback planı:** Runtime binding yoktur; commit revert provenance genişlemesini kaldırır, E4-T2 mapping değerleri değişmez.

**Gözlemlenebilirlik:** `metric_sources` yalnız `source_field`, `action_type`, `fallback_used`; gerçek metric value veya raw payload içermez.

**Güvenlik ve veri etkisi:** Sentetik fixture; production request/write ve identity/credential yok.

**Planlanan:** Conversion rakamlarının kaynağını kullanıcı desteği ve parity incelemesi için açıklanabilir kılmak.

**Gerçekleşen:** Metrik bazlı source provenance ve row confidence hazır; runtime henüz bağlanmadı.

**Sapmalar:** Yok.

**Evidence:** `src/providers/meta/mapper.js`, `tests/e4-t3-conversion-provenance.test.js`.

**Durum:** `Done` — PR #100, CI ve insan merge onayı tamamlandı.

#### E4-T4 task aynası — Meta Time/FX bağlama

**Amaç:** Meta rapor gününü hesabın gerçek saat dilimine, parasal metrikleri ise açık ve denetlenebilir kur bilgisine bağlamak.

**Mevcut durum:** E4-T3 `Done`; mapper context değerlerini taşıyor fakat Meta hesap metadata'sını ortak Time ve FX servisleri üzerinden doğrulamıyordu.

**Planlanan durum:** Hesap kimliği, timezone ve currency Meta account metadata'sından doğrulanır; günlük tarih Time Service, spend ve conversion value alanları FX Service tarafından normalize edilir.

**Kapsam:** Account identity/currency/timezone parity, tek günlük insight sınırı, same-currency rate=1, cross-currency explicit positive rate/provider ve dört parasal fact'in tek dönüşümü.

**Kapsam dışı:** Production Meta request, gerçek kur sağlayıcı çağrısı, Dataset V2 write, refresh job, dual-write, deployment ve UI.

**Bağımlılıklar:** E4-T1–T3 `Done`; ortak `time-service` ve `fx-service` hazır.

**Uygulama adımları:** Account metadata'yı doğrula; provider date'i account timezone ile normalize et; mapper'ın source-currency çıktısını FX Service'e ver; canonical row ve entity key'i yeniden doğrula.

**Kabul kriterleri:** Yanlış account/currency/timezone fail-closed; date range günlük; same-currency rate=1; cross-currency rate/provider zorunlu; desteklenen tüm parasal fact'ler bir kez çevrilir; adet metrikleri değişmez.

**Test planı:** Same-currency, cross-currency, metadata spoof/mismatch, invalid timezone, multi-day insight, missing/invalid FX testleri; full/security/architecture/canonical suite.

**Rollback planı:** Production delegation yoktur; commit revert adapter normalization katmanını kaldırır ve E4-T3 mapper davranışına döner.

**Gözlemlenebilirlik:** Canonical `time` ve `currency` blokları kullanılan timezone, business date, rate date, provider ve engine version'ı taşır; credential veya raw account payload taşımaz.

**Güvenlik ve veri etkisi:** Sentetik fixture; production request/write, müşteri kimliği ve credential yok.

**Planlanan:** Meta gün ve para değerlerinin ortak AdsTable standardında karşılaştırılabilir olması.

**Gerçekleşen:** Account metadata kontrollü Time/FX binding ve executable negatif kontroller hazır; runtime henüz bağlanmadı.

**Sapmalar:** Gerçek FX provider çağrısı yapılmadı; rate ve provider bu pakette açık input sözleşmesidir.

**Evidence:** `src/providers/meta/normalization.js`, `src/providers/meta/adapter.js`, `tests/e4-t4-meta-time-fx.test.js`.

**Durum:** `Done` — PR #101, CI ve insan merge onayı tamamlandı.

#### E4-T5 task aynası — Meta canonical Dataset V2 write

**Amaç:** Doğrulanmış Meta sonuçlarını Dataset V2'ye aynı dönem yeniden işlendiğinde mükerrer kayıt üretmeden güvenli biçimde yazmak.

**Mevcut durum:** E4-T4 `Done`; Meta adapter canonical sonuç üretiyor fakat provider akışı canonical write boundary üzerinden Dataset V2 repository semantiğine bağlanmamıştı.

**Planlanan durum:** Meta sonuçları ownership, canonical contract, hierarchy ve entity key kontrollerinden sonra yalnız ortak write boundary üzerinden UPSERT edilir; tekrar aynı canonical identity'yi değiştirir, çoğaltmaz.

**Kapsam:** Meta dataset writer, user/account ownership, canonical/entity-key doğrulaması, tek boundary delegation, sonuç cardinality kontrolü ve in-memory idempotent repository testleri.

**Kapsam dışı:** Production Supabase write, runtime route/job binding, refresh retry/telemetry, dual-write, deployment ve UI.

**Bağımlılıklar:** E4-T1–T4 `Done`; E3 canonical write boundary ve E2 Dataset V2 repository sözleşmesi hazır.

**Uygulama adımları:** Adapter sonuçlarını doğrula; input user/account ile row ownership parity kur; entity key'i yeniden üret; canonical boundary'ye bir kez devret; tekrar ve düzeltme senaryolarını doğrula.

**Kabul kriterleri:** Provider DTO yazılamaz; wrong user/account reddedilir; invalid canonical/hierarchy/key yazılmaz; aynı period retry tek satır; corrected result aynı identity'yi günceller; write sonucu cardinality saparsa başarı raporlanmaz.

**Test planı:** İlk write, aynı input retry, corrected facts, ownership/canonical/key negatifleri ve cardinality fail-closed; full/security/architecture/canonical suite.

**Rollback planı:** Production binding yoktur; commit revert Meta dataset writer ve testlerini kaldırır, mevcut Dataset V2 repository değişmez.

**Gözlemlenebilirlik:** Sonuç yalnız attempted/persisted adetleri ve canonical repository sonucunu taşır; credential veya provider raw DTO loglamaz.

**Güvenlik ve veri etkisi:** Sentetik fixture ve in-memory repository; production request/write, müşteri verisi ve credential yok.

**Planlanan:** Meta verisinin Dataset V2'ye güvenli ve tekrarlanabilir giriş kapısını kurmak.

**Gerçekleşen:** Ownership kontrollü canonical writer ve idempotent/corrective executable testler hazır; runtime henüz bağlanmadı.

**Sapmalar:** Production Supabase entegrasyonu çalıştırılmadı; açık production onayı gerektirir.

**Evidence:** `src/providers/meta/dataset-writer.js`, `tests/e4-t5-meta-idempotent-write.test.js`.

**Durum:** `Done` — PR #102, CI ve insan merge onayı tamamlandı.

#### E4-T6 task aynası — Meta refresh güvenilirliği

**Amaç:** Meta yenileme işinin geçici servis sorunlarını kontrollü atlatmasını, kalıcı hatalarda durmasını ve operasyon sonucunun hassas veri taşımadan izlenmesini sağlamak.

**Mevcut durum:** E4-T5 `Done`; idempotent writer hazır fakat job lifecycle, sınırlı retry ve güvenli telemetry tek Meta refresh akışında birleşmemişti.

**Planlanan durum:** Bir refresh job altında yalnız transient transport/429/5xx hataları sınırlı exponential backoff ile tekrar edilir; auth/contract hataları tekrar edilmez; T5 idempotency her denemede duplicate'i önler.

**Kapsam:** Meta API safe error classification, 1–5 bounded attempt, 100/200ms exponential backoff baseline, job boundary delegation, allowlisted telemetry ve sanitized terminal failure.

**Kapsam dışı:** Production scheduler, canlı Meta tokenı, Supabase write, queue worker, deployment, dual-write ve UI.

**Bağımlılıklar:** E4-T1–T5 `Done`; ortak refresh job boundary hazır.

**Uygulama adımları:** HTTP/transport hatalarını sınıflandır; transient allowlist kur; tek job içinde bounded retry çalıştır; tamamlanma metadata'sına attempts/rows_written yaz; ham hata mesajını dışarı çıkarma.

**Kabul kriterleri:** 429/5xx/transport retry; auth/request/contract no-retry; attempt üst sınırı; tek job lifecycle; deterministic backoff; telemetry yalnız job/attempt/count/safe-code; terminal hata provider body/credential taşımaz.

**Test planı:** İki transient sonrası başarı, permanent no-retry, exhaustion, HTTP classification/no-body-leak, invalid config/input; full/security/architecture/canonical suite.

**Rollback planı:** Runtime binding yoktur; commit revert refresh runner ve client classification genişlemesini kaldırır, E4-T5 writer değişmez.

**Gözlemlenebilirlik:** `meta_refresh_attempt|retry|completed|failed` event'leri yalnız job id, attempt, limit, rows_written ve safe_code taşır.

**Güvenlik ve veri etkisi:** Sentetik/mocked çalışma; production request/write, token, müşteri verisi ve raw provider body yok.

**Planlanan:** Geçici Meta kesintilerinde gereksiz kullanıcı müdahalesini azaltırken kalıcı hataları hızla görünür kılmak.

**Gerçekleşen:** Bounded retry, safe classification, sanitized telemetry ve job completion metadata hazır; runtime henüz bağlanmadı.

**Sapmalar:** Production scheduler/queue entegrasyonu yapılmadı; açık production onayı gerektirir.

**Evidence:** `src/providers/meta/refresh-runner.js`, `src/providers/meta/client.js`, `tests/e4-t6-meta-refresh-reliability.test.js`.

**Durum:** `Done` — PR #103, CI ve insan merge onayı tamamlandı.

#### E4-T7 task aynası — Meta allowlisted shadow dual-write

**Amaç:** Mevcut V1 Meta snapshot sonucunu değiştirmeden, yalnız açıkça izin verilen kullanıcı ve reklam hesaplarında V2 yazımını gölge olarak çalıştırmak.

**Mevcut durum:** E4-T6 `Done`; V2 refresh zinciri hazır fakat V1 yanında hangi kullanıcı/hesap için devreye gireceğini belirleyen kapalı varsayılanlı dual-write sınırı yoktu.

**Planlanan durum:** Dual-write default kapalı ve allowlist boş; V1 daima önce ve authoritative çalışır; yalnız exact user+account eşleşmesinde V2 çağrılır; V2 başarısızlığı başarılı V1 sonucunu değiştirmez.

**Kapsam:** Boolean master switch, exact pair allowlist, V1-first sıralama, ownership parity, V1 response identity/no-change, V2 safe telemetry ve repeated delegation.

**Kapsam dışı:** `server.js` runtime wiring, production flag/allowlist değeri, canlı V1/V2 write, deployment, kullanıcı rollout'u ve UI.

**Bağımlılıklar:** E4-T1–T6 `Done`; V1 snapshot yolu korunuyor; V2 refresh idempotent.

**Uygulama adımları:** Allowlist'i exact schema ile doğrula; disabled/not-allowlisted/mismatch skip et; V1 sonucunu önce al; allowlisted durumda V2'yi shadow çağır; V2 hatasını güvenli telemetry'ye indirgeme.

**Kabul kriterleri:** Default off/empty; pair eşleşmesi exact; V1 önce; dönen V1 object/shape aynı; non-allowlisted V2 yok; ownership mismatch V2 yok; V2 hata V1'i bozmaz; unknown safeCode dışarı taşınmaz; tekrar T5 idempotency'ye delege edilir.

**Test planı:** Disabled, non-allowlisted, allowlisted order/no-change, V2 failure, unknown-code redaction, ownership mismatch, malformed/duplicate allowlist ve repeated run; full/security/architecture/canonical suite.

**Rollback planı:** Runtime wiring yoktur; commit revert dual-write coordinator ve testini kaldırır; V1 ve V2 bağımsız akışlar değişmez.

**Gözlemlenebilirlik:** Yalnız `skipped|completed|failed`, safe reason/code, attempt ve rows_written; kullanıcı/account identity, raw provider body ve credential yok.

**Güvenlik ve veri etkisi:** Sentetik/mock çalışma; master switch default off, allowlist empty; production request/write yok.

**Planlanan:** V2'yi küçük ve açık bir grupta V1 sonucuna risk oluşturmadan gözlemleyebilmek.

**Gerçekleşen:** Default-off exact-pair shadow coordinator, V1 no-change ve safe failure telemetry testleri hazır; runtime henüz bağlanmadı.

**Sapmalar:** Production allowlist/flag konfigürasyonu ve rollout yapılmadı; açık production onayı gerektirir.

**Evidence:** `src/providers/meta/dual-write.js`, `tests/e4-t7-meta-allowlisted-dual-write.test.js`.

**Durum:** `Done` — PR #104, CI ve insan merge onayı tamamlandı.

#### E4-T8 task aynası — Meta uçtan uca expected totals parity

**Amaç:** Aynı Meta döneminin provider cevabından Formula Engine sonucuna kadar hiçbir aşamada iş rakamı değiştirmeden veya kaybetmeden ilerlediğini tek kabul zincirinde kanıtlamak.

**Mevcut durum:** E4-T7 `Done`; her katman ayrı testli fakat provider→canonical→FX→Dataset V2→Query/Formula zincirinin aynı frozen expected totals ile ortak kabulü yoktu.

**Planlanan durum:** Sentetik Meta fixture USD kaynaktan TRY hedefe açık rate=32 ile işlenir; raw fact, V2 roundtrip, paid funnel, intent formülleri ve retry sonucu tek versioned expected artifact ile karşılaştırılır.

**Kapsam:** Provider fixture, standard action seçimi, Time/FX, canonical write, in-memory V2 read, paid query, Formula/Intent totals, retry idempotency ve drift negatif kontrolü.

**Kapsam dışı:** Production Meta request/token, gerçek FX sağlayıcı, Supabase write, runtime/dual-write activation, deployment, rollout ve UI.

**Bağımlılıklar:** E4-T1–T7 `Done`; canonical/query/formula ve Dataset V2 repository sözleşmeleri hazır.

**Uygulama adımları:** Expected artifact'i freeze et; tam zinciri sentetik client ile çalıştır; raw metrics ve currency/time/provenance'ı doğrula; funnel/intent totals'ı tolerance ile reconcile et; replay ve drift kontrolü ekle.

**Kabul kriterleri:** Tek V2 row; 10 raw metric exact; TRY rate=32; standard purchase provenance; expected funnel/intent totals; Formula v1; replay duplicate yok; bir birim drift testi fail eder; artifact synthetic/versioned.

**Test planı:** Provider-to-V2 raw parity, V2-to-Formula totals parity, complete-chain replay ve intentional drift negative; full/security/architecture/canonical suite.

**Rollback planı:** Runtime etkisi yoktur; commit revert expected artifact ve E4-T8 acceptance testini kaldırır, E4-T1–T7 davranışı değişmez.

**Gözlemlenebilirlik:** Versioned synthetic expected artifact yalnız sentetik metric/totals ve FX contract taşır; identity, credential veya production count yok.

**Güvenlik ve veri etkisi:** Tamamen sentetik/in-memory; production request/write, müşteri verisi ve token yok.

**Planlanan:** Meta vertical slice'ın iş sonucu açısından bütün katmanlarda aynı rakamı verdiğini kanıtlamak.

**Gerçekleşen:** Provider→canonical→FX→V2→Formula raw, funnel, intent, replay ve drift acceptance zinciri hazır.

**Sapmalar:** Gerçek provider/Supabase kabulü çalıştırılmadı; production activation ayrı insan onayı gerektirir.

**Evidence:** `artifacts/e4-meta/e4-t8-expected-parity.json`, `tests/e4-t8-meta-end-to-end-parity.test.js`.

**Durum:** `Done` — PR #105, CI ve insan merge onayıyla sentetik E4-T8 tamamlandı; E4 kapanışı gerçek Meta canlı kabulüne bağlıdır.

#### E4 canlı kabul kapısı — Meta Refresh V2-primary runtime bağlantısı

**İş kararı:** Uygulama geliştirme aşamasında ve tek kullanıcı vardır; V1 dashboard/dataset artık hedef değildir. Meta Refresh, production gate açıldığında V2'ye doğrudan yazacaktır; dual-write kullanılmayacaktır.

**Amaç:** Kullanıcının mevcut Meta Refresh işlemini gerçek Meta verisiyle Dataset V2'ye bağlamak ve Google'a geçmeden önce canlı V2 sonucunu birlikte doğrulamak.

**Mevcut durum:** E4-T1–T8 kod/sentetik kabul `Done`; V2-primary runtime production'da aktiftir. Kalıcı evidence ile yapılan ilk canlı Refresh Meta tarafından geçerli fakat sıfır satırlı cevap verdi. Kanıt, isteğin Meta hesap saat dilimi yerine caller tarafından taşınan `2026-08-31` tarihini kullandığını gösterdi; bu nedenle canlı kabul tamamlanmadı.

**Planlanan durum:** Açık insan production onayı sonrası `META_V2_PRIMARY_REFRESH_ENABLED` checked-in default `true` olur. Explicit `false` rollback sağlar; aktifken aynı Refresh job gerçek Meta Ad daily insights'ı doğrudan canonical→FX→Dataset V2 UPSERT zincirine gönderir ve V1 snapshot yazmaz.

**Kapsam:** Gerçek account discovery, caller tarihini reddedip Meta hesap saat diliminden üretilen tek business date için günlük Meta sorgusu, cursor pagination, business-date FX tarihi, account/currency/timezone parity, source job lineage, server-side Supabase repository, V2 primary handler wiring ve güvenli zero-row sonuç kanıtı.

**Kapsam dışı:** Bu PR içinde canlı Refresh çalıştırılması veya geçmiş dönem backfill, Google E5, sentetik/fake empty row, V1 migration veya UI redesign.

**Teknik karar — boş veri:** Meta gerçek Ad insight döndürmezse işlem başarılı `persisted=0` ve `empty_provider_result=true` üretir; Dataset V2'ye sahte reklam satırı yazılmaz. Meta gerçek satır döndürürse yalnız bu gerçek satırlar UPSERT edilir.

**Kabul kriterleri:** Production-approved gate default true; explicit false rollback; enabled path V2-primary; V1 write yok; tüm cursor sayfaları aynı güvenli endpoint/cursor ile alınır; provider `next` URL/token izlenmez; gerçek satır V2 UPSERT; empty result zero-row/fake-free; response V2 outcome taşır; source job id persist edilir.

**Canlı kabul sırası:** business-date evidence PR ve CI → merge onayı → kullanıcının normal Meta Refresh'i → job metadata içindeki request/response/mapping/V2 kanıtı ile V2 row/ownership/date/currency/provenance kontrolü → gerekirse ayrı onaylı idempotency kontrolü → Formula parity → E4 closeout. E5 bu sıra tamamlanmadan başlamaz.

**Rollback:** Production gate kapatılır; V2-primary çağrı anında durur ve mevcut V1 fallback yolu kodda korunur.

**Güvenlik ve veri etkisi:** Kod bağlantısı production-capable; mevcut V2-primary activation daha önce açık insan onayıyla tamamlanmıştır. Bu PR kendi başına Meta API çağrısı veya production write yapmaz.

**Kalıcı redacted kanıt:** Refresh job metadata yalnız sorgu tarih aralığı/gün sayısı/level/time increment/alan adlarını, provider page/row sayılarını, accepted/rejected sayılarını ve Dataset V2 attempted/persisted sonucunu taşır. Token, kullanıcı/reklam hesabı/varlık kimliği ve ham metrik değeri taşımaz. Başarılı mapping fail-closed olduğu için `rejected=0`; herhangi bir mapping hatası tüm işi başarısız yapar ve kısmi başarı raporlanmaz.

**Evidence:** `src/providers/meta/live-refresh.js`, `src/providers/meta/client.js`, `server.js`, `tests/e4-live-meta-v2-primary.test.js`, `tests/e4-live-meta-runtime-wiring.test.js`.

**Durum:** `Done` — PR #109 sonrası doğru account business date ile canlı Refresh completed/no-error, provider page `1`, provider row `0`, mapping accepted/rejected `0/0`, Dataset V2 attempted/persisted `0/0` ve fake row `0` olarak doğrulandı. Zero-data canlı kabul insan iş kararıyla kapatıldı; gerçek reklam satırı ilk oluştuğunda mevcut evidence ile izlenecek.

### Kabul kriterleri

- Meta mapping route handler içinde değildir.
- Meta Ad leaf'i Campaign ve AdSet lineage'ını kayıpsız taşır; AdSet semantiği generic AdGroup'a dönüştürülmez.
- Meta aynı ortak envelope validator'ından geçer; eksik/özel paralel shape kabul edilmez.
- Wrong user/account write ownership guard ile reddedilir.
- Retry duplicate üretmez.
- Metric support ve gerçek `0`/`null` semantiği korunur.
- Aynı dönem provider raw, V2 ve Formula output kabul eşiğinde reconciled olur.
- Legacy snapshot sonucu dual-write nedeniyle değişmez.

### Test planı

- Golden fixtures, Campaign/AdSet/Ad lineage, deterministic key ve mapping unit testleri.
- Timezone/DST ve currency/FX testleri.
- Missing/partial metric support testleri.
- Repository integration ve idempotent retry.
- Ownership negatif testi.
- Dual-write legacy no-change ve parity raporu.

### Rollback planı

- `meta_v2_write` provider/account feature flag'i kapatılır.
- V1 snapshot read/write korunur.
- V2 yazıları run/adapter version ile izlenir; hatalı batch hedefli temizlenir.
- Provider fetch değişmeden tutulur; yeni adapter delegation geri alınabilir.

### Bağımlılıklar

- E2 ve E3 `Done`.
- Meta conversion mapping kararı.
- Parity eşiği ve canary account listesi.

### Evidence

- Mapping matrix, fixture sonuçları, dual-write run, parity raporu, rejection/error metrics.

## 9. E5 — Google Standard ve PMax adapter

**Durum:** `Done` — E5-T1–T7 tamamlandı; Google canlı V2-primary Refresh, Meta ile aynı zero-row/no-error sonucu ve kalıcı redacted V2 evidence ile kabul edildi.

### Planlanan işler

- **E5-T1 — `Done`:** Conversion action count/value mapping ve provenance.
- **E5-T2 — `Done`:** Gerçek customer currency/timezone.
- **E5-T3 — `Done`:** Standard Campaign→AdGroup→Ad adapter.
- **E5-T4 — `Done`:** PMax Campaign→Asset Group adapter; fake AdGroup/Ad yasağı.
- **E5-T4A — `Done`:** Standard ve PMax output'larını aynı yedi bloklu envelope'a normalize et; farkı yalnız capability/entity değerlerinde koru.
- **E5-T5 — `Done`:** Time/FX/V2/job/telemetry entegrasyonu.
- **E5-T6 — `Done`:** V2-primary koordinasyon; Standard/PMax ayrı completeness; V1 write/fallback yok.
- **E5-T7 — `Done`:** Manuel Google Refresh business-date Standard/PMax sorgularıyla doğrudan V2'ye bağlandı; Meta ile aynı canlı zero-row/no-error kabulü doğrulandı.

#### E5-T1 task aynası — Google conversion count/value ve provenance

**Amaç:** Google conversion action breakdown içindeki ATC, Checkout ve Purchase count/value değerlerini geniş isim tahmini yapmadan versionlı ve açıklanabilir bir sözleşmeye bağlamak.

**İş kararı:** `ADD_TO_CART`, `BEGIN_CHECKOUT` ve `PURCHASE` kategorileri birincildir. Kategori bulunmazsa yalnız kapalı listedeki exact action adı fallback olabilir; `cart`, `order`, `sale` gibi substring eşleşmeleri ve generic `metrics.conversions_value` purchase yerine kullanılamaz. Aynı kategorideki farklı conversion action kayıtları count/value birlikte toplanır.

**Kapsam:** Sentetik conversion fixture, versionlı mapping kuralları, count/value birlikte seçim, category-first/exact-name fallback, redacted provenance, null/zero ve negatif değer kontrolleri.

**Kapsam dışı:** Production Google API çağrısı, customer/timezone/currency, Standard/PMax hierarchy, Dataset V2 write, runtime/deployment ve dual-write.

**Kabul kriterleri:** Lead purchase olmaz; category exact-name fallback'ten önce gelir; geniş isim eşleşmesi yoktur; missing `null`, measured zero `0`; provenance ham action adı/resource/customer/value taşımaz; full/security/architecture/canonical CI PASS.

**Evidence:** `src/providers/google/conversion-mapping.js`, `artifacts/e5-google/e5-t1-conversion-fixture.json`, `docs/E5_T1_GOOGLE_CONVERSION_CHARACTERIZATION.md`, `tests/e5-t1-google-conversion-mapping.test.js`.

**Durum:** `Done` — PR #110, CI ve insan merge kabulü tamamlandı.

#### E5-T2 task aynası — Google customer currency, timezone ve business date

**Amaç:** Google rapor gününü ve kaynak para birimini browser/server varsayımından değil, seçili Google Ads customer metadata'sından güvenilir biçimde üretmek.

**Sözleşme:** Exact provider sorgusu yalnız `customer.id`, `customer.currency_code` ve `customer.time_zone` ister. Dönen customer requested customer ile aynı olmalı; currency ISO-3, timezone IANA olmalı; business date bu timezone içindeki gözlem günüdür. Eksik metadata UTC/default fallback ile devam etmez.

**Kapsam:** Metadata query sözleşmesi, camel/snake provider shape, identity parity, currency/timezone validation, timezone-crossing business date ve identity-free evidence.

**Kapsam dışı:** Production Google API çağrısı, OAuth/customer seçimi değişikliği, conversion mapping entegrasyonu, Standard/PMax adapter, Dataset V2 write, runtime/deployment ve dual-write.

**Kabul kriterleri:** Wrong customer fail-closed; invalid/missing currency/timezone fail-closed; UTC/server/browser fallback yok; DST-capable IANA timezone; evidence customer identity ve raw response taşımaz; full/security/architecture/canonical CI PASS.

**Evidence:** `src/providers/google/account-metadata.js`, `artifacts/e5-google/e5-t2-customer-metadata-fixture.json`, `docs/E5_T2_GOOGLE_CUSTOMER_TIME_CURRENCY.md`, `tests/e5-t2-google-customer-metadata.test.js`.

**Durum:** `Done` — PR #111, CI ve insan merge kabulü tamamlandı.

#### E5-T3 task aynası — Google Standard Campaign→AdGroup→Ad adapter

**Amaç:** Google Standard reklam performansını Campaign ve AdGroup bağlamını kaybetmeden tek Ad leaf canonical satırına çevirmek ve hierarchy-level double count riskini kaldırmak.

**Sözleşme:** Yalnız Ad leaf fact üretilir; Campaign root, AdGroup parent, Ad leaf olarak taşınır. Google AdGroup semantiği Meta AdSet'e çevrilmez. PMax açıkça reddedilir. E5-T1 conversion ve E5-T2 customer metadata aynı yedi bloklu envelope içinde birleşir.

**Kapsam:** Standard mapper/adapter, deterministic entity key, ten raw facts, metric support, conversion provenance, same-currency baseline, business-date parity, wrong date/PMax/missing hierarchy negatifleri.

**Kapsam dışı:** Google API client/runtime, production request/write, PMax, cross-currency FX provider, Dataset V2 writer/job/retry, dual-write ve UI.

**Kabul kriterleri:** Campaign→AdGroup→Ad exact; yalnız Ad leaf fact; PMax reddedilir; missing `null+unknown`, session `null+unsupported`, real zero korunur; provider DTO adapter dışına çıkmaz; canonical/hierarchy/key validation PASS; full/security/architecture/canonical CI PASS.

**Evidence:** `src/providers/google/standard-mapper.js`, `src/providers/google/standard-adapter.js`, `artifacts/e5-google/e5-t3-standard-ad-fixture.json`, `docs/E5_T3_GOOGLE_STANDARD_ADAPTER.md`, `tests/e5-t3-google-standard-adapter.test.js`.

**Durum:** `Done` — PR #112, CI ve insan merge kabulü tamamlandı.

#### E5-T4 task aynası — Google PMax Campaign→Asset Group adapter

**Amaç:** Performance Max performansını Standard AdGroup/Ad hiyerarşisine zorlamadan Campaign→Asset Group canonical yapısına çevirmek ve sahte entity üretimini engellemek.

**Sözleşme:** `campaign_type=performance_max`; Campaign root, Asset Group leaf; parent alanları null. AdGroup/Ad varlığı fail-closed olur. Standard channel bu adapter'a giremez. PMax ve Standard aynı yedi bloklu envelope/raw metric/support anahtarlarını kullanır; fark yalnız campaign/entity değerleridir.

**Kapsam:** PMax mapper/adapter, Asset Group deterministic key, E5-T1 conversion, E5-T2 metadata, ten facts/support, Standard/PMax envelope parity ve negative hierarchy/date/channel testleri.

**Kapsam dışı:** Production Google API/runtime, Dataset V2 write, cross-currency provider, job/retry/dual-write, E5-T4A kapanışı ve UI.

**Kabul kriterleri:** Fake AdGroup/Ad yok; parent null; only Asset Group leaf; Standard reddedilir; missing/unsupported/zero semantiği korunur; provider DTO dışarı çıkmaz; Standard ile aynı envelope; canonical/hierarchy/key ve full/security/architecture/canonical CI PASS.

**Evidence:** `src/providers/google/pmax-mapper.js`, `src/providers/google/pmax-adapter.js`, `artifacts/e5-google/e5-t4-pmax-asset-group-fixture.json`, `docs/E5_T4_GOOGLE_PMAX_ADAPTER.md`, `tests/e5-t4-google-pmax-adapter.test.js`.

**Durum:** `Done` — PR #113, CI ve insan merge kabulü tamamlandı.

#### E5-T4A task aynası — Google Standard/PMax ortak adapter ve envelope closeout

**Amaç:** Standard ve PMax yollarını tek Google provider girişinde doğru mapper'a yönlendirmek ve iki campaign modelinin ayrı canonical şemalara ayrılmadığını merkezi olarak kanıtlamak.

**Sözleşme:** Caller yalnız `standard|performance_max` seçebilir. Standard fetch yalnız Standard mapper'a, PMax fetch yalnız PMax mapper'a bir kez delege edilir. Unknown/missing type provider fetch öncesi reddedilir. Capability farkı root/parent/leaf ve campaign type değerleriyle sınırlıdır.

**Kapsam:** Unified adapter, immutable capability matrix, exact delegation, shared seven-block/raw metric/support/currency/time/provenance key parity, DTO boundary ve unknown-type negatifleri.

**Kapsam dışı:** Production Google client/API/runtime, Dataset V2 write, Time/FX live binding, job/retry/dual-write ve UI.

**Kabul kriterleri:** Tek provider adapter girişi; exact one-path delegation; unknown no-fetch; Standard/PMax aynı envelope; fark yalnız capability/entity değerleri; provider DTO dışarı çıkmaz; full/security/architecture/canonical CI PASS.

**Evidence:** `src/providers/google/adapter.js`, `src/providers/google/capabilities.js`, `docs/E5_T4A_GOOGLE_UNIFIED_ENVELOPE.md`, `tests/e5-t4a-google-unified-adapter.test.js`.

**Durum:** `Done` — PR #114, CI ve insan merge kabulü tamamlandı.

#### E5-T5 task aynası — Google Time/FX/V2/job/telemetry entegrasyonu

**Amaç:** Google customer metadata, business date, unified adapter, günlük FX, canonical validation, Dataset V2 write ve refresh job evidence adımlarını tek fail-closed zincirde birleştirmek.

**Sözleşme:** Job exact user/customer/type ile açılır; customer metadata aynı işte doğrulanır; adapter yalnız seçilen type'ı map eder; dört parasal fact business-date FX ile bir kez çevrilir; ownership/hierarchy/entity key yeniden doğrulanır; tüm satırlar ortak write boundary'ye verilir; cardinality sapması başarı sayılmaz.

**Kapsam:** Google dataset writer, refresh runner, Standard path entegrasyon fixture'ı, cross-currency dönüşüm, same-currency guard, source job lineage, zero-row/fake-free sonuç ve redacted completion evidence.

**Kapsam dışı:** Production Google API/runtime route, real token/customer request, scheduler, retry policy/dual-write, feature flag, UI ve canlı V2 write.

**Kabul kriterleri:** Exact customer/date/type; four monetary facts once-only FX; counts unchanged; source job persisted; ownership/key/cardinality fail-closed; empty result fake-free; evidence identity/token/raw value taşımaz; full/security/architecture/canonical CI PASS.

**Evidence:** `src/providers/google/dataset-writer.js`, `src/providers/google/refresh-runner.js`, `docs/E5_T5_GOOGLE_V2_REFRESH.md`, `tests/e5-t5-google-v2-refresh.test.js`.

**Durum:** `Done` — PR #115, CI ve insan merge kabulü tamamlandı.

#### E5-T6 task aynası — Google V2-primary koordinasyon ve completeness

**Amaç:** Google Standard ve PMax refresh sonuçlarını, kullanım değeri kalmayan Dataset V1/Dashboard V1 yoluna uğratmadan doğrudan Dataset V2 hedefinde tamamlamak.

**İş kararı:** Google yolu Meta ile aynı V2-primary modelini izler. V1 satırı yazılmaz ve V2 hatasında V1'e sessiz fallback yapılmaz. Standard ile PMax ayrı branch olarak çalışır; gerçek zero-row sonuç geçerlidir ve sahte satır üretilmez.

**Kapsam:** Standard→PMax exact branch sırası, branch başına `attempted == persisted` completeness, birleşik V2 sayaçları, zero-row kanıtı, V1 yazılmadığını gösteren allowlisted ve identity-free evidence.

**Kapsam dışı:** Production route/API, canlı Google çağrısı veya V2 write, runtime gate, deployment, scheduler ve canlı kabul. Bunlar ayrı PR ve açık production onayı gerektirir.

**Kabul kriterleri:** V1 callback/fallback yok; iki branch de ayrı completeness verir; terminal Standard hatasından sonra PMax başlatılmaz; count veya empty-result tutarsızlığı fail-closed olur; zero-row fake-free kabul edilir; evidence customer/entity/token/raw payload/metrik değeri taşımaz; full/security/architecture/canonical CI PASS.

**Evidence:** `src/providers/google/v2-primary.js`, `docs/E5_T6_GOOGLE_V2_PRIMARY.md`, `tests/e5-t6-google-v2-primary.test.js`.

**Durum:** `Done` — PR #116, CI ve insan merge kabulü tamamlandı.

#### E5-T7 task aynası — Google canlı V2-primary runtime

**Amaç:** Kullanıcının manuel Google Refresh işlemini V1 snapshot üretmeden Standard ve PMax verisini doğrudan Dataset V2'ye yazan production-capable runtime'a bağlamak.

**Sözleşme:** Customer metadata aynı Google hesabından alınır; customer timezone'ındaki tek business date sorgulanır; Standard Ad ve PMax Asset Group ayrı map/write edilir; branch completeness birleşik redacted evidence ile döner. V2 hatasında V1 fallback ve V1 Google Sheets sync yoktur.

**Kapsam:** Default-on explicit runtime gate, canlı Google search client binding, exact business-date Standard/PMax performance ve conversion sorguları, canonical write boundary, response/job evidence, zero-row ve V1 no-write wiring testleri.

**Kapsam dışı:** Bu PR sırasında canlı refresh çalıştırmak, kullanıcı adına UI aksiyonu almak, backfill ve scheduler dönüşümü. Canlı manuel refresh merge/Vercel sonrası kullanıcı tarafından yapılır.

**Kabul kriterleri:** Refresh date-range isteğinden bağımsız tek customer business date; Standard/PMax direct V2; `attempted == persisted`; zero-row fake-free; V1 snapshot/write/fallback ve stale V1 Sheets sync yok; identity/raw-provider-free evidence; full/security/architecture/canonical CI PASS.

**Evidence:** `src/providers/google/live-refresh.js`, `server.js`, `.env.example`, `docs/E5_T7_GOOGLE_LIVE_V2_RUNTIME.md`, `tests/e5-live-google-v2-primary.test.js`, `tests/e5-live-google-runtime-wiring.test.js`.

**Durum:** `Done` — PR #117–#125 corrective zinciri merge edildi ve `main` kontrolleri başarılıdır. PR #123 ile eklenen güvenli aşama kanıtı önce object/positional search imza uyuşmazlığını, ardından geçerli zero-row ProtoJSON cevabındaki eksik `results` alanını gerçek sınırlarında gösterdi. PR #124 imzayı uyarladı; PR #125 eksik `results` alanını zero-row kabul ederken mevcut fakat dizi olmayan alanı fail-closed bıraktı. Kullanıcının 2026-08-31 tarihli manuel Refresh'i `completed`, hata mesajı ve failure stage olmadan, kalıcı `google_v2_evidence` ile sonuçlandı. E5 canlı kabulü tamamlandı; E6 artık uygulanabilir sıradaki provider epic'idir.

### Kabul kriterleri

- Conversion mapping explicit ve versionlıdır.
- Standard hierarchy provider ile reconciled olur.
- PMax satırı yalnız desteklenen Asset Group capability'sini taşır.
- Standard ve PMax ayrı canonical şema üretmez; aynı envelope ve validator'ı kullanır.
- Unsupported alanlar `null + metric_support` kalır.
- Retry/idempotency ve ownership testleri geçer; Google V2-primary yolunda legacy write/fallback bulunmaz.

### Test planı

Golden fixtures, conversion mapping, Standard/PMax hierarchy, timezone/FX, ownership, retry, V2 completeness ve provider→canonical→V2 parity testleri.

### Rollback planı

Production activation ayrı ve default-off gate ile yapılır; rollback gate'i kapatır, V1 fallback başlatmaz; idempotent V2 upsert aynı iş günü için güvenli yeniden çalıştırılır.

### Bağımlılıklar

E4 referans slice kabulü; Google conversion action ve PMax reporting kararları.

## 10. E6 — TikTok adapter

**Durum:** `Parked` — 20 Eylül 2026 ürün kararıyla Shopify ilk aktif provider diliminden çıkarıldı. Tamamlanmış E6 artefaktları ve tarihsel evidence korunur; yeni OAuth/reconnect, refresh/shadow, provider teması ve production activation yapılmaz. Yeniden açılış ayrı açık kullanıcı kararı ve güncel resmi revalidation gerektirir.

### Planlanan işler

- **E6-T1 — `Done`:** Resmî TikTok Business API SDK commit'iyle v1.3 synchronous BASIC `AUCTION_AD` report yüzeyi, Ad-leaf additive grain, delivery metrics ve fail-closed event kuralları PR #127 ve başarılı CI ile donduruldu.
- **E6-T2 — `Done`:** OAuth/sandbox sınırı, Preview auth düzeltmesi ve zero-row characterization PR #132 ile merge edildi. İnsan kararıyla delivery-only ilerleme seçildi.
- **E6-T3 — `Deferred evidence gate`:** Dokuz ATC/Checkout/Purchase count/value adayı provider tarafından kabul edildi ancak sandbox tek günlük sorgusu zero-row döndü. Event alanları `unknown` ve sonraki delivery adapter'da `unsupported/null` kalacak; non-empty kanıt ayrı gate olarak açık kalır.
- **E6-T4 — `Done`:** PR #133 ile production fact yalnız `AUCTION_AD` leaf'ten üretilir; duplicate business-date/entity-key batch'i fail-closed reddedilir.
- **E6-T4A — `Done`:** PR #133 ile zorunlu `Campaign → AdGroup → Ad` lineage ve deterministic entity key delivery mapper'da uygulanmıştır.
- **E6-T4B — `Done`:** PR #133 ile TikTok delivery output'u yedi bloklu canonical envelope'a normalize edilir; event facts `unsupported/null`, eksik delivery facts `unknown/null` kalır.
- **E6-T5 — `Done`:** PR #134 ile legacy fallback marker'ları canonical mapper öncesinde izole edilir; synthetic-only input boş canonical sonuç ve `synthetic_written_to_canonical=0` evidence üretir.
- **E6-T6A — `Done`:** PR #135 ile advertiser timezone/currency metadata'sı, provider business date ve fail-closed same/cross-currency FX delivery mapper'a bağlanmıştır.
- **E6-T6B1 — `Done`:** PR #136 ile Dataset V2 writer ownership/entity/synthetic/cardinality guard'ları ve safe failure stage'leriyle canonical boundary'ye bağlanmıştır.
- **E6-T6B2 — `Done`:** PR #137 ile advertiser metadata, delivery-only AUCTION_AD read, Dataset V2 writer ve refresh job evidence injectable runner'da compose edilmiştir.
- **E6-T6C1 — `Done`:** PR #138 ile legacy ve canonical V2 Ad nüfusları entity-level spend/impressions/clicks, event-null ve synthetic policy için redacted parity evidence ile karşılaştırılır.
- **E6-T6C2 — `Done`:** PR #139 ile legacy-authoritative write, V2 shadow runner ve parity fail-isolated/no-change coordinator'da compose edilmiştir.
- **E6-T6D1 — `Verification / BLOCKED`:** Kod/main hazır; shadow rollout onayı, runtime registration ve üç clean live parity sonucu yoktur. Primary activation ayrı açık onay gerektirir.
- **E6-T6D2 — `Implementation / live evidence pending`:** İnsan onaylı, default-off TikTok shadow runtime legacy-authoritative refresh'e kaydedildi. Production OAuth'ın sandbox advertiser listeleyemediği doğrulandı; explicit server-only review bridge account picker ve sandbox report routing'e bağlandı. Üç ardışık deployed live `PASS` henüz toplanmadı.
- **E6-T6D3:** Ayrı primary production activation kararı.

#### E6-T1 task aynası — TikTok production report contract

**Amaç:** Provider mapping başlamadan önce TikTok production rapor yüzeyini resmî, pinlenmiş kaynakla sınırlandırmak ve legacy generic conversion→purchase yorumunu yasaklamak.

**Sözleşme:** `/open_api/v1.3/report/integrated/get/`, `GET`, `BASIC`, `AUCTION_AD`, `ad_id`; doğrulanmış delivery alanları `spend`, `impressions`, `clicks`. Production fact yalnız Ad leaf'te additive olur; Campaign ve AdGroup lineage'dır. Event count/value alanları gerçek advertiser karakterizasyonuna kadar `unknown` kalır ve eksik değer sıfır değildir.

**Kapsam dışı:** Canlı TikTok isteği, event mapping, canonical mapper, Dataset V2 write, synthetic temizliği ve production activation.

**Evidence:** `src/providers/tiktok/report-contract.js`, `artifacts/e6-tiktok/e6-t1-report-contract-fixture.json`, `docs/E6_T1_TIKTOK_REPORT_CONTRACT.md`, `tests/e6-t1-tiktok-report-contract.test.js`.

**Durum:** `Done` — resmî TikTok SDK repository commit'i `f809c396520df2d7b201a9ccc5378d822b728ed3` pinlendi. SDK endpoint/report type/data-level sözleşmesini doğrular; event metric isimlerini kapalı enum olarak yayımlamadığı için ATC/Checkout/Purchase hakkında tahmin yapılmadı. PR #127 merge edildi ve kontroller başarılıdır.

#### E6-T2 hazırlık aynası — TikTok OAuth advertiser discovery

**Kesin kanıt:** OAuth sonrası account picker, connected OAuth token ile resmî production host'taki `/v1.3/oauth2/advertiser/get/` endpoint'ini sorgular. Redacted canlı connection metadata da token kaynağını `platform_connections.access_token`, report base'i `https://business-api.tiktok.com/open_api` olarak doğruladı. Liste sandbox'tan değil OAuth advertiser discovery'den boş dönmüştür.

**Sandbox kararı:** İnsan iş kararıyla ayrı non-production sandbox akışı onaylandı. Sandbox ayrı host ve server-held token kullanır; OAuth token sandbox host'una taşınmaz. Yalnız preview/development ortamında iki explicit switch, server-held token ve advertiser ID birlikte mevcutsa account picker sandbox advertiser'ı döndürür. Production guard değişkenlerin tamamını reddetmeye devam eder.

**UI corrective:** Başarılı fakat boş advertiser listesi modalı açıldığında reconnect URL hemen tüketilir; `Close` aynı modalı tekrar açmaz.

**Evidence:** `docs/E6_T2_TIKTOK_ACCOUNT_DISCOVERY_AUDIT.md`, `tests/e6-t2-tiktok-account-selection.test.js`, `public/dashboard.html` ve iki korunmuş dashboard patch'i.

**Durum:** `Verification / Merge approval` — PR #128 modal corrective ve PR #129 sandbox source merge edildi. PR #130 Preview doğrulaması başarısız oldu ve merge edilmeden kapatıldı. PR #132 auth kapısını düzeltti ve read-only characterization'ı tamamladı. Dokuz aday metric sorguda kabul edildi fakat sonuç zero-row olduğundan field/value semantiği kanıtlanmadı. İnsan kararıyla delivery-only ilerleme seçildi: sonraki adapter yalnız `spend/impressions/clicks` map edecek; event alanları non-empty kanıta kadar `unsupported/null` kalacak. Redacted evidence repository'ye alındı ve geçici route/flag/UI düğmesi kaldırıldı. PR #132 merge onayı bekler.

**Geçici corrective sonucu:** İnsan onayıyla ayrı database kurulmadan read-only characterization çalıştırıldı. Endpoint hiçbir connection/ownership/job/snapshot/Dataset write yapmadı ve yalnız safe field-presence evidence döndürdü. Çalışma zero-row olduğu için event semantiği fail-closed biçimde kabul edilmedi; geçici endpoint kanıt alındıktan sonra kaldırıldı.

#### E6-T4 + E6-T4A + E6-T4B task aynası — delivery-only adapter

**Amaç:** İnsan onaylı delivery-only kararıyla TikTok `spend/impressions/clicks` alanlarını yalnız Ad leaf'te canonical fact'a dönüştürmek; Campaign/AdGroup'u lineage olarak korumak ve hierarchy toplamlarının iki kez sayılmasını engellemek.

**Kapsam:** `src/providers/tiktok/delivery-mapper.js`, deterministic entity key, yedi canonical blok, duplicate leaf guard, sentetik/negatif/non-leaf rejection ve delivery-only metric support.

**Kapsam dışı:** Runtime fetch wiring, Dataset V2 write, FX conversion, production activation ve event semantic mapping.

**Durum:** `Done` — fixture ve beş executable test Campaign→AdGroup→Ad lineage'ını, yalnız Ad-leaf additive fact'ı, generic conversion ignore kuralını, event `unsupported/null` davranışını, missing-is-not-zero kuralını ve duplicate double-count rejection'ı doğruladı. PR #133 full/security/architecture/canonical kontrolleri ve insan onayıyla merge edildi; main Security regression başarılıdır.

#### E6-T5 task aynası — synthetic fallback isolation

**Amaç:** Legacy boş-rapor fallback satırlarını dashboard uyumluluğundan silmeden canonical production adapter girişinden ayırmak; sentetik sıfırların gerçek performance gibi yazılmasını engellemek.

**Kapsam:** Explicit synthetic provenance, fallback reason/source-confidence ve fallback kimlik/status marker'larının izolasyonu; safe count evidence; yalnız production satırlarının E6-T4 mapper'a aktarılması.

**Kapsam dışı:** Legacy dashboard snapshot davranışını kaldırmak, Dataset V2 write, runtime flag activation, FX ve production rollout.

**Durum:** `Done` — beş executable test karışık input'ta yalnız gerçek Ad leaf'in map edildiğini, tüm fallback marker ailelerinin izole edildiğini, synthetic-only input'un boş canonical sonuç verdiğini ve `synthetic_written_to_canonical=0` invariant'ını doğruladı. PR #134 insan onayıyla merge edildi ve main Security regression başarılıdır.

#### E6-T6A task aynası — Time/FX binding

**Amaç:** Advertiser metadata'sındaki timezone/source currency ile provider daily date'i canonical time'a bağlamak ve supported delivery monetary fact'ini onaylı FX oranıyla tam bir kez normalize etmek.

**Kapsam:** Advertiser identity binding, IANA timezone, daily business date, ISO currency, same/cross-currency rate kuralları, synthetic isolation ve normalized duplicate guard.

**Kapsam dışı:** Dataset V2 write/runtime wiring, dual-write, parity ve production activation.

**Durum:** `Done` — beş executable test same-currency metadata'yı, cross-currency spend dönüşümünü, event value `unsupported/null` korunmasını, invalid identity/timezone/date/currency/rate rejection'ını ve isolation/dedup invariant'ını doğruladı. PR #135 insan onayıyla merge edildi ve main Security regression başarılıdır.

#### E6-T6B1 task aynası — Dataset V2 writer boundary

**Amaç:** TikTok delivery-only canonical satırlarını FX resolver ve canonical write boundary üzerinden Dataset V2'ye hazır hale getirmek; write öncesi ownership, entity key, synthetic ve cardinality invariant'larını doğrulamak.

**Kapsam:** FX lookup, E6-T6A normalization, E6-T5 isolation, canonical validation, write cardinality ve allowlisted safe failure stage'leri.

**Kapsam dışı:** Express/live refresh route composition, dual-write, parity ve production activation.

**Durum:** `Done` — beş executable test gerçek delivery write'ını, zero-row/synthetic-only boş write'ı, ownership/cardinality fail-closed davranışını, safe stage sınıflandırmasını ve redacted count evidence'ını doğruladı. PR #136 insan onayıyla merge edildi ve main Security regression başarılıdır.

#### E6-T6B2 task aynası — live refresh job composition

**Amaç:** Advertiser metadata, yalnız AUCTION_AD delivery read, E6-T6B1 writer ve ortak refresh job boundary'yi kimlik/metric sızdırmayan evidence ile compose etmek.

**Kapsam:** Delivery-only provider request, omitted zero-row kabulü, malformed response/identity rejection, source job binding, redacted completed metadata ve safe provider stage.

**Kapsam dışı:** Express route/production flag activation, legacy dual-write ve parity kabulü.

**Durum:** `Done` — altı executable test normal V2 composition'ı, omitted zero-row'u, malformed/identity/date fail-closed davranışını, provider safe stage'ini, event/synthetic zero-write evidence invariant'larını ve writer count/cardinality doğrulamasını kapsadı. PR #137 insan onayıyla merge edildi ve main Security regression başarılıdır.

#### E6-T6C1 task aynası — delivery parity evaluator

**Amaç:** Legacy ve V2 TikTok Ad satırlarını kimlik bazında karşılaştırarak delivery facts, event-null ve synthetic isolation parity'sini değer/kimlik sızdırmadan kanıtlamak.

**Kapsam:** Entity-set parity, per-Ad spend/impressions/clicks parity, legacy synthetic isolation count, canonical event/synthetic policy ve redacted assertion evidence.

**Kapsam dışı:** Write, server dual-write composition ve production activation.

**Durum:** `Done` — beş executable test PASS evidence, metric drift, synthetic placeholder isolation, entity/event-policy mismatch, duplicate/malformed rejection ve input no-change davranışını doğruladı. PR #138 insan onayıyla merge edildi ve main Security regression başarılıdır.

#### E6-T6C2 task aynası — shadow dual-write no-change composition

**Amaç:** Legacy write'ı otoriter tutarak V2 runner ve parity'yi shadow modda çalıştırmak; V2/parity başarısızlığını legacy sonuçtan izole etmek ve production activation'ı kapalı tutmak.

**Kapsam:** Legacy-first order, V2 shadow failure isolation, allowlisted stage, parity evidence, request no-mutation ve `production_activation=false` invariant'ı.

**Kapsam dışı:** Express route registration ve production activation.

**Durum:** `Done` — beş executable test PASS shadow akışını, V2 failure isolation'ını, parity drift görünürlüğünü, legacy failure short-circuit'ini, request isolation ve redacted stage davranışını doğruladı. PR #139 insan onayıyla merge edildi ve main Security regression başarılıdır.

#### E6-T6D1 task aynası — production activation readiness gate

**Amaç:** Kod/main, shadow rollout, live parity ve primary approval kapılarını birbirinden ayırmak; hiçbir değerlendirme fonksiyonunun production activation çalıştırmamasını garanti etmek.

**Kapsam:** Yedi code gate, main/rollback readiness, ayrı shadow onayı, runtime registration, minimum üç clean live parity, zero synthetic write ve ayrı primary approval reason-code evidence'ı.

**Kapsam dışı:** Shadow route registration, live write/parity çalıştırma ve primary production activation.

**Durum:** `Verification / BLOCKED` — committed evidence shadow onayı/runtime/live parity/primary approval eksiklerini gösterir ve `production_activation_performed=false` taşır. Altı executable test onay ayrımını, minimum parity eşiğini, invalid counters/code gates fail-closed davranışını ve redacted artifact'ı doğrular.

#### E6-T6D2 operasyon aynası — production review bridge ve live shadow evidence

**Doğrulanan engel:** `dev.adstable.app` Vercel Production deployment'ı TikTok production OAuth tokenıyla `/v1.3/oauth2/advertiser/get/` çağrısı yapar. Bu kimlik sandbox advertiser'ı listeleyemediği için başarılı OAuth sonrasında provider `data.list=[]` döndürür ve account-selection modalı `No accessible account was found.` gösterir. Preview-scoped `TIKTOK_SANDBOX_*` değerleri Production deployment'a taşınmaz; ayrıca genel sandbox değişkenleri production startup guard tarafından bilinçli olarak reddedilir.

**Geçici çözümün amacı:** TikTok review süresince yalnız önceden tanımlanmış tek sandbox advertiser'ı, tokenı browser'a veya API cevabına koymadan mevcut account-selection ve legacy-authoritative refresh yaşam döngüsüne bağlamak. Bu bridge genel müşteri sandbox desteği, OAuth bypass veya TikTok V2-primary activation değildir.

**Açma sözleşmesi:** Production review bridge yeni token/advertiser environment adları üretmez; mevcut sandbox kimliğini aşağıdaki üç server-side Vercel Production değişkeniyle kullanır:

- `TIKTOK_REVIEW_FALLBACK_ENABLED=true`
- `TIKTOK_SANDBOX_ACCESS_TOKEN=<server-only sandbox token>`
- `TIKTOK_SANDBOX_ADVERTISER_ID=<approved sandbox advertiser id>`
- `TIKTOK_SANDBOX_ADVERTISER_NAME=<optional display label>`

Flag, token ve advertiser ID üçlüsü eksikse yalnız review bridge fail-closed olarak devre dışı kalır; optional TikTok review konfigürasyonu `/login`, `/api/public-config` veya diğer platformları durduramaz. Eski `TIKTOK_SANDBOX_ENABLED`, `TIKTOK_SANDBOX_ACCESS_TOKEN`, `TIKTOK_SANDBOX_ADVERTISER_*`, `TIKTOK_TEST_ACCESS_TOKEN` ve `TIKTOK_FORCE_SANDBOX_REPORTS` Production environment'ta kalsa bile runtime bunları karantinaya alır: sandbox mode, forced reports ve test page kesin olarak kapalı kalır. Böylece yanlış/yarım TikTok env geçişi uygulamanın auth yüzeyini düşürmeden ilgili özelliği fail-closed tutar.

**Request ve seçim akışı:** OAuth callback önce normal production tokenını server-side connection'a kaydeder ve explicit account selection ister. `/api/tiktok/advertisers` önce normal production OAuth discovery çağrısını yapar. Gerçek advertiser listesi non-empty ise aynen onu döndürür ve bridge devreye girmez. Liste empty ise ve review sözleşmesi eksiksizse yalnız configured review advertiser; `sandbox=true`, `reportBase=https://sandbox-ads.tiktok.com/open_api` ve `tokenSource=server_review_access_token` routing metadata'sıyla döner. Access token hiçbir response alanında bulunmaz.

**Persistence ve refresh akışı:** Kullanıcı review advertiser'ı seçtiğinde mevcut `/api/accounts/select` lifecycle'ı ownership, platform account, schedule ve connection metadata'sını yazar. Sonraki TikTok refresh, persisted `tokenSource` ve `reportBase` değerlerinin ikisini de doğrular. Eşleşme varsa server-held mevcut `TIKTOK_SANDBOX_ACCESS_TOKEN` ile sandbox report host'una gider; eşleşme yoksa normal connected OAuth tokenı ve production API host'u kullanılmaya devam eder. Caller query/body/header ile sandbox token veya routing seçemez.

**Shadow sınırı:** `TIKTOK_V2_SHADOW_ENABLED=true` ayrıca açık olduğunda legacy snapshot önce ve otoriter olarak yazılır. Aynı provider-derived gerçek Ad-leaf nüfusu Dataset V2 shadow writer'a geçer; Campaign/AdGroup toplamları ve synthetic fallback'ler canonical fact olamaz. Shadow/parity hatası legacy sonucu başarısız yapmaz. Response ve job metadata yalnız redacted `tiktok_shadow_evidence` taşır; `production_activation=false` kalır.

**Production operasyon sırası:** (1) Env-sadeleştirme PR'ı merge edilir. (2) Vercel Production'da mevcut sandbox token/advertiser değişkenlerinin scope'u doğrulanır ve yalnız `TIKTOK_REVIEW_FALLBACK_ENABLED=true` eklenir. (3) Production redeploy yapılır. (4) TikTok reconnect/account selection tekrar çalıştırılır ve configured sandbox advertiser seçilir. (5) `TIKTOK_V2_SHADOW_ENABLED=true` ile en az üç ardışık manual refresh çalıştırılır. (6) Her koşuda legacy başarı, shadow `PASS`, entity/delivery parity true ve `synthetic_written_to_canonical=0` doğrulanır. Bu altı adım tamamlanmadan E6-T6D2 `Done` olmaz.

**Kabul kriterleri:** Empty OAuth listesi yalnız eksiksiz review config ile tek allowlisted advertiser üretir; normal non-empty OAuth listesi override edilmez; token hiçbir browser cevabına/log/evidence'a girmez; seçilen routing metadata server-side persist edilir; refresh sandbox host ve review tokenını birlikte kullanır; legacy otoritesi korunur; üç ardışık live parity `PASS` ve zero synthetic write kanıtı alınır.

**Rollback:** Önce `TIKTOK_V2_SHADOW_ENABLED=false` yapılarak shadow write durdurulur. Review erişimini kapatmak için `TIKTOK_REVIEW_FALLBACK_ENABLED=false` yapılır; token/id/name daha sonra kaldırılabilir ve eksik/geçiş hâli uygulama startup'ını durdurmaz. Redeploy sonrası production OAuth discovery ve legacy refresh normal token/host yoluna döner. Mevcut legacy snapshot'lar silinmez; Dataset V2 shadow satırları primary read kaynağı olmadığından dashboard otoritesini etkilemez.

**Geçicilik ve kaldırma kriteri:** TikTok App Review tamamlanıp production OAuth gerçek advertiser'ı non-empty listelediğinde bridge kullanılmaz; kalıcı kaldırma PR'ında review flag/token/id/name, sandbox routing dalı ve bu geçici runbook birlikte silinir. Primary activation yalnız üç clean live evidence incelendikten sonra E6-T6D3'te ayrıca insan onayıyla kararlaştırılır.

**Evidence:** `docs/E6_T6D2_TIKTOK_REVIEW_BRIDGE.md`, `docs/E6_T6D2_TIKTOK_LIVE_SHADOW.md`, `server.js`, `security/production-config.js`, `tests/e6-t2-tiktok-account-selection.test.js`, `tests/production-config.test.js`, `tests/e6-t6d2-tiktok-live-shadow.test.js`.

**E6 kapanış sadeleştirmesi (2026-09-07):** Review advertiser discovery ve manual refresh production üzerinde doğrulandı. İlk live koşu teknik olarak tamamlandı fakat campaign/adgroup raporları boş, Ad raporu TikTok `40100` QPS limitli ve legacy sonuç üç sentetik fallback entity idi; canonical V2'ye sentetik satır yazılmadı. Bu nedenle zero-to-zero parity artık `PASS` sayılmaz. Report level çağrıları 1 QPS sınırına göre en az 1100 ms aralıklı çalışır, `40100`/429 bounded backoff ile en çok üç kez denenir ve devam eden provider hatası refresh'i fail eder. Geçerli empty response doğrudan `rows: []` kalır; sahte Campaign/AdGroup/Ad üretilmez ve ham provider payload snapshot'a persist edilmez.

**Kapanış sınırı:** Test/review hesabından delivery verisi beklenmez; bu ortam auth, advertiser erişimi, empty-result semantiği ve sentetik-write izolasyonunu kanıtlar. Yeni E6 modülü veya alt paketi açılmayacaktır. Kod akışı bu sadeleştirme ile kapanır. Geriye yalnız gerçek delivery verili normal advertiser üzerinde, her iki tarafta en az bir gerçek Ad satırı içeren üç ardışık shadow `PASS` ve ayrı primary activation/rollback insan kararı kalır; bu operasyonel kanıt oluşana kadar `production_activation=false` korunur.

**Durum:** `Code complete / non-empty production evidence pending` — account selection ve empty-result güvenliği tamamlandı; test hesabında veri oluşmasını beklemek E6 işi değildir.

### Kabul kriterleri

- Synthetic row gerçek performance olarak görünmez.
- Provider hierarchy toplamları double-count üretmez.
- TikTok Ad leaf'i Campaign ve AdGroup lineage'ını kayıpsız taşır.
- TikTok provider-specific fact şekli adapter sınırını geçmez.
- Unknown/unsupported değerler sıfırlaştırılmaz.
- Ownership, retry, parity ve legacy no-change geçer.

### Test planı

Provider fixtures, Campaign/AdGroup/Ad lineage, deterministic key, synthetic rejection, hierarchy totals, metric support, time/FX, retry ve parity.

### Rollback planı

TikTok V2 flag kapatılır; V1 korunur; synthetic/canonical store ayrımı geriye uyumludur.

### Bağımlılıklar

E4; TikTok production reporting contract kararı.

## 11. E7 — Klaviyo adapter

**Durum:** `Verification blocked — live legacy path still bypasses T6/T8 runtime`

### Planlanan işler

- **E7-T1:** Tek platform altında `channel=email|sms` contract'ı.
- **E7-T2:** Campaign→Campaign Message mapping.
- **E7-T3:** Flow→Flow Message ayrı root mapping.
- **E7-T3A:** Campaign ve Flow branch'leri için branch-aware deterministic entity key; same leaf ID collision koruması.
- **E7-T3B:** Campaign/Flow ve Email/SMS sonuçlarını aynı yedi bloklu envelope'a normalize et; ayrımı entity/channel değerleriyle taşı.
- **E7-T4:** Open≠Click düzeltmesi ve journey count/value support.
- **E7-T5:** SMS provider spend; unsupported ise `null`, uydurma `0` yok.
- **E7-T6:** `Verification blocked` — contract hazır; live refresh hâlâ eski 30-gün dağıtım yolunda.
- **E7-T7:** `Done/Parked` — UTM güvenilirliği nedeniyle GA4 Organic ingestion kapalı; Blend capability korunur.
- **E7-T8:** `Verification blocked` — runtime sınırları hazır; production composition ve live evidence yok.

### Kabul kriterleri

- Campaign/Flow ve Message kimlikleri provider'a izlenebilir.
- Email/SMS gerçek channel ile ayrılır.
- Provider/manual/estimated spend provenance ayrıdır.
- Unsupported journey/spend gerçek sıfır görünmez.
- Organic Campaign/Flow altına dağıtılmaz.
- Campaign Message ve Flow Message branch identity'leri aynı leaf ID durumunda dahi çakışmaz.
- Klaviyo branch/channel farklılıkları paralel canonical şemalar üretmez.

### Test planı

Campaign/Flow fixtures, Email/SMS, open-click negative, spend provenance, unsupported/null, Organic separation, retry ve parity.

### Rollback planı

Channel/branch bazlı flags; mevcut Email spend compatibility path korunur; otomatik pricing ayrı karar olmadan açılmaz.

### Bağımlılıklar

E4; Klaviyo event/spend mapping kararları; matched platform account kuralı.

### E7 T1–T5 birleşik uygulama kaydı — 2026-09-07

`src/providers/klaviyo/mapper.js` Email/SMS channel contract'ını, Campaign Message ve Flow Message sibling branch hierarchy'sini, branch-aware deterministic key'i, Open≠Click kuralını, journey support/null semantiğini ve yalnız provider kaynaklı SMS spend sınırını tek mapper'da uygular. Email spend T6 kararı öncesinde, Organic ayrımı da T7 kararı öncesinde bilinçli olarak açılmaz. Bu noktaya gelindiğinde kullanıcı uyarılacak; T6 ve T7 kullanıcı açıklaması alınmadan uygulanmayacaktır.

**Evidence:** `docs/E7_KLAVIYO_ADAPTER.md`, `tests/e7-klaviyo-adapter.test.js`, `src/providers/klaviyo/mapper.js`.

### E7-T7 karar kaydı — GA4 Organic park

Kullanıcı UTM kurulumunun eksik veya hatalı olması paid/organic attribution'ı güvenilmez kıldığı için GA4 Organic ingest'ten vazgeçildi. OAuth başlangıç/callback, GA4 property discovery/binding, manual snapshot ve automation sabit fail-closed politika ile park edildi; environment flag ile açılamaz. Mevcut connection/snapshot kayıtları destructive biçimde silinmez. `PAID`, `ORGANIC` ve `BLEND` analysis scope ile aggregate/formula capability korunur; ileride güvenilir backend attribution kaynağı kararı bu capability'yi yeniden besleyebilir.

**Evidence:** `src/providers/organic/ingest-policy.js`, `tests/e7-t7-organic-park.test.js`, `src/oauth/organic-handlers.js`, `server.js`.

### E7-T6 karar ve contract kaydı — usage-weighted maliyet

Kullanıcı aylık plan/currency girişini korur; ancak bedel artık takvim günlerine eşit bölünmez. Günlük Email allocation, o günün `Sent Email` adedinin ay toplamındaki payı üzerinden hesaplanır. Açık ay değerleri `provisional`, kapanmış ay değerleri `finalized` provenance taşır. SMS provider actual spend bütün tahminlerden önceliklidir. Overage yalnız included send ve kullanıcıya ait sözleşmesel unit cost birlikte sağlanırsa kümülatif günlük farktan estimate edilir. SMS actual provider spend yoksa kullanıcı açıkça unit cost tanımlamadıkça spend `unsupported/null` kalır. Global/internet örnek fiyatı ve actual+estimate double count yasaktır.

Bu formül canonical mapper ve unit test düzeyinde uygulanmıştır. 2026-09-07 canlı denetimi, production refresh'in hâlâ `normalizeKlaviyoInsight` içindeki eski `estimatedMonthlySpend / 30` yolunu kullandığını ve yeni `estimated_monthly_spend` kaydını okumadığını göstermiştir. Bu nedenle T6 production wiring tamamlanmış sayılmaz; kullanıcı arayüzünün yeniden adlandırılması tek başına kabul kanıtı değildir.

**Evidence:** `src/providers/klaviyo/mapper.js`, `tests/e7-klaviyo-adapter.test.js`, `server.js`, `docs/E7_KLAVIYO_ADAPTER.md`.

### E7 canlı corrective kapısı — account selection / spend ekranı / empty refresh

2026-09-07 production denetiminde account selection'ın ilk Save çağrısı Supabase `522` HTML gövdesini modalda gösterdi, tekrar Save ise hesabı başarıyla kaydetti. Aynı denetimde eski `Estimated Monthly Spend` ekranının T6 sonrası yanlış adla kaldığı ve hatasız boş refresh'in `klaviyo_empty_period_fallback` adlı sentetik Campaign satırı ürettiği doğrulandı. Corrective sınır: upstream HTML hiçbir kullanıcı/evidence/metadata yüzeyine taşınmaz; account discovery sentetik ID üretmez; form `Email Monthly Plan Cost` ve allocated provenance ile sunulur; boş provider sonucu `rows=[] / empty_result=true` olur. Bu corrective doğrulanmadan E9 başlamaz.

Canlı kayıt denetimi ayrıca son manual job'ın `completed` olduğunu fakat snapshot'ın tek `empty_period_fallback` satırı taşıdığını ve Klaviyo V2 tablosunda hiç satır bulunmadığını doğruladı. Bu, hatasız refresh'in yeni E7 runtime/parity akışından geçtiğini kanıtlamaz; yalnız eski snapshot yolunun hatasız tamamlandığını gösterir. E7-T8 modülü `server.js` production composition'ına bağlanmadan ve gerçek/boş provider sonucu aynı sınırda gözlenmeden E7 code-complete olarak kapatılamaz.

### E7-T8 birleşik runtime kapanış kaydı

Account identity/timezone/provider date doğrulaması, ortak FX normalization, tek Dataset V2 write boundary, Campaign/Flow + Email/SMS duplicate koruması, zero-row no-fake-write, exact branch/channel/fact/support parity ve legacy-authoritative shadow failure isolation `src/providers/klaviyo/runtime.js` içinde tamamlandı. Production primary activation yapılmaz. E7 için yeni alt paket açılmaz; gerçek provider message/report DTO'su ile live evidence alınana kadar mevcut legacy Klaviyo snapshot otoritesi korunur.

**Evidence:** `src/providers/klaviyo/runtime.js`, `tests/e7-t8-klaviyo-runtime.test.js`, `docs/E7_KLAVIAuth/provider-token güvenlik contract'ları.

**Uygulama adımları:** Shared client factory eklendi; kök doğrudan constructor import/çağrıları kaldırıldı; enabled/disabled, wiring ve invalid dependency testleri full/security suite'e bağlandı.

**Kabul kriterleri:** Client graph immutable; Supabase yalnız tam service-role credential çiftiyle ve session persistence kapalı kurulur; bağlı store'lar aynı client/vault instance'larını alır; disabled optional yüzey factory çağırmaz; invalid composition dependency fail-closed; full/security CI PASS.

**Test planı:** Dedicated E3-T4 dependency graph testi, E3-T1/T2/T3 regresyonları, full/security suite, syntax ve diff kontrolü.

**Rollback planı:** Shared client delegation commit'i revert edilerek E3-T3 sonrası kök construction geri alınır; data/schema rollback yoktur.

**Gözlemlenebilirlik:** Yalnız composition/test sonucu; credential, keyring, token, identity veya provider payload loglanmaz.

**Güvenlik ve veri etkisi:** Mevcut server-only construction seçeneklerini koruyan repository refactor'ıdır; production request, database/provider çağrısı, data/schema/policy/grant/token veya deployment etkisi yoktur.

**Planlanan:** E3-T4 immutable shared dependency graph ve executable wiring evidence.

**Gerçekleşen:** Repository implementasyonu PR #51 ile review edilmiş, PR ve merge sonrası security/Vercel kontrolleri geçmiş ve `main` üzerine merge edilmiştir. Public entrypoint incident düzeltmesi aynı PR üzerinde tamamlanmıştır.

**Sapmalar:** Google/provider request client'ları kullanım noktalarında bırakıldı; E3-T4 yalnız gerçekten shared server-side data/security dependency'lerini merkezileştirir.

**Evidence:** `src/clients/shared-clients.js`, `tests/e3-t4-shared-clients.test.js`, E3/full/security test çıktıları ve PR CI.

**Durum:** `Done` — PR #52 merge commit `29ae299f228f10a1b3026c4c9da843ffc502a1dc`; post-merge Security Regression ve Vercel production deployment PASS, ardından insan canlı smoke kontrolü `dev.adstable.app` landing sayfasının açıldığını doğruladı. Public static yüzey Express function boot failure'ından fiziksel olarak ayrılmıştır.

### E3-T5-A task aynası — HTTP middleware boundary

**Amaç:** E3-T5 middleware extraction işinin ilk kontrollü parçasında request correlation, metadata-only HTTP logging ve uncaught error normalization için tek canonical sınır kurmak.

**Mevcut durum:** E3-T1–E3-T4 `Done`; E3-T5-A merge/CI ve canlı incident kabulü sonrası `Done`. Express base middleware composition sınırındadır; request ID/correlation ve merkezi uncaught error contract yoktur. Auth/access/ownership helper'ları kök monolitte kalmaktadır.

**Planlanan durum:** `src/middleware/http-boundary.js` güvenli request ID üretim/iletimini, hassas query/body/header taşımayan tamamlanma logunu ve internal mesaj sızdırmayan standart error response'unu dependency injection ile sağlar.

**Kapsam:** Request ID validation/generation/response header, completion metadata, error metadata, 4xx/exposed ve 5xx message policy, composition/error-handler registration ve executable testler.

**Kapsam dışı:** Auth/access/ownership extraction (E3-T5-B), route registration (E3-T6), mevcut route-local catch bloklarının toplu dönüşümü, response/business contract, production request veya deployment.

**Bağımlılıklar:** E3-T4 shared clients ve E3-T2 composition root.

**Uygulama adımları:** Request/error boundary factory'leri eklendi; request boundary app creation sırasında, terminal error boundary route registration sonrasında kuruldu; correlation, redaction, fail-closed dependency ve error normalization testleri security/full suite kapsamına alındı.

**Kabul kriterleri:** Güvenli client request ID korunur, malformed ID yansıtılmaz, response correlation header taşır, query/body/header loglanmaz, beklenmeyen 5xx mesajı maskelenir, request ID error response/log ile eşleşir ve E3 characterization/full/security CI PASS.

**Test planı:** Dedicated E3-T5-A testi, E3 characterization, full/security suite, JavaScript syntax ve diff kontrolü.

**Rollback planı:** Middleware registration ve modül commit'i revert edilir; data/schema rollback yoktur.

**Gözlemlenebilirlik:** Event, request ID, method, path, status, duration ve güvenli error code; query string, body, headers, token, credential, identity veya provider payload yok.

**Güvenlik ve veri etkisi:** Repository/runtime HTTP boundary refactor; production request, provider/database çağrısı, data/schema/policy/grant/token mutation veya canlı deployment yapılmaz.

**Planlanan:** E3-T5-A canonical HTTP middleware boundary.

**Gerçekleşen:** Repository implementasyonu PR #52 ile review edilmiş ve `main` üzerine merge edilmiştir. Security Regression, Vercel production deployment ve insan landing-page smoke kontrolü PASS olmuştur.

**Sapmalar:** E3-T5 parent yalnız E3-T5-B auth/access/ownership extraction tamamlandıktan sonra `Done` olabilir.

**Evidence:** `src/middleware/http-boundary.js`, `tests/e3-t5a-http-middleware-boundary.test.js`, E3/full/security çıktıları ve PR CI.

**Durum:** `Done` — PR #52 merge commit `29ae299f228f10a1b3026c4c9da843ffc502a1dc`; full/security CI, production deployment ve insan canlı smoke kapıları tamamlandı.

### E3-T5-B task aynası — auth, access ve ownership boundary

**Amaç:** Kimlik doğrulama, subscription/lifecycle capability, provider connection ve platform-account ownership enforcement uygulamalarını tek injectable canonical sınırda toplamak.

**Mevcut durum:** E3-T5-A `Done`. Aynı enforcement davranışları kök `server.js` içinde dağınık helper fonksiyonları olarak bulunuyordu.

**Planlanan durum:** `src/middleware/access-boundary.js`, doğrulanmış bearer user kimliğini esas alan ve caller-controlled identity kabul etmeyen immutable bir enforcement graph üretir.

**Kapsam:** User authentication, subscription access, lifecycle access, provider connection, manual refresh connection ve active ownership enforcement; mevcut status/body contract parity; dependency injection ve negatif testler.

**Kapsam dışı:** Route registration/extraction (E3-T6), policy değerlerinin redesign edilmesi, OAuth/provider business logic, schema/data veya production işlemi.

**Bağımlılıklar:** E3-T5-A HTTP boundary, E1 OAuth/IDOR contract'ları ve E3-T4 shared clients.

**Uygulama adımları:** Canonical access factory eklendi; altı enforcement helper'ı kök monolitten kaldırılıp immutable boundary sonucuna bağlandı; auth/capability/connection/ownership negatif testleri security/full suite'e eklendi.

**Kabul kriterleri:** Kimlik yalnız doğrulanmış request user'dan gelir; inactive capability 403; eksik connection 404; ownership user/account/status ile fail-closed; response parity korunur; invalid dependency boot sırasında reddedilir; full/security CI PASS.

**Test planı:** Dedicated E3-T5-B testi, OAuth/IDOR ve E3 characterization, full/security suite, syntax ve diff kontrolü.

**Rollback planı:** Factory delegation commit'i revert edilerek önceki kök helper uygulamaları geri alınır; data/schema rollback yoktur.

**Gözlemlenebilirlik:** Yalnız HTTP boundary güvenli metadata'sı; bearer token, user/account identity, connection veya provider payload loglanmaz.

**Güvenlik ve veri etkisi:** Mevcut fail-closed enforcement'ın repository refactor'ıdır; production/provider/database çağrısı veya mutation yoktur.

**Planlanan:** E3-T5-B canonical access enforcement boundary.

**Gerçekleşen:** Repository implementasyonu PR #53 ile review edilmiş, full/security CI ve Vercel kontrolleri geçmiş ve `main` üzerine merge edilmiştir.

**Sapmalar:** Policy hesaplayıcıları bu küçük extraction'da yerinde bırakılmış, yalnız enforcement tekilleştirilmiştir. E3-T5 parent PR merge edilmeden `Done` değildir.

**Evidence:** `src/middleware/access-boundary.js`, `tests/e3-t5b-access-boundary.test.js`, OAuth/IDOR/E3/full/security çıktıları ve PR CI.

**Durum:** `Done` — PR #53 merge commit `4836514de0fd4720b6f14d2eeec89b5066801536`; focused IDOR, full/security CI ve Vercel deployment status kapıları tamamlandı. E3-T5-A ve E3-T5-B birlikte parent E3-T5'i kapatır.

### E3-T6-A task aynası — public route registration

**Amaç:** E3-T6 route-registration işinin ilk kontrollü parçasında public/static sayfalar, public config ve TikTok test guard registration'ını kök monolitten ince ve injectable route modülüne taşımak.

**Mevcut durum:** E3-T1–E3-T5 `Done`. Public route bildirimleri kök `server.js` içinde tek satırlı inline handler kümesi olarak bulunuyordu.

**Planlanan durum:** `src/routes/public-routes.js` immutable route manifest'i ve explicit dependency validation ile route registration yapar; handler'lar yalnız response delegation taşır.

**Kapsam:** Dokuz public page route'u, TikTok test feature guard, `/api/public-config`, physical file mapping, duplicate/order contract ve root delegation.

**Kapsam dışı:** Authenticated business/API route extraction (E3-T6-B), OAuth extraction (E3-T7), response redesign, data/schema veya production işlemi.

**Bağımlılıklar:** E3-T5 middleware boundary, E3-T1 characterization ve PR #52 static-entrypoint incident contract'ı.

**Uygulama adımları:** Public route manifest/registrar eklendi; root inline declarations kaldırıldı; static handler, config parity, feature guard ve missing dependency testleri full/security suite'e bağlandı.

**Kabul kriterleri:** Route'lar tam bir kez kaydedilir; static file mapping deterministik; public config shape ve TikTok 404 parity korunur; relative/missing dependency fail-closed; critical characterization ve full/security CI PASS.

**Test planı:** Dedicated E3-T6-A registration testi, E3-T1 characterization, Vercel static contract, full/security suite, syntax ve diff kontrolü.

**Rollback planı:** Registrar delegation commit'i revert edilerek inline public registration geri alınır; data/schema rollback yoktur.

**Gözlemlenebilirlik:** E3-T5-A güvenli request metadata'sı; public config dışı environment değeri, credential veya identity loglanmaz.

**Güvenlik ve veri etkisi:** Repository/runtime route registration refactor; production/provider/database çağrısı veya mutation yoktur.

**Planlanan:** E3-T6-A public route registration boundary.

**Gerçekleşen:** Repository implementasyonu PR #54 ile review edilmiş, full/security CI ve Vercel kontrolleri geçmiş ve `main` üzerine merge edilmiştir.

**Sapmalar:** Authenticated API route'ları kontrollü E3-T6-B kapsamına bırakılmıştır; parent E3-T6 henüz `Done` değildir.

**Evidence:** `src/routes/public-routes.js`, `tests/e3-t6a-public-route-registration.test.js`, E3 characterization/Vercel/full/security çıktıları ve PR CI.

**Durum:** `Done` — PR #54 merge commit `82e1dc68feebd51cd3678436a0614a3eb08a92ca`; characterization, full/security CI ve Vercel deployment status kapıları tamamlandı.

### E3-T6-B task aynası — authenticated API route registration

**Amaç:** E3-T6'nın authenticated yüzeyinde route→authorization→service/policy akışını ince, injectable ve merkezi error boundary'ye delege eden bir referans registration ile kurmak.

**Mevcut durum:** E3-T6-A `Done`. Kritik `/api/account/status` route'u authentication, subscription lookup, lifecycle policy ve response/error handling'i inline kök handler içinde birleştiriyordu.

**Planlanan durum:** `src/routes/account-status-routes.js` yalnız explicit dependency'lerle route'u kaydeder; verified user authorization önce çalışır, service/policy dependency'leri sırayla çağrılır ve hatalar canonical error boundary'ye iletilir.

**Kapsam:** `/api/account/status`, dependency validation, auth-before-service ordering, response parity ve error forwarding.

**Kapsam dışı:** Bütün legacy API route'larının toplu taşınması, OAuth extraction (E3-T7), service/repository business logic redesign, schema/data veya production işlemi.

**Bağımlılıklar:** E3-T6-A registrar, E3-T5 access/error boundary ve E3-T1 critical characterization.

**Uygulama adımları:** Authenticated account-status registrar eklendi; inline kök route kaldırıldı; auth ordering, response parity, error forwarding ve missing dependency testleri full/security suite'e bağlandı.

**Kabul kriterleri:** Authentication subscription erişiminden önce; unauthenticated contract 401 parity; başarılı response shape değişmez; async hata response yazmadan `next(error)` ile canonical boundary'ye gider; invalid dependency fail-closed; full/security CI PASS.

**Test planı:** Dedicated E3-T6-B testi, E3-T1 characterization, access/error boundary, full/security suite, syntax ve diff kontrolü.

**Rollback planı:** Registrar delegation commit'i revert edilerek inline account-status route'u geri alınır; data/schema rollback yoktur.

**Gözlemlenebilirlik:** Canonical HTTP request/error metadata'sı; bearer token, user/subscription identity veya payload loglanmaz.

**Güvenlik ve veri etkisi:** Repository/runtime route registration refactor; production/provider/database çağrısı veya mutation yoktur.

**Planlanan:** E3-T6-B authenticated route registration reference flow.

**Gerçekleşen:** Repository implementasyonu PR #55 ile review edilmiş, full/security CI ve Vercel kontrolleri geçmiş ve `main` üzerine merge edilmiştir.

**Sapmalar:** Legacy route'lar risk kontrollü sonraki epic extraction'larında aynı registrar pattern'ine taşınacaktır; E3-T6 parent merge tamamlanmadan `Done` değildir.

**Evidence:** `src/routes/account-status-routes.js`, `tests/e3-t6b-authenticated-route-registration.test.js`, E3 characterization/access/error/full/security çıktıları ve PR CI.

**Durum:** `Done` — PR #55 merge commit `a176e128ed2a7643c58cc66b67ba93febd2b1bbe`; characterization, full/security CI ve Vercel deployment status kapıları tamamlandı. E3-T6-A ve E3-T6-B birlikte parent E3-T6'yı kapatır.

### E3-T7-A task aynası — OAuth transaction boundary

**Amaç:** E1'de güvenli hale getirilen OAuth transaction create/consume ve authorization response orchestration'ını kök monolitten immutable, test edilebilir OAuth modülüne taşımak.

**Mevcut durum:** E3-T1–E3-T6 `Done`. Transaction cleanup/create/consume ve JSON/redirect response-mode helper'ları kök `server.js` içinde bulunuyordu.

**Planlanan durum:** `src/oauth/transaction-boundary.js`, transaction store'u injectable dependency olarak tüketir; create öncesi cleanup, atomic consume delegation ve açık response-mode davranışını tek sınırda korur.

**Kapsam:** OAuth transaction cleanup/create, provider+redirect+PKCE input delegation, state normalization/consume, missing-store fail-closed create, JSON handshake ve browser redirect response mode.

**Kapsam dışı:** Provider OAuth start/callback route extraction (E3-T7-B), transaction-store persistence redesign, token exchange, production provider isteği veya schema/data işlemi.

**Bağımlılıklar:** E1-T3 OAuth transaction store, E1 OAuth security regression ve E3-T6 route registration.

**Uygulama adımları:** OAuth transaction boundary eklendi; üç kök helper kaldırılıp immutable boundary alias'larına bağlandı; cleanup ordering, bound input, missing-state/store ve response-mode testleri security/full suite'e eklendi.

**Kabul kriterleri:** Create cleanup sonrası çalışır; user/provider/redirect/PKCE aynen store'a bağlanır; consume yalnız store üzerinden ve normalized state ile çalışır; missing store create'i fail-closed; response mode parity korunur; OAuth/full/security CI PASS.

**Test planı:** Dedicated E3-T7-A testi, OAuth security/transaction-store regresyonları, full/security suite, syntax ve diff kontrolü.

**Rollback planı:** Boundary delegation commit'i revert edilerek kök helper'lar geri alınır; data/schema rollback yoktur.

**Gözlemlenebilirlik:** Transaction state, PKCE, token, user/provider payload loglanmaz; yalnız mevcut güvenli HTTP metadata'sı vardır.

**Güvenlik ve veri etkisi:** Mevcut OAuth orchestration'ın repository refactor'ıdır; production/provider/database çağrısı veya mutation yoktur.

**Planlanan:** E3-T7-A OAuth transaction orchestration boundary.

**Gerçekleşen:** Repository implementasyonu PR #56 ile review edilmiş, full/security CI ve Vercel kontrolleri geçmiş ve `main` üzerine merge edilmiştir.

**Sapmalar:** Provider start/callback route'ları E3-T7-B kontrollü extraction kapsamındadır; parent E3-T7 henüz `Done` değildir.

**Evidence:** `src/oauth/transaction-boundary.js`, `tests/e3-t7a-oauth-transaction-boundary.test.js`, OAuth security/store/full/security çıktıları ve PR CI.

**Durum:** `Done` — PR #56 merge commit `1c340cb27dcd689f731b57965c979cc0fc5c4968`; OAuth/full/security CI ve deployment status kapıları tamamlandı.

**Canlı sınır:** PR #46 merge/review tamamlanmıştır. Revize read-only diagnostic preflight ancak yeni açık insan production onayından sonra tek istek olarak çalıştırılabilir.


### E3-T7-B3-B2 task aynası — Organic, Klaviyo ve TikTok OAuth handlers

**Amaç:** Kalan Organic/GA4, Klaviyo ve TikTok OAuth start/callback orchestration'ını kök monolitten immutable, injectable provider handler modüllerine taşımak.

**Mevcut durum:** PR #60 ile Google Sheets extraction merge edilmiş ve `main` security/Vercel kontrolleri PASS olmuştur. Üç provider'ın OAuth handler'ları kök `server.js` içinde inline bulunuyordu.

**Planlanan durum:** Her provider canonical transaction ve route registrar sınırını kullanır; verified transaction identity, PKCE, reconnect/selection ve provider-specific response parity korunur.

**Kapsam:** Organic, Klaviyo ve TikTok OAuth start/callback handler factory'leri, root composition, canonical route registration ve negatif güvenlik testleri.

**Kapsam dışı:** Provider ingest, account selection redesign, API business route'ları, schema/data ve production OAuth çağrıları.

**Bağımlılıklar:** E3-T7-A/B1/B2/B3-A/B3-B1 ve E1 OAuth security baseline.

**Uygulama adımları:** Üç injectable handler modülü eklendi; inline route'lar kaldırıldı; transaction identity, invalid state, PKCE ve already-connected guard testleri security suite'e bağlandı.

**Kabul kriterleri:** Caller identity kullanılmaz; invalid state token exchange öncesi durur; Klaviyo PKCE transaction'a bağlıdır; TikTok connected guard korunur; response parity ve full/security CI PASS olur.

**Test planı:** Dedicated E3-T7-B3-B2 testi, OAuth security/full suite, syntax ve diff kontrolü.

**Rollback planı:** Handler wiring commit'i revert edilerek inline implementation geri alınır; data/schema rollback yoktur.

**Gözlemlenebilirlik:** Token, PKCE, state, user/account identity veya provider payload loglanmaz; mevcut güvenli HTTP metadata sınırı korunur.

**Güvenlik ve veri etkisi:** Repository refactor; production OAuth/provider/database çağrısı veya mutation yapılmaz.

**Planlanan:** E3-T7-B3-B2 remaining provider OAuth extraction.

**Gerçekleşen:** Organic, Klaviyo ve TikTok handler factory'leri ve root delegation tamamlandı; executable negatif testler eklendi.

**Sapmalar:** Uygulanamaz — kapsam planlandığı gibi uygulandı.

**Evidence:** `src/oauth/organic-handlers.js`, `src/oauth/klaviyo-handlers.js`, `src/oauth/tiktok-handlers.js`, `tests/e3-t7b3b2-provider-oauth-handlers.test.js` ve CI çıktıları.

**Durum:** `Done` — PR #61 merge commit `d849fe53cd0e6ad430e79139a0b5dcb33a4a271f`; focused/full/security CI, Vercel ve post-merge `main` Security Regression kapıları tamamlandı. E3-T7 parent kapanmıştır.


### E3-T8-A task aynası — refresh job lifecycle boundary

**Amaç:** Refresh/snapshot job persistence ve queued→running→terminal orchestration'ı kök monolitten injectable, test edilebilir bir job sınırına taşımak.

**Mevcut durum:** E3-T7 PR #61 ile merge ve post-merge CI kapılarını tamamladı. Job duplicate guard, insert ve status timestamp davranışları kök `server.js` helper'larında bulunuyordu.

**Planlanan durum:** `src/jobs/refresh-job-boundary.js` create/transition/run portlarını immutable dependency graph ile sunar; root yalnız compatibility delegation taşır.

**Kapsam:** Active-job duplicate guard, queued insert contract, running/completed/failed timestamps, safe failure transition, injectable clock/client ve root delegation.

**Kapsam dışı:** Provider-specific snapshot work extraction ve automation/recovery policy orchestration (E3-T8-B), schema/data, production job veya provider çağrısı.

**Bağımlılıklar:** E3-T2 composition root, E3-T4 shared client, E3-T5 error boundary ve E3-T7 OAuth extraction.

**Uygulama adımları:** Job boundary factory eklendi; kök create/status helper implementasyonları boundary alias'larına dönüştürüldü; deterministic lifecycle ve failure testleri security suite'e bağlandı.

**Kabul kriterleri:** Duplicate active job 409 ile insert öncesi reddedilir; queued row parity korunur; running ve terminal timestamp'leri canonical clock kullanır; work hatası failed transition denedikten sonra aynı hatayı yeniden fırlatır; invalid dependency fail-closed; full/security CI PASS.

**Test planı:** Dedicated E3-T8-A testi, E3 characterization, full/security suite, syntax ve diff kontrolü.

**Rollback planı:** Boundary delegation commit'i revert edilerek kök helper implementasyonları geri alınır; data/schema rollback yoktur.

**Gözlemlenebilirlik:** Mevcut snapshot_jobs status/timestamp/error_message alanları korunur; credential, token veya provider payload loglanmaz.

**Güvenlik ve veri etkisi:** Repository/runtime refactor; production database/provider çağrısı veya mutation yapılmaz.

**Planlanan:** E3-T8-A canonical refresh job lifecycle boundary.

**Gerçekleşen:** Factory, root delegation ve executable lifecycle/failure testleri tamamlandı.

**Sapmalar:** Parent E3-T8, provider work ve automation/recovery orchestration E3-T8-B ile boundary `run` portuna taşınmadan `Done` değildir.

**Evidence:** `src/jobs/refresh-job-boundary.js`, `tests/e3-t8a-refresh-job-boundary.test.js`, full/security çıktıları ve PR CI.

**Durum:** `Done` — PR #62 merge commit `23f5dfc3f5ba0bff0e60be61e16e0203a200b62f`; focused/full/security CI, Vercel ve post-merge `main` Security Regression kapıları tamamlandı.


### E3-T8-B1 task aynası — manual snapshot job orchestration

**Amaç:** Organic, TikTok ve Klaviyo manual snapshot handler'larındaki tekrarlı create→running→write→completed/failed job akışını E3-T8-A boundary `run` portuna taşımak.

**Mevcut durum:** E3-T8-A PR #62 ile merge ve post-merge CI kapılarını tamamladı. Üç handler lifecycle geçişlerini ayrı ayrı yönetiyordu.

**Planlanan durum:** `src/jobs/manual-snapshot-orchestrator.js` provider work callback'ine yalnız immutable source-job context verir ve completion evidence sözleşmesini tekilleştirir.

**Kapsam:** Organic/TikTok/Klaviyo manual refresh job orchestration, source job identity, completion metadata, failure job correlation ve root wiring.

**Kapsam dışı:** Meta/Google özel diagnostic completion payload'ları ve automation/recovery orchestration (E3-T8-B2), provider fetch/write business logic, schema/data veya production işlemi.

**Bağımlılıklar:** E3-T8-A refresh job lifecycle boundary.

**Uygulama adımları:** Manual orchestrator eklendi; üç handler boundary `run` portuna delege edildi; job boundary failure'a non-enumerable job correlation ekledi; focused testler security suite'e bağlandı.

**Kabul kriterleri:** Job metadata parity korunur; provider write verified job ID alır; completion snapshot/spread evidence taşır; failed transition tek kez boundary tarafından yapılır; HTTP hata response job ID correlation'ını korur; full/security CI PASS.

**Test planı:** E3-T8-A/B1 focused testleri, characterization, full/security suite, syntax ve diff kontrolü.

**Rollback planı:** Orchestrator delegation commit'i revert edilerek üç handler'ın inline lifecycle akışı geri alınır; data/schema rollback yoktur.

**Gözlemlenebilirlik:** Existing snapshot_jobs lifecycle ve HTTP job_id correlation korunur; credential/provider payload loglanmaz.

**Güvenlik ve veri etkisi:** Repository/runtime orchestration refactor; production job/database/provider çağrısı veya mutation yapılmaz.

**Planlanan:** E3-T8-B1 manual snapshot orchestration boundary.

**Gerçekleşen:** Organic, TikTok ve Klaviyo manual handler delegation ve executable orchestration/failure testleri tamamlandı.

**Sapmalar:** Meta/Google ve automation/recovery akışları provider-specific evidence şekilleri nedeniyle E3-T8-B2 kontrollü kapsamına bırakıldı; parent E3-T8 henüz `Done` değildir.

**Evidence:** `src/jobs/manual-snapshot-orchestrator.js`, `src/jobs/refresh-job-boundary.js`, `tests/e3-t8b1-manual-snapshot-orchestrator.test.js`, full/security çıktıları ve PR CI.

**Durum:** `Done` — PR #63 merge commit `d38eda63d8fa395036d898cd480cda1cdb8acd7a`; focused/full/security CI, Vercel ve post-merge `main` Security Regression kapıları tamamlandı.


### E3-T8-B2-A task aynası — TikTok/Klaviyo automation recovery orchestration

**Amaç:** TikTok ve Klaviyo automation primary/recovery snapshot job zincirini canonical E3-T8 job boundary üzerine taşımak ve paired recovery davranışını tekilleştirmek.

**Mevcut durum:** E3-T8-B1 PR #63 ile merge ve post-merge CI kapılarını tamamladı. İki provider aynı create→running→write→completed/failed ve optional recovery akışını kopyalıyordu.

**Planlanan durum:** `src/jobs/automation-snapshot-orchestrator.js` primary ve paired recovery run'larını yönetir; provider wrapper yalnız prerequisites, policy ve write dependency'sini sağlar.

**Kapsam:** TikTok/Klaviyo primary automation job, optional paired recovery, recovery failure isolation/correlation, schedule success update parity ve ortak provider wrapper.

**Kapsam dışı:** Organic/Meta/Google özel ownership/diagnostic/recovery akışları (E3-T8-B2-B), policy redesign, provider business logic, schema/data veya production işlem.

**Bağımlılıklar:** E3-T8-A ve E3-T8-B1.

**Uygulama adımları:** Automation orchestrator eklendi; TikTok/Klaviyo tekrarları ortak wrapper ve boundary run portuna taşındı; paired identity ve recovery failure testleri security suite'e bağlandı.

**Kabul kriterleri:** Primary metadata parity; recovery `pairedPrimaryJobId` taşır; provider write immutable job context alır; recovery failure primary success'i bozmaz ve job ID taşır; primary failure rethrow edilir; schedule yalnız primary success sonrası güncellenir; full/security CI PASS.

**Test planı:** E3-T8 focused orchestration testleri, characterization, full/security suite, syntax ve diff kontrolü.

**Rollback planı:** Orchestrator/wrapper delegation commit'i revert edilerek iki inline automation akışı geri alınır; data/schema rollback yoktur.

**Gözlemlenebilirlik:** Existing job/recovery result ve schedule timestamp sözleşmeleri korunur; credential/provider payload loglanmaz.

**Güvenlik ve veri etkisi:** Repository/runtime orchestration refactor; production cron/job/database/provider çağrısı veya mutation yapılmaz.

**Planlanan:** E3-T8-B2-A TikTok/Klaviyo automation/recovery boundary.

**Gerçekleşen:** Common automation orchestrator, provider wrapper ve executable primary/recovery/failure testleri tamamlandı.

**Sapmalar:** Organic/Meta/Google farklı prerequisites ve completion diagnostics nedeniyle E3-T8-B2-B kontrollü kapsamına bırakıldı; parent E3-T8 henüz `Done` değildir.

**Evidence:** `src/jobs/automation-snapshot-orchestrator.js`, `tests/e3-t8b2a-automation-snapshot-orchestrator.test.js`, full/security çıktıları ve PR CI.

**Durum:** `Done` — PR #64 merge commit `eb25e9effc1c198960e85fdf2bb5f29cba616dc3`; focused/full/security CI, Vercel ve post-merge `main` Security Regression kapıları tamamlandı.


### E3-T8-B2-B1 task aynası — Organic automation orchestration

**Amaç:** Organic automation primary/recovery job zincirini canonical automation orchestrator'a taşırken ownership, timezone ve time-engine diagnostic metadata parity'sini korumak.

**Mevcut durum:** E3-T8-B2-A PR #64 ile merge ve post-merge CI kapılarını tamamladı. Organic aynı lifecycle zincirini ek time diagnostics ile inline yönetiyordu.

**Planlanan durum:** Automation orchestrator provider-specific primary/recovery metadata extension portları taşır; pairing alanı caller metadata tarafından override edilemez.

**Kapsam:** Organic automation delegation, primary time diagnostics, recovery engine version, paired recovery, schedule update ve response parity.

**Kapsam dışı:** Meta/Google özel lifecycle/diagnostic akışları (E3-T8-B2-B2), policy/time-engine redesign, schema/data veya production işlem.

**Bağımlılıklar:** E3-T8-B2-A automation orchestrator.

**Uygulama adımları:** Orchestrator'a controlled metadata extension eklendi; Organic inline lifecycle kaldırıldı; pairing override negatif testi eklendi.

**Kabul kriterleri:** Organic prerequisite/skip parity korunur; primary diagnostic metadata kayıpsızdır; recovery engine version taşır; caller pairing override edemez; recovery failure isolation ve schedule success timing değişmez; full/security CI PASS.

**Test planı:** E3-T8 automation focused testleri, characterization, full/security suite, syntax ve diff kontrolü.

**Rollback planı:** Organic delegation ve metadata extension commit'i revert edilerek inline lifecycle geri alınır; data/schema rollback yoktur.

**Gözlemlenebilirlik:** Existing job time diagnostics, recovery result ve schedule timestamps korunur; credential/provider payload loglanmaz.

**Güvenlik ve veri etkisi:** Repository/runtime orchestration refactor; production cron/job/database/provider çağrısı veya mutation yapılmaz.

**Planlanan:** E3-T8-B2-B1 Organic automation boundary.

**Gerçekleşen:** Organic delegation, controlled metadata extension ve pairing override testleri tamamlandı.

**Sapmalar:** Meta/Google diagnostic shapes E3-T8-B2-B2 kapsamına bırakıldı; parent E3-T8 henüz `Done` değildir.

**Evidence:** `src/jobs/automation-snapshot-orchestrator.js`, `server.js`, `tests/e3-t8b2a-automation-snapshot-orchestrator.test.js`, full/security çıktıları ve PR CI.

**Durum:** `Done` — PR #65 merge commit `21dfb0596a36d51696df6eb5823ed466866796c2`; focused/full/security CI, Vercel ve post-merge `main` Security Regression kapıları tamamlandı.


### E3-T8-B2-B2-A task aynası — Meta/Google manual job orchestration

**Amaç:** Meta ve Google manual snapshot lifecycle'larını canonical manual orchestrator'a taşımak ve Google completion diagnostics'i allowlisted saf evidence mapper'a çıkarmak.

**Mevcut durum:** E3-T8-B2-B1 PR #65 ile merge ve post-merge CI kapılarını tamamladı. Meta/Google create/transition/failure akışlarını inline yönetiyor, Google üç grain diagnostic payload'ını handler içinde kuruyordu.

**Planlanan durum:** Manual orchestrator controlled job metadata ve custom completion portları sunar; canonical manual alanlar caller tarafından override edilemez; Google evidence ayrı saf mapper'dır.

**Kapsam:** Meta/Google manual job delegation, time/account/limit metadata parity, Google campaign/adgroup/ad diagnostic evidence, failure job correlation ve HTTP response parity.

**Kapsam dışı:** Meta/Google automation/recovery orchestration (E3-T8-B2-B2-B), provider fetch/write logic, diagnostic redesign, schema/data veya production işlem.

**Bağımlılıklar:** E3-T8-B1 manual orchestrator ve E3-T8-B2-B1 merge kabulü.

**Uygulama adımları:** Manual orchestrator metadata/custom completion portları eklendi; Meta ve Google handler'ları delege edildi; Google evidence mapper ve negatif testler security suite'e bağlandı.

**Kabul kriterleri:** Manual canonical metadata override edilemez; Meta time/limit parity; Google account/date/login metadata parity; provider write verified source job alır; Google evidence raw rows taşımaz; failed transition tek kez boundary tarafından yapılır ve HTTP job correlation korunur; full/security CI PASS.

**Test planı:** Manual orchestrator/evidence focused testleri, characterization, full/security suite, syntax ve diff kontrolü.

**Rollback planı:** Delegation/evidence mapper commit'i revert edilerek inline lifecycle ve diagnostic mapping geri alınır; data/schema rollback yoktur.

**Gözlemlenebilirlik:** Existing job metadata/Google diagnostic allowlist ve HTTP correlation korunur; raw row, credential veya provider payload loglanmaz.

**Güvenlik ve veri etkisi:** Repository/runtime orchestration refactor; production job/database/provider çağrısı veya mutation yapılmaz.

**Planlanan:** E3-T8-B2-B2-A Meta/Google manual orchestration.

**Gerçekleşen:** Meta/Google delegation, manual extension portları, Google evidence mapper ve executable testler tamamlandı.

**Sapmalar:** Meta/Google automation/recovery E3-T8-B2-B2-B kapsamına bırakıldı; parent E3-T8 henüz `Done` değildir.

**Evidence:** `src/jobs/manual-snapshot-orchestrator.js`, `src/jobs/google-snapshot-job-evidence.js`, `tests/e3-t8b2b2a-google-job-evidence.test.js`, full/security çıktıları ve PR CI.

**Durum:** `Done` — PR #66 merge commit `25705b33307423ebdd23db044ab3c45186b796b9`; focused/full/security CI, Vercel ve post-merge `main` Security Regression kapıları tamamlandı.


### E3-T8-B2-B2-B task aynası — Meta/Google automation orchestration

**Amaç:** Meta ve Google automation primary/recovery lifecycle'larını canonical automation orchestrator'a taşımak ve provider-specific completion evidence parity'sini korumak.

**Mevcut durum:** E3-T8-B2-B2-A PR #66 ile merge ve post-merge CI kapılarını tamamladı. Meta/Google automation akışları create/transition/recovery durumlarını inline yönetiyordu.

**Planlanan durum:** Automation orchestrator custom primary/recovery completion portları sunar; Meta/Google yalnız prerequisite, metadata, write ve response shaping taşır.

**Kapsam:** Meta/Google primary/recovery delegation, time/login/limit metadata, Google row/spread completion evidence, paired recovery, failure isolation, schedule update ve result parity.

**Kapsam dışı:** Provider fetch/write ve automation policy redesign, schema/data veya production işlem; E3-T9 architecture guard ayrı tasktır.

**Bağımlılıklar:** E3-T8-A–B2-B2-A job boundaries/orchestrators.

**Uygulama adımları:** Automation custom completion portları eklendi; Meta/Google inline lifecycle kaldırıldı; primary/recovery spread evidence mapper ve executable testleri security suite'e bağlandı.

**Kabul kriterleri:** Provider prerequisite/skip parity; diagnostic metadata kayıpsız; recovery canonical pairing; Google primary metadata korunur ve recovery narrow contract değişmez; primary failure rethrow, recovery failure isolation; schedule yalnız primary success sonrası güncellenir; full/security CI PASS.

**Test planı:** Automation/evidence focused testleri, characterization, full/security suite, syntax ve diff kontrolü.

**Rollback planı:** Delegation/custom completion commit'i revert edilerek inline Meta/Google lifecycle geri alınır; data/schema rollback yoktur.

**Gözlemlenebilirlik:** Existing job metadata, recovery result, schedule timestamps ve provider result contracts korunur; raw row/credential/provider payload loglanmaz.

**Güvenlik ve veri etkisi:** Repository/runtime orchestration refactor; production cron/job/database/provider çağrısı veya mutation yapılmaz.

**Planlanan:** E3-T8-B2-B2-B Meta/Google automation orchestration.

**Gerçekleşen:** Meta/Google delegation, custom completion ports, spread evidence mapper ve executable testler tamamlandı.

**Sapmalar:** Uygulanamaz — kapsam planlandığı gibi uygulandı. E3-T8 parent PR merge/CI tamamlanana kadar `Verification` kalır.

**Evidence:** `src/jobs/automation-snapshot-orchestrator.js`, `src/jobs/snapshot-job-evidence.js`, `tests/e3-t8b2b2b-snapshot-job-evidence.test.js`, full/security çıktıları ve PR CI.

**Durum:** `Done` — PR #67 merge commit `2cb6b14453b2fbf543f9e2fe4f3c54208169a2d9`; focused/full/security CI, Vercel ve post-merge `main` Security Regression kapıları tamamlandı. Parent E3-T8 kapanmıştır.


### E3-T9 task aynası — architecture growth guard

**Amaç:** Yeni business logic, named/async function veya route registration'ın kök `server.js` monolitini büyütmesini ve extracted `src` modüllerinin root monolite geri bağımlı olmasını CI seviyesinde engellemek.

**Mevcut durum:** E3-T8 PR #67 ile merge ve post-merge CI kapılarını tamamladı. Monolit büyütmeme kuralı belge düzeyindeydi; executable CI guard yoktu.

**Planlanan durum:** Frozen baseline ve fail-closed architecture evaluator, root satır/function/route growth'ünü ve `src`→`server.js` ters bağımlılığını reddeder; workflow içindeki zorunlu security suite guard testini çalıştırır.

**Kapsam:** Server line/named/async/route ceilings, extracted-module root import rejection, deterministic JSON CLI, unit/negative testler, locked security manifest ve mevcut GitHub Actions security-suite entegrasyonu.

**Kapsam dışı:** Canonical DTO/repository boundary guard (E3-T10), mevcut monolitin kalan extraction'ı, runtime/data/schema veya production işlem.

**Bağımlılıklar:** E3-T1–E3-T8 modularization baseline.

**Uygulama adımları:** Baseline manifest ve evaluator eklendi; CLI/npm script oluşturuldu; mevcut zorunlu security suite manifestine architecture testi bağlandı; growth/root-import negatif testleri security suite'e eklendi.

**Kabul kriterleri:** Current main PASS; herhangi guarded metric growth FAIL; invalid limit FAIL; `src` module root server import FAIL; deterministic redacted output; security/full CI PASS.

**Test planı:** Dedicated architecture test, CLI, locked manifest, full/security suite, workflow contract, syntax ve diff kontrolü.

**Rollback planı:** Guard/manifest entegrasyon commit'i revert edilir; runtime/data/schema rollback yoktur. Guard bypass için baseline sessizce yükseltilemez; yeni business logic önce target module extraction ile net-negative root değişiklik üretmelidir.

**Gözlemlenebilirlik:** Yalnız metric/limit/violation code JSON çıktısı; source, credential, env veya payload yok.

**Güvenlik ve veri etkisi:** CI/repository statik kontrol; production runtime/database/provider çağrısı veya mutation yapılmaz.

**Planlanan:** E3-T9 executable architecture guard.

**Gerçekleşen:** Baseline/evaluator, CLI, tests ve mevcut zorunlu workflow tarafından çalıştırılan security manifest entegrasyonu tamamlandı.

**Sapmalar:** Uygulanamaz — kapsam planlandığı gibi uygulandı.

**Evidence:** `security/e3-architecture-baseline.json`, `security/architecture-guard.js`, `tests/e3-t9-architecture-guard.test.js`, `tests/security-regression-manifest.js`, `.github/workflows/security-regression.yml` ve CI çıktıları.

**Durum:** `Done` — PR #68 merge commit `718b370dc2df7b5897e79aa27b759c85ad2ff8f6`; architecture/security/full CI, Vercel ve post-merge `main` Security Regression kapıları tamamlandı.


### E3-T10 task aynası — canonical boundary guard

**Amaç:** Provider-specific DTO veya doğrudan Dataset V2 erişiminin canonical validator/hierarchy ve repository portunu atlayarak persistence, Formula Engine veya Funnel Query sınırına geçmesini executable CI kontrolüyle engellemek.

**Mevcut durum:** E3-T9 PR #68 ile merge ve post-merge CI kapılarını tamamladı. Repository implementasyonlarında validator vardı; cross-module bypass ve provider→business-math import yasağı için merkezi guard/port yoktu.

**Planlanan durum:** Canonical write boundary validation-before-delegation uygular; policy guard Dataset V2 relation ve canonical upsert allowlist'ini, business-math provider import yasağını ve zorunlu boundary dosyalarını fail-closed denetler.

**Kapsam:** Runtime module scan, direct Dataset V2 access rejection, canonical upsert bypass rejection, Formula/Query provider import rejection, canonical write port, deterministic CLI, negative tests ve locked security manifest.

**Kapsam dışı:** Provider adapter implementasyonları (E4+), Dataset V2 schema/data, production ingest veya canlı işlem.

**Bağımlılıklar:** V4 canonical envelope freeze, existing canonical/hierarchy validators, Dataset V2 repositories ve E3-T9 architecture guard.

**Uygulama adımları:** Canonical write boundary ve policy manifest eklendi; runtime scanner/CLI oluşturuldu; bypass/provider-import/missing-boundary negatif testleri security suite'e bağlandı.

**Kabul kriterleri:** Current runtime PASS; invalid provider DTO repository'ye ulaşmadan FAIL; direct Dataset V2 runtime access allowlist dışı FAIL; canonical repository method bypass FAIL; Formula/Query provider import FAIL; missing boundary/policy FAIL; full/security CI PASS.

**Test planı:** Dedicated canonical boundary test, CLI, Phase 1/2 regression, locked manifest, full/security suite, syntax ve diff kontrolü.

**Rollback planı:** Guard/port/manifest commit'i revert edilir; runtime/data/schema rollback yoktur. Guard allowlist'i provider ihtiyacıyla sessizce genişletilemez; yeni write path canonical port üzerinden kurulmalıdır.

**Gözlemlenebilirlik:** Yalnız checked-module count ve violation code JSON çıktısı; source, row, credential, identity veya provider payload yok.

**Güvenlik ve veri etkisi:** CI/repository static/runtime validation boundary; production database/provider çağrısı veya mutation yapılmaz.

**Planlanan:** E3-T10 canonical validation/repository/business-math boundary guard.

**Gerçekleşen:** Canonical write port, policy/scanner CLI, bypass/import/missing-boundary testleri ve security manifest entegrasyonu tamamlandı.

**Sapmalar:** Uygulanamaz — kapsam planlandığı gibi uygulandı.

**Evidence:** `funnel-core/canonical-write-boundary.js`, `security/e3-canonical-boundary-policy.json`, `security/canonical-boundary-guard.js`, `tests/e3-t10-canonical-boundary-guard.test.js` ve CI çıktıları.

**Durum:** `Done` — PR #69 merge commit `67c7a19d17541db734747e6de712486555585f3e`; PR Security/Vercel kapıları ile post-merge `main` Security Regression (security + full regression) tamamlandı. Production işlemi yapılmadı.

### E2-T6-D1 task aynası — postcheck failure diagnostic preparation

**Amaç:** E2-T6 V3 rollback-only acceptance sonrasında alınan `POSTCHECK_FAILED` sonucunu transaction retry, cleanup veya raw production veri ifşası olmadan allowlisted failed gate code seviyesinde teşhis etmek.

**Mevcut durum:** Read-only preflight 21/21 PASS verdi. Açık production onayıyla transaction tam bir kez ve mandatory postcheck tam bir kez gönderildi; operator `POSTCHECK_FAILED` üretti, capsule `consumed` oldu ve retry yapılmadı.

**Kapsam:** Consumed state ve exact `POSTCHECK_FAILED` outcome binding; explicit diagnostic confirmation; mevcut postcheck sorgusunun operator-local baseline ile read-only çalıştırılması; yalnız failed gate code, checked gate count ve `productionCountsExposed:false` çıktısı; fail-closed test ve security manifest entegrasyonu.

**Kapsam dışı:** Bu PR'da production diagnostic sorgusu, transaction retry, cleanup/recovery, write, schema/policy/grant/ledger, deployment, environment veya secret değişikliği.

**Kabul kriterleri:** Exact confirmation olmadan çalışma yok; yalnız consumed failed capsule kabul edilir; gate allowlist/count/passed sözleşmesi exact olur; all-pass diagnostic `currentPostcheckPass:true` olarak raporlanır; malformed diagnostic fail-closed olur; actual/expected production count ve identity raporlanmaz; full/security CI PASS.

**Rollback:** Diagnostic operator/script/test ve manifest kaydı revert edilir. Consumed production capsule ve outcome değiştirilmez.

**Durum:** `Verification` — repository hazırlığı ve PR/CI tamamlanmadan, ardından ayrı açık production onayıyla diagnostic çalıştırılmadan E2-T6-D1 `Done` değildir. E2-T6 ve parent E2 `Verification/In progress` kalır.

### E2-T6-D1-R1 revision aynası — diagnostic all-pass ve safe-stage contract

**Incident:** Açık onaylı ilk read-only diagnostic request `DIAGNOSTIC_FAIL_CLOSED` üretti. Repository-local prerequisite audit PASS olduğundan failure query veya result-contract aşamasındadır; ikinci production query/retry çalıştırılmadı.

**Düzeltme:** Güncel postcheck artık tüm gate'ler PASS ise bunu hata saymak yerine `currentPostcheckPass:true` ve boş `failedGateCodes` ile redacted başarı olarak raporlar. Query ve contract failure ayrı allowlisted safe code üretir; count/identity/raw error yine raporlanmaz.

**Durum:** `Verification` — corrective PR #72 merge commit `a1c11fb2ce5e642858ed2734a6061b66a7351f17` ile `main` üzerine alındı; PR security/Vercel kapıları, post-merge `main` Security Regression ve 439 testlik yerel full regression PASS. GitHub'daki merge sonucu başarılıdır; açık PR kalmamıştır. Ayrı açık production onayıyla tek read-only diagnostic tamamlanmadan E2-T6-D1-R1 veya E2-T6 `Done` değildir ve diagnostic retry çalıştırılmamıştır.

**Sıradaki uygulanabilir kapı:** E2-T6-D1-R1 için yalnız açık insan production onayı sonrasında tek read-only diagnostic. E2 `Done` olmadığı için E4-T1 bağımlılık kapısı henüz açılmamıştır; production onayı gelmeden E4 kodlaması başlatılmaz.

### E2-T6-D2 task aynası — capsule-independent current-state safety audit

**Amaç:** D1'e ait tüketilmiş operator capsule/outcome sidecar'ının yeni çalışma ortamında bulunmaması nedeniyle geçmiş sonucu tahmin ederek yeniden üretmeden, production'ın bugün yeni bir E2-T6 kabulüne hazırlanmasının güvenli olup olmadığını tek salt-okuma isteğiyle belirlemek.

**Kapsam:** Merge edilmiş V3 preflight'in 21 kapısını yeniden kullanan checksum/main-bound audit operatorü; exact insan confirmation; tek Management API read-only request; sıfır retry; yalnız başarısız gate kodları ile `safeToPrepareFreshAcceptance` kararı; production count, identity, raw row ve raw error ifşa etmeyen çıktı.

**Kapsam dışı:** Eski capsule/outcome rekonstrüksiyonu, V3 diagnostic veya transaction retry, E2-T6 PASS iddiası, fixture write, cleanup, schema/policy/grant/ledger/data/environment/deployment değişikliği ve E2-T7 execution.

**Kabul kriterleri:** Query tek read-only `WITH` statement'tır; exact 21 gate allowlist/boolean/integer contract fail-closed doğrulanır; query/contract/prerequisite hataları ayrı güvenli kodlara dönüşür; hiçbir count veya identity raporlanmaz; full/security CI PASS. Audit PASS yalnız yeni ve ayrı isim alanlı acceptance hazırlığına izin verir; E2-T6'yı `Done` yapmaz.

**Gerçekleşen:** PR #74 merge commit `6b2618571ded9076530b6294df313d848f7e4c4a` ve post-merge `main` CI PASS sonrasında açık insan production onayı alındı. Audit production'a tam bir read-only istek gönderdi, 21/21 gate PASS verdi, başarısız gate kodu üretmedi ve `safeToPrepareFreshAcceptance:true` sonucunu döndürdü. Production count/identity ifşa edilmedi; retry, write, cleanup, schema/policy/grant/ledger/data/environment/deployment değişikliği yapılmadı.

**Evidence:** `artifacts/dataset-v2-acceptance/e2-t6-rls/current-state-audit-v1.json` ve executable redaction/contract testi.

**Durum:** `Done` — repository paketi, PR/CI, tek insan onaylı read-only production audit ve redacted evidence tamamlandı. Bu sonuç E2-T6 kabulünü `Done` yapmaz; yalnız yeni ve ayrı namespace/version kullanan fresh acceptance hazırlığına güvenli geçiş kapısını açar. E2-T6/T7 `Verification` ve parent E2 `In progress` kalır.

**Sıradaki uygulanabilir iş:** E2-T6-D3 fresh namespaced acceptance preparation. Bu repository hazırlığı production işlemi değildir; ilerideki preflight ve rollback-only transaction ayrı açık production onaylarına tabi kalır.

### E2-T6-D3 task aynası — fresh namespaced V4 acceptance preparation

**Amaç:** D2 current-state safety audit PASS sonrasında geçmiş V1/V2/V3 operation veya capsule'larını tekrar kullanmadan, E2-T6'nın 16-case canlı RLS kabulünü yeni `e2_t6_rls_v4` namespace/version ile güvenli biçimde yeniden hazırlamak.

**Kapsam:** PR #75 merge main binding; V4 checksum-bound 21-gate preflight; iki distinct eligible user; corrected canonical fixture; intact 16-case rollback-only transaction; 19-gate mandatory postcheck; bütün historical E2-T6 namespace'lerinde aggregate residue-zero kapısı; tek kullanımlık repository-dışı state/outcome; safe terminal code ve redacted evidence.

**Kapsam dışı:** Bu PR'da production preflight/transaction/postcheck, geçmiş operation retry, cleanup, persistent DDL, schema/policy/grant/ledger/data/environment/deployment değişikliği ve E2-T7 execution.

**Kabul kriterleri:** V4 operation/fixture/artifact isimleri distinct olur; preflight ve postcheck bütün E2-T6 namespace residue'sunu fail-closed izler; transaction tek outer `BEGIN`/zorunlu `ROLLBACK` taşır ve V3 disposable-corrected contract'ı korur; operator query/gate/transaction/postcheck hatalarını güvenli terminal kodlarla ayırır; capsule tekrar kullanılamaz; focused/full/security CI PASS.

**Gerçekleşen:** Preparation PR #76 merge commit `2dc3da54d1a3415bc5b4272e8f51970fd9c03bca` ve post-merge `main` CI PASS oldu. Ayrı açık insan production onayıyla V4 preflight tam bir kez gönderildi; repository full regression 452/452 ve canlı 21/21 gate PASS sonrasında repository-dışı tek kullanımlık capsule `APPROVAL_READY` oluşturuldu. Transaction request sayısı sıfırdır; production count/identity ifşa edilmedi ve write/cleanup/deployment yapılmadı.

**Evidence:** `artifacts/dataset-v2-acceptance/e2-t6-rls/v4-preflight-live.json`; operator-local beş baseline source control'a alınmaz.

**Durum:** `Verification` — preparation ve preflight PASS; rollback-only transaction ile mandatory postcheck henüz çalıştırılmadı. Bunlar yalnız ayrı açık production onayından sonra tek execute akışında gönderilir. Redacted evidence review PASS olmadan E2-T6 `Done` değildir; E2-T7 `Verification` ve parent E2 `In progress` kalır.

### E2-T6-D3-R1 task aynası — V4 postcheck failure diagnostic

**Incident:** PR #77 merge ve post-merge CI sonrasında açık insan production onayıyla V4 execute tam bir kez çalıştırıldı. Transaction response 16-case evidence converter'dan geçti; mandatory postcheck tam bir kez gönderildi ve terminal outcome `POSTCHECK_FAILED` oldu. Capsule `CONSUMED`; transaction/postcheck retry edilmedi.

**Amaç:** Başarılı transaction evidence ile başarısız postcheck sonucunu, tüketilmiş V4 capsule/outcome'a exact bağlanan tek read-only diagnostic ile yalnız failed gate code seviyesinde ayırmak.

**Kapsam:** V4 state/outcome binding; exact confirmation; mevcut postcheck'in operator-local baseline ile tek read-only çalıştırılması; count/identity/raw error içermeyen allowlisted sonuç; query/contract/prerequisite safe-code ayrımı; executable redaction testi.

**Kapsam dışı:** Bu PR'da production diagnostic, transaction/postcheck retry, cleanup, write, schema/policy/grant/ledger/data/environment/deployment değişikliği ve E2-T7 execution.

**Durum:** `Verification` — diagnostic preparation PR/CI/merge ve ayrı açık production onayıyla tek diagnostic tamamlanmadan E2-T6 `Done` değildir. V4 transaction veya mandatory postcheck hiçbir koşulda tekrar gönderilmez.

### E2-T6-D3-R2 task aynası — safe diagnostic shape classification

**Incident:** PR #78 merge ve post-merge CI sonrasında açık insan production onayıyla ilk V4 diagnostic tam bir kez gönderildi. Query aşaması tamamlandı fakat strict result contract `DIAGNOSTIC_CONTRACT_FAILED` üretti. Diagnostic retry edilmedi; production count/identity/raw result ifşa edilmedi.

**Düzeltme:** Diagnostic yalnız allowlisted gate kodları üzerinden `failed`, `missing`, `duplicate` ve `malformed` listeleri ile bilinmeyen gate varlığını boolean olarak sınıflandırır. Bilinmeyen değer asla echo edilmez. Böylece structural mismatch güvenli biçimde teşhis edilirken count, identity, raw row veya raw error açığa çıkmaz.

**Durum:** `Verification` — R2 PR/CI/merge ve ayrı açık production onayıyla tek yeni read-only shape diagnostic tamamlanmadan E2-T6 `Done` değildir. Önceki diagnostic, transaction veya mandatory postcheck retry edilmez.

### E2-T6-D3-R3 task aynası — zero-baseline-safe corrective diagnostic

**Bulgu:** PR #79 merge ve post-merge CI sonrasında açık insan production onayıyla shape diagnostic tam bir kez gönderildi. Sonuç 18 gate, sıfır failed/duplicate/malformed/unknown ve yalnız `DATASET_V2_BASELINE` missing oldu. Bu, Dataset V2 başlangıç nüfusu sıfırken `count(*) ... cross join expected ... group by` ifadesinin sıfır input group nedeniyle gate satırı üretmemesinden kaynaklanan deterministic SQL result-shape hatasıdır; güvenlik gate failure veya residue kanıtı değildir.

**Düzeltme:** Üç population baseline gate'i zero-row-safe scalar subquery olarak üreten ayrı read-only corrective diagnostic SQL'i hazırlanır. Original mandatory postcheck ve checksum-bound acceptance artefaktı değiştirilmez; consumed transaction/postcheck tekrar edilmez. Corrective diagnostic exact state baselines ile 19 gate'i tek istekte değerlendirir.

**Gerçekleşen:** PR #80 merge commit `ed783c4f79defe10800bf9656f6945892a9e2ab2` ve post-merge CI PASS sonrasında açık insan production onayıyla zero-baseline-safe corrective diagnostic tam bir kez gönderildi. Sonuç 19/19 PASS; failed/missing/duplicate/malformed gate yok, unknown gate yok, current postcheck PASS. Production count/identity ifşa edilmedi; diagnostic retry, transaction/postcheck retry, cleanup, write veya deployment yapılmadı.

**Evidence:** `artifacts/dataset-v2-acceptance/e2-t6-rls/v4-corrective-diagnostic-live.json` ve executable exact/redaction testi.

**Kabul önerisi:** V4 21/21 preflight PASS, corrected canonical 16-case transaction evidence PASS, mandatory rollback, terminal observability, deterministic missing-gate root cause ve zero-safe 19/19 current postcheck PASS birlikte E2-T6 kabul zincirini tamamlar. Original mandatory postcheck'in `POSTCHECK_FAILED` sonucu silinmez; zero-baseline SQL shape sapması olarak evidence zincirinde korunur.

**Durum:** `Done` — closeout PR #81 merge commit `97e380daa37dd38d811d918c8c5f8e121100af7f`, post-merge Security/Full Regression ve açık insan iş kabulü tamamlandı. E2-T7 final no-change acceptance sıradaki uygulanabilir iştir; parent E2, T7/T8 tamamlanana kadar `In progress` kalır.

### E2-T7-A task aynası — V3/V4 final residue coverage

**Amaç:** E2-T7 final no-change kabulünün, ilk hazırlıktan sonra eklenen E2-T6 V3 ve kabul edilen V4 canonical fixture anahtarlarını da eksiksiz kapsamasını sağlamak.

**Kapsam:** Fixture inventory'ye V3/V4 escaped canonical prefix eklenmesi; baseline ve final read-only selector'larının V1–V4 bütün T6 nesillerini hem `E2_T6_RESIDUE` hem `TOTAL_E2_RESIDUE` kapılarında izlemesi; runbook ve executable exact-source testlerinin güncellenmesi.

**Kapsam dışı:** Bu PR'da production baseline/final query, cleanup/delete, data/schema/policy/grant/ledger/environment/deployment değişikliği ve E2-T8 execution.

**Kabul kriterleri:** Inventory sekiz exact/prefix kaynağı taşır; V3/V4 prefix'leri committed transaction entity key kaynaklarıyla eşleşir; iki read-only SQL aynı selector setini kullanır; broad wildcard yoktur; converter contract ve full/security CI PASS.

**Durum:** `Verification` — repository preparation PR/CI/merge tamamlanmadan ve ayrı açık production onayıyla E2-T7 baseline/final evidence ile insan review'ı PASS olmadan E2-T7 `Done` değildir.

### E2-T7-B task aynası — fail-closed baseline/final operator

**Amaç:** E2-T7'nin 19-gate baseline ve final no-change kontrollerini manuel placeholder veya sonuç birleştirmesi olmadan, checksum/main-bound ve tek kullanımlık operator akışıyla yürütmek.

**Kapsam:** PR #83 merge main binding; full regression ön kapısı; baseline query tek istek; 19 satırlık operator-local `0600` baseline sidecar ve beş scalar baseline capsule; exact confirmation; final placeholder otomasyonu; final query tek istek; redacted evidence converter; consumed state, zero retry ve safe terminal codes.

**Kapsam dışı:** Bu PR'da production baseline/final query, cleanup/delete, fixture operation tekrarı, data/schema/policy/grant/ledger/environment/deployment değişikliği ve E2-T8 execution.

**Kabul kriterleri:** Baseline/final SQL read-only kalır; operator bütün executable artefaktlara checksum-bound olur; baseline gate failure state oluşturmaz; final gönderilmeden önce state consumed işaretlenir ve tekrar gönderilemez; baseline/final raw count'ları source control'a girmez; yalnız redacted PASS evidence paylaşılır; focused/full/security CI PASS.

**Gerçekleşen:** Operator preparation PR #84 merge commit `b3744f617895b93951c3fc873b38a94898caf1eb` ve post-merge CI PASS oldu. Ayrı açık insan production onayıyla baseline tam bir kez gönderildi; repository full regression 467/467 ve canlı baseline 19/19 PASS. Repository-dışı `0600` baseline sidecar ile tek kullanımlık `APPROVAL_READY` capsule oluşturuldu. Final request sıfır; production count/identity ifşa edilmedi ve retry/cleanup/write/deployment yapılmadı.

**Evidence:** `artifacts/dataset-v2-acceptance/e2-t7-cleanup/v2-baseline-live.json`; ham baseline satırları ve beş scalar değer source control'a alınmaz.

**Durum:** `Verification` — operator preparation ve baseline PASS; final no-change query henüz çalıştırılmadı. Ayrı açık production onayıyla tek final request, redacted evidence ve insan review PASS olmadan E2-T7 `Done` değildir.

### E2-T7-B-D1 task aynası — final evidence corrective diagnostic

**Amaç:** Tek kullanımlık E2-T7 final sorgusu veri değişikliği yapmadan tamamlandığı halde evidence sözleşmesinin sonucu reddetmesinin nedenini, production adetleri ve kimlikleri yayımlamadan sınıflandırmak.

**Gerçekleşen (2026-08-29):** Açık insan production onayıyla E2-T7-B final no-change sorgusu yalnız bir kez gönderildi. Sorgu hatası oluşmadı; sonuç evidence katmanında `FINAL_EVIDENCE_FAILED` güvenli koduyla fail-closed oldu. Kapsül `CONSUMED` durumundadır, outcome üretilmemiştir ve retry yapılmayacaktır.

**Düzeltme paketi:** Ayrı `e2_t7_final_diagnostic_v1` işlemi, tüketilmiş başarısız kapsülü şart koşar; aynı 19 kapıyı tek salt-okunur sorguyla yalnız `check_code` ve boolean `passed` biçiminde sınıflandırır. Ham/özet production adetleri, beklenen değerler ve kimlikler sonuçtan çıkarılmıştır. Tanılama da tek kullanımlıdır ve sorgudan önce tüketilir.

**Durum:** `Verification` — repository test/CI/merge tamamlandıktan sonra ayrı açık insan production onayı olmadan tanılama çalıştırılmaz. Tanılama bir kabul retry'ı veya E2-T7 PASS kanıtı değildir; güvenli düzeltme kararının girdisidir.

### E2-T7-B-D1-R1 task aynası — diagnostic CLI bağlantı düzeltmesi

**Gerçekleşen (2026-08-29):** PR #86 merge ve post-merge CI PASS sonrasında açık insan production onayıyla tanılama başlatıldı; CLI var olmayan `createClient` sembolünü çağırdığı için Management API istemcisi kurulmadan yerel olarak durdu. Production sorgusu sıfır, diagnostic outcome yok ve kapsül tüketilmedi.

**Düzeltme:** CLI repository'nin gerçek `createManagementClient` fabrikasına bağlandı; import sırasında yan etki üretmeyen `main` sınırı ve argüman/fabrika regresyon testi eklendi.

**Durum:** `Verification` — düzeltme PR/CI/merge tamamlanmadan production tanılaması çalıştırılmaz. İlk onaylı girişim production'a ulaşmadığı için ayrı production sorgusu oluşmamıştır.

### E2-T7-B-D2 task aynası — named-baseline corrective diagnostic

**Gerçekleşen (2026-08-29):** Açık insan production onayıyla `e2_t7_final_diagnostic_v1` tek salt-okunur request olarak tamamlandı. Redacted sonuç yalnız beş operator-local baseline kapısını (`DATASET_V2_BASELINE`, `DATASET_V1_BASELINE`, `SNAPSHOT_BASELINE`, `CONNECTED_CONNECTIONS`, `ENCRYPTED_TOKEN_ROWS`) başarısız sınıflandırdı; request `1`, retry `0`, production count/identity exposure `false`.

**Kök neden:** V1/final operator beş özdeş `(-1)::bigint` placeholder'ını dosyadaki pozisyona göre, fakat baseline anahtarlarını farklı bir sırayla yerleştirdi. Bu nedenle beş baseline değeri yanlış kapılara bağlandı; tanılama gerçek bir residue/security ihlali göstermedi.

**Düzeltme paketi:** V2 tanılama her baseline için ayrı ve testle bire bir doğrulanan marker kullanır. V1 diagnostic tüketilmiş olmalı; V2 sorgudan önce tüketilir, tek request/read-only kalır ve yalnız check code/boolean sınıflandırması döndürür.

**Durum:** `Verification` — repository PR/CI/merge ve ayrı açık insan production onayı tamamlanmadan V2 tanılama çalıştırılmaz. V1 sonucu E2-T7 PASS sayılmaz ve final kabul retry'ı yapılmaz.

### E2-T7-B-D2 evidence ve review kaydı

**Gerçekleşen (2026-08-29):** PR #88 merge commit `e7aa1d981f59f06de278a2a8a74782a5d5cc86cc` ve post-merge Security/Full Regression PASS sonrasında açık insan production onayıyla named-baseline V2 tanılaması tek kez çalıştırıldı. Sonuç `ALL_GATES_PASS`, 19/19 gate, failed code boş, request `1`, retry `0`; production count ve identity yayımlanmadı.

**Karar anlamı:** V1/final başarısızlığı production drift veya residue kanıtı değildir; positional placeholder eşleme kusurudur. Aynı operator-local baseline değerleri isimleriyle doğru kapılara bağlandığında tüm residue, Dataset V2/V1/snapshot, ledger/OAuth/token/schema/RLS/policy/grant ve persistent-object kontrolleri PASS olmuştur.

**Evidence:** `artifacts/dataset-v2-acceptance/e2-t7-cleanup/v2-diagnostic-live.json` yalnız allowlisted/redacted sonuç taşır. Ham sayımlar ve kimlikler source control'a alınmamıştır.

**Durum:** `Done` — evidence PR #89 merge commit `c68ffce246a17c8068280cd965235ec82528f6c6`, post-merge Security/Full Regression ve açık insan iş kabulü tamamlandı. E2-T8 `Verification` ve parent E2 `In progress` kalır.


### E2-T7 closeout kaydı

**Gerçekleşen (2026-08-29):** Baseline 19/19 PASS sonrasında tek final request evidence katmanında positional placeholder kusuruyla fail-closed oldu ve retry edilmedi. V1 read-only tanılama beş baseline kapısını sınıflandırdı. Root cause repository'de doğrulandı; named-baseline V2 read-only tanılama tek request ile 19/19 `ALL_GATES_PASS` verdi. PR #89 merge commit `c68ffce246a17c8068280cd965235ec82528f6c6`, post-merge CI ve açık insan iş kabulü tamamlandı.

**Kapanış kararı:** E2-T7 `Done`. Production data/schema/ledger/policy/grant/environment/deployment değişmedi; fixture residue ve persistent evidence object kapıları PASS; raw count/identity paylaşılmadı. Parent E2 yalnız E2-T8 tamamlanana kadar `In progress` kalır.

### E2-T8-A task aynası — source capture operatorü

**Amaç:** Production application-schema current-state baseline hazırlığı için source inventory ve schema-only capture'ı iki ayrı insan onayıyla, tek kullanımlık kapsül ve repository-dışı karantina sınırında yürütmek.

**Kapsam:** `E2-T8-A1` read-only source inventory preflight; `E2-T8-A2` fixed `public` schema-only/no-owner capture; `E2-T8-A3` approved-main/artifact checksum ve external `0600` state/inventory; `E2-T8-A4` strict captured-SQL validator ile redacted checksum sonucu.

**Kabul kriterleri:** Source inventory tek read-only request; unclassified/duplicate/malformed object fail-closed; raw inventory repository dışında; capture yalnız exact ikinci confirmation ile; raw SQL yalnız repository dışı `0700/0600` karantinada; row/identity/URI/SQL çıktısı paylaşılmaz; state sorgu/capture öncesi tüketilir ve retry yoktur.

**Gerçekleşen (2026-08-29):** PR #90 merge commit `a5e0917acf650e753161a97a85baf9e851d967b9` ve post-merge CI PASS sonrasında capture operator repository paketi hazırlandı. Bu pakette production inventory/capture, target provisioning, restore, migration, ledger, data/schema/policy/grant/environment/deployment değişikliği yapılmadı.

**Durum:** `Verification` — PR/CI/merge sonrasında source inventory preflight için ayrı açık production onayı; preflight PASS sonrasında schema-only capture için ikinci açık production onayı gerekir. Static testler actual capture veya restore değildir.

> **2026-09-07 — Pinterest Paket 1 canlı corrective:** İkinci Connect denemesi environment readiness kontrolünü geçti ancak `PINTEREST_OAUTH_START_FAILED` ile OAuth transaction oluşturulmadan durdu. Repository migration denetiminde kesin kök neden bulundu: `oauth_transactions_provider_check` izin listesinde `pinterest` yoktu. Pinterest resmi V5 OpenAPI tanımındaki authorization/token URL'leri mevcut runtime ile eşleştiğinden provider URL'si değiştirilmedi. Constraint'e `pinterest` eklendi; Paket 1 canlı OAuth/account-selection doğrulaması alınana kadar `Verification` durumundadır.

> **2026-09-07 — Pinterest Paket 1 advertiser discovery corrective:** Canlı OAuth authorization/callback başarıyla tamamlandı ve encrypted connection yenilendi; önceki OAuth transaction engelinin kapandığı doğrulandı. Account picker'ın boş kalmasının kesin nedeni Pinterest resmi V5 `AdAccount.time_zone` alanının normalizer tarafından okunmamasıydı (`timezone`/`timezone_name` bekleniyordu). Resmi `time_zone` desteği eklendi; eksik identity/currency/timezone fail-closed kuralı korundu. Paket 1 canlı account-selection kanıtına kadar `Verification` durumundadır.

> **2026-09-07 — Pinterest Paket 1 ID-only discovery corrective:** `time_zone` düzeltmesi sonrasında canlı picker yine boş kaldı. Resmi Pinterest V5 OpenAPI yeniden incelendi: list endpoint'indeki `AdAccount` şemasında yalnız `id` zorunlu; `name`, `currency`, `time_zone` opsiyoneldir. Dolayısıyla list row'unun tam profil olduğu varsayımı kaldırıldı. Runtime accessible ID listesini alıp her ID'yi resmi `/ad_accounts/{id}` ile, mevcut üç hesap sınırı içinde zenginleştirir; eksik alan uydurmadan fail-closed kalır. Canlı account-selection kanıtına kadar Paket 1 `Verification` durumundadır.

> **2026-09-07 — Pinterest ürün kararı / PARKED:** Kullanıcı Pinterest entegrasyonundan vazgeçti. Paket 1 `Stopped/Parked`; Paket 2–3 `Not started/Parked` durumuna alındı. Yeni OAuth start/callback, provider token değişimi ve account discovery kapatıldı; dashboard `Parked` gösterir. Mevcut encrypted connection/token, ownership ve tarihsel snapshot kayıtları destructive olarak silinmez. Yeniden başlatma yalnız yeni açık kullanıcı iş kararıyla mümkündür.

