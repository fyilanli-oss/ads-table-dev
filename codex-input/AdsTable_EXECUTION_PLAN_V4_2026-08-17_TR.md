Warning: truncated output (original token count: 88243)
Total output lines: 3702

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
| R3 | Dataset V2 workspace tenant dönüşümü | E2 + E3 | `In progress / R3-A+B Done; R3-C held for R6 activation` — additive canlı şema ve server-authoritative workspace runtime sınırı tamamlandı; provider activation ve final tenant enforcement henüz yapılmadı. |
| R4 | Workspace provider connection birleşimi | E10-T6 | `Done / R4-A+B+C` — kanonik boş tablo canlıda; standalone OAuth/refresh/write hattı fail-closed donduruldu; tarihsel kayıtlar korundu. |
| R5 | Mevcut Klaviyo account konsolidasyonu | E7 + E10-T6 | `Done / controlled clean reset completed` — no-refresh doğrulama `409` ile fail-closed durduğu için eski grant kanoniğe taşınmadı. Shopify modalındaki işlem-anı onayıyla revoke tamamlandı; embedded satır `revoked`, canonical Klaviyo `0` kaldı ve tarihsel alanlar korundu. |
| R6 | Embedded provider runtime → canonical V2 | E4 + E5 + E7 | `In progress / R6-A+B+C Done; R7-A Ready` — ortak authority→connection→currency→runner→workspace V2 sınırı kodlandı fakat production'a kaydedilmedi. R6-C canlı preflight ve postcheck PASS; Dataset V2 `user_id` nullable, veri `0`, `workspace_id` staged nullable ve legacy korumalar yerinde. Gerçek provider kabulü R7-A'nın ilk currency ve canonical connection kaydından sonra R6-D'de yapılır. |
| R7 | Currency-first ve Shopify-native Connect/Disconnect UX | E10-T5 + E10-T6-C2I-V10 | `R7-A repository complete; production acceptance next; R7-B blocked by R6-D` — currency gate, explanation modal, canonical OAuth pending write ve Meta/Google/Klaviyo verified account selection repository'de tamamlandı. Production deployment ve merchant currency/provider kabulünden sonra R6-D; onun ardından R7-B açılır. |
| R8 | V1 tarihsel geçiş ve resumable backfill | E9 | `Blocked by R3/R6` — doğrulanmış legacy binding, re-fetch veya canonical validation; fake/synthetic/ambiguous satır yok. |
| R9 | Production read cutover | E13 | `Blocked by R6–R8` — provider bazlı canary, V2 read, SLO/parity/restore/rollback ve insan GO. |
| R10 | Standalone OAuth ve V1 legacy retirement | E14 | `Blocked by R9 stabilization` — consumer-zero, read-disable observation, ayrı retirement migration ve restore noktası. |
| R11 | WooCommerce readiness contract | E10 tenant model future extension | `Deferred` — WooCommerce implementation yok; ortak workspace/currency/connection/V2 çekirdeğinin Shopify'a kilitlenmediğini kanıtlayan contract. |

#### Paketler için ortak uygulama ve kabul kuralları

- **R0:** Eski ve embedded Klaviyo akışları aynı anda refresh üretemez. Hiçbir token/kayıt silinmez; Klaviyo `/oauth/revoke` çağrılmaz. Rollback, yalnız konfigürasyon kapısını eski güvenli duruma döndürür.
- **R1 — Done:** `user_id` kullanan canonical envelope, Dataset V2, repository, query, job, backfill, ownership ve RLS noktaları; `workspace_id` kullanan Shopify installation/OAuth noktalarıyla birlikte envanterlendi. `contracts/r1-workspace-authority-v1.json` versionlı authority kararıdır; `docs/R1_WORKSPACE_AUTHORITY_DECISION.md` exact R2–R7 migration/release sırasını ve kabul kapılarını kaydeder. Kod/DB/provider mutation yapılmadı. R2 ayrı açık insan onayı almadan başlamaz.
- **R2 — Done:** Additive `workspaces` ve `workspace_settings` migration'ı, versionlı currency contract'ı, salt okunur preflight/postcheck ve fail-closed rollback hazırlandı. 20 Eylül 2026 ilk canlı preflight'ta bulunan embedded migration ledger farkı, ayrı açık onayla R2-A kapsamında yalnız `20260911130000` ve `20260911150000` sürümleri işlenerek kapatıldı; E9/backfill sahte `applied` yapılmadı. İkinci ayrı açık onayla `20260920090105_create_workspace_currency_foundation` canlıya uygulandı. Postcheck `PASS`: bir doğrulanmış Shopify workspace'i canonical registry'ye seed edildi, `workspace_settings` boş bırakıldı, foreign key doğrulandı, RLS + force RLS açık, browser rolleri kapalı ve `service_role` yalnız explicit CRUD yetkili. OAuth/provider/Dataset V2 adetleri değişmedi. Shopify currency hiçbir alana kaynak olmadı. Advisor taramasında R2 kaynaklı yeni WARN/performance bulgusu yoktur; server-only tablolardaki policiesiz RLS bilgi kaydı beklenen deny-by-default modelidir. R3 ayrı kapsam ve onay kapısıyla başlar.
- **R3 — In progress / R3-A Done; R3-B runtime verification:** 23 Eylül 2026 canlı preflight Dataset V2'nin `0` satır içerdiğini, canonical workspace'in bulunduğunu, `workspace_id` kolonunun ve `backfill_checkpoints` tablosunun bulunmadığını doğruladı. Açık production onayıyla additive `20260923083734_add_dataset_v2_workspace_tenant` migration'ı uygulandı; nullable `workspace_id`, doğrulanmış workspace foreign key'i, canonical unique index ve üç query indexi oluşturuldu. Postcheck `PASS`; Dataset V2 yine `0` satır, workspace-bound satır `0`; eski `user_id`, unique index, authenticated SELECT policy ve grant'ler korundu. Canonical contract V2, workspace-scoped repository/Query Service/backfill sınırı ile negatif cross-workspace testleri hazırdır. `NOT NULL`, eski unique/index/policy retirement, backfill ve provider runtime cutover yapılmadı. `backfill_checkpoints` canlıda hiç oluşmadığı için eski user-scoped E9 migration karantinada kalır; fiziksel workspace checkpoint tablosu R8'de lease/control semantiğiyle oluşturulacaktır. Sonraki kapı R3-B runtime doğrulaması; R3-C final enforcement ayrı onay ister.
- **R3 — In progress / R3-A+B Done; R3-C held for R6 activation:** R3-A canlı sonucu korunur: migration `20260923083734`, postcheck `PASS`, Dataset V2 `0` satır ve workspace-bound satır `0`. R3-B'de server-resolved workspace authority kullanan ortak runtime sınırı eklendi; caller tenant alanları, cross-workspace write ve cross-workspace query fail-closed reddedilir. Contract V2'de `workspace_id` zorunlu tenant, `user_id` ise yalnız optional compatibility/actor alanıdır. Canlı metadata `user_id` kolonunun hâlâ `NOT NULL` ve `public.users(id)` foreign key'ine bağlı olduğunu doğruladı; Shopify embedded installation zorunlu Supabase user UUID taşımadığı için sahte/eşleştirilmiş user üretmek yasaktır. Bu nedenle provider aktivasyon migration'ı R6 ile aynı kapıda `user_id` alanını nullable yapacak; R6 writer kabulünden sonra R3-C `workspace_id NOT NULL`, workspace policy ve legacy index retirement ile final enforcement'ı tamamlayacaktır. Böylece eski R3↔R6 bağımlılık döngüsü kaldırıldı: R4 artık R3-A+B sonrasında açılır, R6 R4-R5'i bekler, R3-C R6 kabulünü bekler. Bu pakette provider/OAuth/runtime route/deployment aktive edilmedi.
- **R4:** Canonical connection store `workspace_id + provider` başına tek state taşır; OAuth callback tek başına `Connected` değildir; active account server-side ownership doğrulaması ister. Token yalnız encrypted envelope'dur. Eski ve yeni OAuth runtime aynı anda authoritative olamaz.
- **R4-A+B — Tamamlandı / R4-C write-freeze approval gate:** 23 Eylül 2026 canlı preflight `PASS` sonrasında açık production onayıyla `20260923091731_create_workspace_provider_connections` uygulandı. `workspace_id + provider` keyed kanonik tablo `0` satırla oluşturuldu; primary key, doğrulanmış workspace foreign key'i, lifecycle indexi, constraint'ler, force RLS, browser deny ve explicit service-role CRUD postcheck'i `PASS` verdi. Mevcut sayılar değişmedi: `1` Shopify-scoped connected Klaviyo, `8` legacy connection, `7` encrypted legacy token, `0` plaintext token. Embedded ve legacy Klaviyo aynı anda bulunduğu için hiçbir kayıt/token otomatik kopyalanmadı. OAuth route, runtime, provider grant/revoke, legacy write davranışı ve deployment değiştirilmedi. Advisor kontrolünde R4-B kaynaklı yeni WARN yoktur; server-only tablodaki policiesiz RLS ve boş tablonun kullanılmamış lifecycle indexi bilgi düzeyinde beklenen sonuçtur. R4-C standalone write freeze, R5 konsolidasyonundan önce ayrı açık onay ister.
- **R4-C — Tamamlandı / R5 approval gate:** Açık onayla `20260923093756_r4c_freeze_legacy_provider_writes` uygulandı. Standalone Meta/Google/Klaviyo/TikTok/Pinterest OAuth transaction insert'leri ile legacy connection, encrypted token, ownership, schedule ve job write'ları database trigger'larıyla fail-closed donduruldu; Shopify embedded OAuth transaction'ları korunur. Canlı preflight, postcheck ve gerçek negatif yazma denemesi `PASS` verdi. Aktif `1` Google, `1` Klaviyo ve parked `1` TikTok schedule durduruldu. Dokuz gündür açık kalan `1` Google queued ve `1` TikTok running automation job silinmeden `failed` yapıldı; Klaviyo açık job sayısı zaten `0` idi. Legacy `8` connection ve `7` encrypted token değişmedi; plaintext token `0`, kanonik connection `0` kaldı. Uygulama katmanında standalone route, save ve refresh-job guard'ları ile cron provider filtresi hazırlandı; deployment yapılmadı. Provider revoke/decrypt/re-encrypt/veri taşıma yapılmadı. R5 ayrı açık onay ister.
- **R5:** Eşleşen Klaviyo account ID destekleyici kanıttır, tek başına tenant sahipliği değildir. Embedded token yalnız ayrı açık provider-temas onayıyla Account API'de doğrulanır. Başarılı doğrulama sonrasında eski connection local `migrated/disabled` olur; historical V1/snapshot korunur; rollback süresi bitmeden token envelope temizliği yapılmaz.
- **R5-A — Tamamlandı / R5-B verification gate:** Canlı redacted envanterde `2` legacy Klaviyo satırı, `1` Shopify embedded connected Klaviyo ve `0` canonical Klaviyo bulundu. Legacy satırlardan yalnız biri embedded account ID ile eşleşir; ikinci satırda seçilmiş account yoktur. OAuth kayıtlarında legacy `user_id` ile `workspace_id` birlikte bulunmadığı ve üyelik/binding tablosu olmadığı için otomatik tenant eşlemesi reddedildi. Açık R5 onayıyla additive `20260923132407_create_legacy_user_workspace_bindings` migration'ı uygulandı ve tablo bilinçli olarak `0` satır bırakıldı. Tek legacy user için birden fazla aktif workspace eşlemesi unique index ile engellenir; RLS + force RLS açık, browser rolleri kapalı, service role explicit CRUD yetkilidir. Provider teması, account taşıma, canonical insert, legacy disable, revoke veya token silme yapılmadı. R5-B, embedded token'ın Klaviyo Account API'de ayrı açık provider-temas onayıyla doğrulanmasını ve matching legacy aday için insan attestation kaydını bekler.
- **R5-A human binding + R5-B repository hazırlığı:** Kullanıcı, embedded account ID ile eşleşen eski AdsTable Klaviyo hesabının bu workspace'e ait olduğunu açıkça beyan etti. Yalnız bu legacy aday için `1` aktif `human_attested` binding yazıldı; seçilmiş hesabı olmayan ikinci legacy satır unbound kaldı. Mevcut accounts endpoint'inin `401` halinde token refresh yazısı yapabildiği görülerek salt-okunur onay kapsamında çalıştırılması reddedildi. Bunun yerine Shopify session-bound `GET /api/shopify/providers/klaviyo/accounts/verify` ve R5 operatör parametresi hazırlandı: tek Account API GET çağrısı, refresh/write yok, `401` halinde fail-closed, account/token ifşası yok. Testler `16/16` Klaviyo ve `3/3` R5 PASS. Kod henüz deploy edilmediği için provider teması ve R5-C taşıması yapılmadı.
- **R5-B canlı sonuç + R5-C kontrollü temiz reset kararı:** PR #231 merge commit `bab09110eb532e318d893c24535ce33953df63c2` production'a alındı ve `dev.adstable.app` alias'ı bu READY deployment'a bağlandı. Kullanıcının açık provider-temas onayıyla session-bound no-refresh doğrulama tam bir kez çalıştırıldı; `/api/shopify/providers/klaviyo/accounts/verify` `409` döndürdü. Supabase salt-okunur kontrolü embedded kaydın `connected`, aktif hesaplı, `USD` currency'li ve şifreli access/refresh token zarflı olduğunu doğruladı; refresh, retry, revoke, canonical insert veya token silme yapılmadı. Eski grant doğrulanamadığı için R5 v1 kanonik taşıma yolu kapatıldı ve sonuç `reauthorization_required` kabul edildi. Kullanıcı temiz başlangıç kararı verdi: R5-C, yalnız işlem anındaki ayrı açık onayla refresh token'ı bir kez revoke edecek; provider başarısından önce yerel durumu değiştirmeyecek; embedded satırı `revoked` yaparken hesap/maliyet/currency/token zarfları ve tarihsel verileri koruyacak; aynı işlemde OAuth başlatmayacaktır. Hazırlık sözleşmesi `contracts/r5-klaviyo-consolidation-v2.json` ve `docs/R5C_KLAVIYO_CONTROLLED_CLEAN_RESET.md` içindedir. Canlı revoke ve Supabase mutation henüz yapılmamıştır.
- **R5-C — Tamamlandı / R6 approval gate:** PR #232 merge commit `cb51f91f257622c9e6715fd2ae960cf9a0b596d4` ile hazırlık, PR #233 merge commit `8f183720fe1e64975ce8adabbeb8318f55257259` ile Shopify-native execution gate production'a alındı. Son deployment `READY`, target `production`, alias `dev.adstable.app` ve alias hatası `null` olarak doğrulandı. Merchant resmi `s-modal` içindeki **Remove connection** eylemiyle işlem-anı onayı verdi; session-bound `POST /api/shopify/providers/klaviyo/accounts/reset` production logunda bir kez `200` döndü. Salt-okunur Supabase postcheck `PASS`: tek embedded Klaviyo satırı `revoked`, `connected` satır `0`, canonical Klaviyo `0`; aktif hesap, aylık maliyet, source currency ve encrypted access/refresh zarfları tarihçe olarak korundu. Aktif human-attested binding `1`, Klaviyo schedule/job `0`, legacy freeze trigger `6`, plaintext legacy token `0` kaldı. Reset yeni OAuth, refresh, canonical insert, Dataset V2 yazımı veya token silme başlatmadı; Connect R6/R7 kabulüne kadar kapalıdır. R5 tamamlandı; sonraki çalışma ayrı kapsam ve onay kapısıyla R6'dır.
- **R6-A — Tamamlandı / R6-B workspace runtime gate:** 23 Eylül 2026 salt-okunur canlı preflight `PASS_PREPARATION_ONLY` verdi: `1` workspace, `0` workspace settings/reporting currency, `0` canonical connection, `0` Dataset V2 satırı, `0` aktif legacy schedule/job, `6` legacy freeze trigger ve `1` revoked embedded Klaviyo. Workspace foreign key ve unique index yerinde; Dataset V2 `user_id` hâlâ `NOT NULL`. Provider teması, migration veya database write yapılmadı. Versionlı contract `contracts/r6-workspace-provider-runtime-v1.json`, analist kararı `docs/R6_WORKSPACE_PROVIDER_RUNTIME.md` ve salt-okunur kapı `docs/security/sql/R6_WORKSPACE_RUNTIME_PREFLIGHT.sql` içindedir. R6 canlı kabulü reporting currency ve canonical connection istediği, bunları R7 oluşturduğu için eski R6↔R7 döngüsü alt kapılara ayrıldı: R6-B ortak workspace runtime, R6-C aktivasyon migration hazırlığı, R7-A currency/Connect foundation, R6-D gerçek provider kabulü, R3-C tenant sıkılaştırması, R7-B final Connected/Disconnect deneyimi. İlk aktif dilim yalnız Meta, Google Ads ve Klaviyo'dur; TikTok/Pinterest production path'e kaydedilmez.
- **R6-B — Tamamlandı / R6-C activation migration gate:** Ortak runtime boundary server-resolved workspace, aynı workspace için canonical `connected` provider kaydı ve yalnız `merchant_selected` reporting currency bulunmadan provider runner veya Dataset V2 yazısına geçmez. Request içindeki tenant alanları reddedilir; yalnız Meta/Google Ads/Klaviyo allowlist'tedir; access/refresh token sonuçta ifşa edilmez. Provider runner'lar production composition root'a kaydedilmediği için canlı çalışma kapalıdır. Repository testleri authority→connection→currency→runner→workspace write sırasını, eksik currency/connection fail-closed davranışını ve TikTok/Pinterest reddini doğrular. Provider teması, migration ve database write yapılmadı.
- **R6-C — Tamamlandı / R7-A Ready:** Açık production onayıyla preflight `PASS_PREPARATION_ONLY` verdi; `r6_dataset_v2_workspace_activation` canlı ledger'a `20260923154503` sürümüyle uygulandı ve postcheck `PASS` oldu. Dataset V2 `user_id` nullable; `workspace_id` R3-C'ye kadar staged nullable; legacy unique index/SELECT policy ve dört workspace indexi yerinde; Dataset V2 `0` satır. Advisor kontrolünde R6-C kaynaklı yeni WARN/ERROR yoktur; boş tablodaki üç workspace indexinin unused kaydı beklenen INFO'dur. Provider runtime aktive edilmedi; OAuth/provider teması/veri yazımı yapılmadı. Kanıt `docs/security/evidence/R6C_DATASET_ACTIVATION_LIVE.json` içindedir. Sıradaki iş R7-A reporting currency ve canonical Connect foundation'dır.
- **R7:** Connect kartı önce açıklama modalı açar; modal dışı close/Cancel provider teması yapmaz. OAuth top-level'dır. Account selection ve Klaviyo plan cost/currency Shopify-native modal/form ile tamamlanır. Connected kartında Disconnect bulunur; Cancel non-destructive, tarihsel analytics korunur. Resmi Shopify componentleri, session token ve mobil acceptance zorunludur.
- **R7-A1 — Repository complete / R7-A2 next:** Shopify session ile çözülen workspace için reporting currency ilk kez merchant tarafından seçilir; Shopify currency okunmaz/default yapılmaz. Currency kaydı olmadan Data Sources ve OAuth start fail-closed durur. Meta/Google Ads/Klaviyo aktif, TikTok/Pinterest Parked'tır. Connect kartı provider teması yapmadan açıklama modalı açar; yalnız modal içindeki açık eylem top-level OAuth başlatır. Callback artık eski Shopify connection tablosuna değil `workspace_provider_connections` tablosuna `pending_account_selection` yazar. Klaviyo seçim ve maliyet kaydı canonical store'a yönlendirilmiştir. Production deployment/provider teması yapılmadı. Meta ve Google Ads verified account discovery/seçimi R7-A2'de tamamlanacaktır; R6-D ve R7-B kapıları kapalıdır.
- **R7-A2 — Repository complete / production acceptance gate:** Meta `me/adaccounts`, Google Ads `ListAccessibleCustomers + customer_client` ve Klaviyo Accounts API üzerinden provider-supplied hesap kimliği/adı/source currency kaydetme anında yeniden doğrulanır. Browser workspace, account adı veya currency authority değildir. Üç aktif provider account seçimlerini Shopify-native modallarda tamamlar; Klaviyo plan cost kendi verified source currency'siyle ayrı adımda kaydedilir. TikTok/Pinterest parked kalır. Production deployment, merchant reporting currency seçimi, canlı provider teması veya canonical connection henüz yapılmadı; sonraki kapı R7-A production acceptance, ardından R6-D'dir.
- **R7-A production corrective — OAuth route/UI readiness:** İlk merchant kabulünde Klaviyo status ve OAuth start rotalarının `404` verdiği production runtime loguyla doğrulandı. Kök neden, R7-A2'de Google Ads account discovery için eklenen developer-token şartının yanlışlıkla bütün provider route composition'ını kapatması; presentation function'ın ise yalnız genel feature flag'e bakarak Connect göstermesiydi. Provider readiness artık provider bazında izole edilir: eksik Google Ads hazırlığı Klaviyo veya Meta rotalarını kapatamaz; bilinen fakat hazır olmayan provider `503 SHOPIFY_PROVIDER_NOT_CONFIGURED` döndürür. İlk currency seçimi ve kısa Connect açıklaması `small-100` Shopify-native modallara taşınır; generic hata yerine güvenli hata kodu gösterilir. Corrective deployment ve merchant retest tamamlanmadan R7-A production acceptance verilmez; R6-D/R7-B kapıları kapalı kalır.
- **R8:** V1 satırları doğrudan SQL copy ile V2'ye taşınmaz. Provider re-fetch tercih edilir; mümkün değilse yalnız canonical validation/provenance geçen legacy fact yazılır. Direct/Others, sentetik fallback ve belirsiz Organic otomatik taşınmaz. Backfill resumable/idempotent ve provider bazlı coverage ölçümlüdür.
- **R9:** Read cutover provider/workspace canary ile ilerler. Error, lag, partial, FX rejection, currency consistency ve duplicate identity gözlenir. V2 read rollback'i V1 verisini değiştirmez.
- **R10:** Standalone OAuth route, refresh/automation, V1 read/write ve compatibility alanları ancak consumer-zero ve stabilization sonrasında ayrı release/migration ile emekli edilir. Snapshot retention/audit ve token deletion kararları belgelenir; destructive işlem restore kanıtından önce yapılamaz.
- **R11:** Ortak tablolarda zorunlu Shopify identity bulunmaz. Gelecekte `woocommerce_installations → workspace_id` adapter'ı eklenebilir; email/domain/provider account benzerliği workspace'leri otomatik birleştiremez. Aynı workspace'te birden fazla commerce installation ayrıca versionlı ürün kararı gerektirir.

#### Revizyonun geçiş ve kanıt kuralı

Her R paketi başlamadan zorunlu task aynasıyla ayrıntılandırılır; planlanan/gerçekleşen/sapma ayrımı korunur. Supabase DDL önce repository migration ve rollback olarak hazırlanır; canlı uygulama ayrı açık production onayı ister. Migration sonrası schema/constraint/RLS/grant sorguları, Supabase security/performance advisor, ilgili unit/integration/security testleri ve redacted acceptance evidence zorunludur. Bir R paketinin `PASS` olması sonraki provider teması, production mutation veya destructive retirement için örtük onay değildir.

## 2. V3 gerçekleşme haritası

| V3 fazı | Planlanan | Doğrulanmış gerçekleşen | V4 kararı |
|---|---|---|---|
| Phase 1 | Funnel Core iskeleti | Canonical contract, hierarchy, analysis scope, Formula/Time/FX servisleri, repository arayüzü ve Query Service uygulanmış; yerel senaryolar mevcut | **Tamamlandı — koruma/regresyon kapsamı** |
| Phase 2 | Dataset V2 migration | Migration, corrective migration ve Supabase repository uygulanmış; canlı tablo mevcut | **Kod artefaktı tamam; canlı kabul E2'de açık** |
| Phase 3 | Meta adapter | Provider→canonical→V2 production vertical slice | **Açık — E4** |
| Phase 4 | Google adapter | Standard mapping; PMax contract hazırlığı | **Done — E5 canlı V2-primary kabulü tamamlandı** |
| Phase 5 | TikTok adapter | Gerçek metrics ve synthetic ayrımı | **Parked — tamamlanmış artefaktlar korunur; OAuth/refresh/activation kapalı** |
| Phase 6 | Klaviyo adapter | Campaign/Flow/Message ve Email/SMS | **Açık — E7** |
| Phase 7 | GA4 Organic | Property/domain/timezone/currency/provenance | **Parked — capability korunur; ingest/connect yüzeyi kapalı** |
| Phase 8 | Shopify Public Embedded Foundation | Install/auth, shop-workspace, minimum scope, billing ve review readiness | **Açık — E10** |
| Phase 9 | Funnel API | Shopify-aware, authenticated ve scope-aware backend output | **Açık — E11** |
| Phase 10 | Embedded dashboard binding | App Bridge shell ve presentation-only Funnel UI | **Açık — E12** |
| Phase 11 | Parity/geçiş | Uçtan uca zincir doğrulaması | **E4–E13 boyunca zorunlu kapı** |

> Phase 1 ve Phase 2'nin `Done` işareti yalnız kendi önceki faz sınırları içindir. Canlı DB kabulü, runtime ingest ve production binding'in tamamlandığı anlamına gelmez.

### 2.1 Canonical provider envelope ve capability-aware hierarchy — V4 freeze

V3 §10.2'nin ana kararı yalnız entity seviyelerinin farklılığı değildir. **Meta, Google, TikTok, Klaviyo ve GA4 kaynaklı Organic dahil bütün adapter'ların aynı canonical envelope'a normalize edilmesidir.** Bu standart provider'ların farklı API şekillerini Dataset V2, Formula Engine, Funnel API ve UI için tek dile çeviren mimari omurgadır. Bir adapter'ın bu envelope dışına çıkması provider-specific şemaları yeniden bütün katmanlara sızdırır ve sistemi başlangıç noktasına döndürür.

Bu nedenle aşağıdaki envelope V4'te E0–E14 boyunca **değiştirilemez cross-cutting contract ve acceptance gate** olarak freeze edilmiştir. Adapter'lar yalnız alanların değerini ve provider capability'sine göre support durumunu belirler; blokları kaldırmaz, yeniden adlandırmaz veya provider'a özel paralel payload üretmez.

Canonical model provider'da bulunmayan bir seviyeyi uydurmaz. Her leaf satır provider'ın gerçekten desteklediği en düşük analytical entity'yi temsil eder; root ve parent lineage açıkça taşınır.

| Capability branch | Zorunlu canonical hierarchy | Yasaklanan sentetik davranış |
|---|---|---|
| Meta Paid | `Campaign → AdSet → Ad` | AdSet'i AdGroup olarak yeniden adlandırmak veya lineage'ı düşürmek |
| Google Standard | `Campaign → AdGroup → Ad` | Campaign/AdGroup lineage'ı olmayan leaf üretmek |
| Google PMax | `Campaign(type=performance_max) → Asset Group` | Sahte AdGroup veya Ad üretmek |
| TikTok Paid | `Campaign → AdGroup → Ad` | Aynı fact'i birden fazla seviyede toplayarak double-count üretmek |
| Klaviyo Campaign | `Campaign → Campaign Message` | Sahte AdGroup/Ad seviyesi üretmek |
| Klaviyo Flow | `Flow → Flow Message` | Flow'u Campaign altına yerleştirmek veya sentetik `Email Flow` parent üretmek |
| GA4 Organic | `Platform-level Organic identity` | Organic satırı Campaign/AdGroup/Ad altına zorlamak |

#### Tek standart canonical envelope

```json
{
  "identity": {
    "user_id": "uuid",
    "platform": "meta|google|tiktok|klaviyo",
    "traffic_type": "paid|organic",
    "source_system": "meta_ads|google_ads|tiktok_ads|klaviyo|ga4",
    "channel": "email|sms|null",
    "platform_account_id": "string",
    "date": "YYYY-MM-DD"
  },
  "entity": {
    "campaign_type": "standard|performance_max|null",
    "root_entity_type": "campaign|flow|organic|null",
    "root_entity_id": "string|null",
    "root_entity_name": "string|null",
    "parent_entity_type": "adset|adgroup|campaign|flow|null",
    "parent_entity_id": "string|null",
    "parent_entity_name": "string|null",
    "entity_type": "ad|asset_group|campaign_message|flow_message|organic",
    "entity_id": "string",
    "entity_name": "string"
  },
  "raw_metrics": {
    "impression": "number|null",
    "ad_click": "number|null",
    "session": "number|null",
    "spend_value": "number|null",
    "add_to_cart": "number|null",
    "add_to_cart_value": "number|null",
    "checkout": "number|null",
    "checkout_value": "number|null",
    "purchase": "number|null",
    "purchase_value": "number|null"
  },
  "metric_support": {
    "impression": "supported|unsupported|unknown",
    "ad_click": "supported|unsupported|unknown",
    "session": "supported|unsupported|unknown",
    "spend_value": "supported|unsupported|unknown",
    "add_to_cart": "supported|unsupported|unknown",
    "add_to_cart_value": "supported|unsupported|unknown",
    "checkout": "supported|unsupported|unknown",
    "checkout_value": "supported|unsupported|unknown",
    "purchase": "supported|unsupported|unknown",
    "purchase_value": "supported|unsupported|unknown"
  },
  "currency": {
    "source_currency": "USD",
    "target_currency": "TRY",
    "fx_rate": 1,
    "fx_rate_date": "YYYY-MM-DD",
    "fx_provider": "provider",
    "fx_engine_version": "vN"
  },
  "time": {
    "source_timezone": "IANA timezone",
    "business_date": "YYYY-MM-DD",
    "time_engine_version": "vN"
  },
  "provenance": {
    "source_system": "meta_ads|google_ads|tiktok_ads|klaviyo|ga4",
    "adapter_version": "vN",
    "source_confidence": "real|fallback|partial",
    "synthetic": false,
    "ga4_property_id": "string|null",
    "raw_reference": {}
  }
}
```

Envelope her adapter için aynı yedi bloğu taşır: `identity`, `entity`, `raw_metrics`, `metric_support`, `currency`, `time`, `provenance`. `identity.date` ile `time.business_date` aynı canonical business date'i ifade eder. `identity.source_system` ile `provenance.source_system` aynı olmalıdır. Her raw metric anahtarı karşılık gelen bir `metric_support` anahtarıyla birlikte bulunur.

Provider'da bir metrik veya entity seviyesi bulunmuyorsa contract değiştirilmez: değer `null`, support durumu `unsupported|unknown` olur ve provenance sebebi açıklar. Gerçek ölçülen `0` ise `supported` olarak korunur. Provider'a özgü ek ham detay gerekiyorsa canonical alanları değiştirmek yerine redacted `provenance.raw_reference` veya versionlı adapter evidence içinde tutulur; Formula Engine bu provider-specific ayrıntıya bağımlı olamaz.

Entity alanlarının geçerli kombinasyonu capability branch tarafından belirlenir. Alanın provider'da bulunmaması durumunda değer `null` kalır; görünen ad, placeholder ID veya sentetik entity ile doldurulmaz. Stable `entity_key`, identity ve hierarchy branch'inden deterministik üretilir; frontend görünen isimlerden identity üretmez.

#### Canonical envelope invariants

- Bütün provider ve GA4 Organic adapter'ları aynı yedi top-level bloğu eksiksiz üretir.
- Platforma özel alternatif raw fact DTO'su Dataset V2 repository sınırını geçemez.
- `source_system`, `traffic_type`, `channel` ve platform kombinasyonu canonical validation'dan geçer.
- On raw metric anahtarının tamamı ve birebir support anahtarları bulunur.
- `supported` metrik finite number taşır; gerçek `0` geçerlidir. `unsupported|unknown` metrik yalnız `null` taşır.
- Time normalization tamamlanmadan `identity.date/time.business_date`; FX tamamlanmadan monetary facts production-ready sayılmaz.
- Organic satır `source_system=ga4` ve `ga4_property_id` provenance taşır; GA4 bir paid platform olarak modellenmez.
- `synthetic=true` production canonical performance olarak Dataset V2'ye yazılamaz.
- Contract, adapter, time ve FX version provenance'ı yeniden üretim ve parity için izlenebilir olur.
- Derived KPI'lar bu envelope'un raw fact kaynağına yazılmaz; aggregate sonrası Formula Engine tarafından hesaplanır.

#### Hierarchy kabul kriterleri

- Adapter output'u ilgili capability branch'in root/parent/leaf kombinasyonunu taşır.
- Aynı provider ID ve branch aynı deterministik `entity_key`i üretir.
- Klaviyo Campaign Message ve Flow Message aynı leaf ID'ye sahip olsa bile branch identity nedeniyle çakışmaz.
- PMax, Klaviyo ve Organic için olmayan canonical seviyeler `null` kalır.
- Dataset V2 round-trip hierarchy ve lineage alanlarını kayıpsız korur.
- Funnel API stable identity ile capability-aware child ilişkisi döndürür.
- Funnel UI yalnız API hierarchy'sini render eder; klasik Ad hierarchy'sine zorlamaz.
- Aggregate yalnız seçilen analytical grain'deki canonical leaf fact'leri toplar; parent/leaf double-count oluşmaz.

#### Hierarchy test kapısı

- Her branch için accepted golden fixture.
- Her yasak sentetik şekil için canonical ve DB rejection fixture'ı.
- Deterministic key ve same-ID/different-branch collision testi.
- Provider raw→canonical→V2 round-trip lineage testi.
- Parent/leaf double-count negatif testi.
- API drilldown ve UI capability render contract/E2E testi.

#### Canonical envelope test kapısı

- Meta, Google Standard, Google PMax, TikTok, Klaviyo Email/SMS ve GA4 Organic için aynı schema validator'a giren golden fixture.
- Eksik top-level blok, eksik metric/support anahtarı ve provider-specific paralel şekil rejection testleri.
- Identity/source/channel, date/business-date ve provenance/source-system cross-field invariant testleri.
- Gerçek `0`, unsupported `null`, unknown `null`, partial provenance ve synthetic rejection testleri.
- Time/FX öncesi ve sonrası envelope testi; dört monetary fact'in birlikte normalize edildiğinin kanıtı.
- Canonical envelope→Dataset V2→canonical envelope kayıpsız round-trip testi.
- Adapter/contract/time/FX version provenance ve replay testleri.

#### Contract rollback ve değişiklik yönetimi

- Hierarchy contract değişikliği adapter içinde sessizce yapılamaz; contract/adapter version artışı ve decision log gerektirir.
- Yeni branch provider/account feature flag ile açılır; eski branch verisiyle aynı aggregate'e version kontrolü olmadan karıştırılmaz.
- Hatalı hierarchy yazımında ilgili adapter version durdurulur, run ID ile etkilenen V2 satırları belirlenir ve doğrulanmış adapter ile yeniden üretilir.
- Rollback hiçbir zaman sahte entity üretmeye veya unsupported seviyeyi `0`/placeholder ile doldurmaya dönemez.
- Canonical blok/alan değişikliği yalnız versionlı contract migration, bütün adapter fixture'ları, Dataset V2 mapper, API contract ve rollback planı birlikte kabul edilirse yapılabilir.
- Tek bir provider ihtiyacı ortak envelope'u sessizce çatallayamaz; yeni capability önce ortak contract decision log'unda değerlendirilir.

### 2.2 Normalization pipeline — V4 freeze

V3 §11–12'deki Time ve FX kararları canonical envelope'un opsiyonel yardımcıları değildir. Bütün provider'lar için production fact oluşma sırası aşağıdaki tek pipeline'dır:

```text
Provider raw response
→ provider adapter mapping
→ canonical identity/entity/support validation
→ provider business-date normalization
→ monetary raw facts için tek-rate FX normalization
→ production canonical validation
→ Dataset V2 upsert
→ scope-aware aggregate
→ Formula/Compare/Intent
→ Funnel API
→ presentation-only UI/export
```

#### Time contract

- Paid satırın `source_timezone` değeri provider account metadata'sından gelir.
- Organic satırın `source_timezone` değeri GA4 Property metadata'sından gelir.
- Server UTC tarihi hiçbir provider'ın business date'i olarak kullanılamaz.
- `identity.date = time.business_date` olmalıdır ve canonical unique identity bu business date'i kullanır.
- Timezone bulunamıyorsa UTC fallback ile production fact üretilmez; satır rejection/evidence akışına gider.

#### FX contract

- FX aggregation ve Formula Engine'den önce uygulanır.
- `spend_value`, `add_to_cart_value`, `checkout_value` ve `purchase_value` aynı satırda aynı rate/date/provider ile normalize edilir.
- Aynı canonical satırın monetary alanları farklı currency veya rate halinde bırakılamaz.
- Cross-currency rate yoksa sentetik `1` kullanılmaz; satır retry/rejection akışına gider.
- Aynı currency durumunda rate `1` gerçek, izlenebilir normalization sonucu olarak taşınır.

#### Pipeline kabul/test/rollback kapısı

- Hiçbir adapter Dataset V2'ye Time/FX ve production canonical validation'ı atlayarak yazamaz.
- Her provider için timezone boundary/DST ve currency fixture'ları bulunur.
- Dört monetary metric'in aynı rate ile dönüştüğü ve source provenance'ın korunduğu test edilir.
- Missing timezone/rate, mixed currency ve invalid rate negatif testleri zorunludur.
- Time/FX engine version değişikliği decision log, version bump, parity ve hedefli replay planı gerektirir.
- Hatalı engine version feature flag ile durdurulur; run/version ile etkilenen facts yeniden üretilir. UTC fallback veya sentetik FX rollback değildir.

### 2.3 Analysis Scope, aggregation ve Formula Engine — V4 freeze

V3 §13–14'teki business math tek backend standardıdır. Dataset V2 yalnız normalized raw fact source-of-truth'tur; UI, export, adapter veya repository ayrı formül motoru olamaz.

#### Analysis Scope contract

```text
PAID:
  funnel_click = paid.ad_click

ORGANIC:
  funnel_click = organic.session

PAID_ORGANIC_BLEND:
  additive paid raw facts + additive organic raw facts
  → derived metrics toplam raw facts üzerinden yeniden hesaplanır

INTENT:
  Paid-only
```

Blend, Paid ve Organic satır KPI'larının ortalaması değildir. Organic yalnız seçili AdsTable platform hesabına deterministic olarak eşleşmiş GA4 facts'ten gelir. En az bir analysis scope aktif kalır.

#### Aggregate-first Formula contract

Önce aynı scope ve grain içindeki on canonical raw metric toplanır; sonra derived değerler hesaplanır:

```text
sales           = purchase_value
abandoned       = max(checkout - purchase, 0)
abandoned_value = max(checkout_value - purchase_value, 0)
ctr             = funnel_click / impressions * 100
cpc             = spend / funnel_click
roas            = sales / spend
cps             = spend / purchase
profit          = sales - spend
margin          = profit / sales * 100
```

Intent Paid-only oranları:

```text
add_to_cart_rate = paid_add_to_cart / paid_ad_click * 100
checkout_rate    = paid_checkout / paid_add_to_cart * 100
abandoned_rate   = paid_abandoned / paid_checkout * 100
purchase_rate    = paid_purchase / paid_checkout * 100
```

#### Formula invariants

- Oranlar toplanmaz veya satır oranlarının ortalaması alınmaz: `SUM(raw numerator) / SUM(raw denominator)` kullanılır.
- Denominator `0`, unsupported veya hesaplanamazsa derived sonuç `null` olur.
- Unsupported/unknown additive input kısmi toplamı sessizce gerçek toplam gibi sunulmaz; support sonucu propagate edilir.
- `sales - spend` canonical adı `profit`tir; `revenue` olarak kalıcılaştırılmaz.
- Campaign ve child facts aynı total içinde double-count edilmez.
- Compare iki period için aynı Formula Engine'i kullanır: `(current - previous) / abs(previous) * 100`; previous `0` ise change `null`dır.
- Different-length period normalization gerekiyorsa tek versionlı backend policy olur.
- Funnel Table, Compare, Intent ve Export aynı Dataset V2/engine output'unu tüketir.

#### Formula kabul/test/rollback kapısı

- Aynı aggregate fixture Formula, API, Compare, Intent, Export ve UI'da aynı sonucu verir.
- Blend aggregate-first sonucu ile yanlış KPI-average sonucu arasındaki negatif test bulunur.
- Zero denominator, unsupported propagation, abandoned floor, profit naming ve hierarchy double-count testleri zorunludur.
- Formula değişikliği `formula_engine_version`, golden parity, decision log ve önceki versiona read rollback gerektirir.
- Frontend veya adapter'da duplicate formula tespit edilirse production acceptance verilmez.

### 2.4 Dataset V2 grain, identity ve transition — V4 freeze

Dataset V2, snapshot geçmişi veya derived sonuç deposu değil, Funnel'ın daily canonical raw fact source-of-truth katmanıdır.

#### Tek canonical grain

```text
1 user
+ 1 platform
+ 1 platform account
+ 1 provider business date
+ 1 traffic type
+ 1 gerçek capability-aware leaf entity
```

Mantıksal unique key:

```text
user_id
+ platform
+ platform_account_id
+ business_date
+ traffic_type
+ entity_key
```

- Aynı key ile refresh yeni satır üretmez; idempotent UPSERT yapar.
- `snapshot_id` canonical identity'ye girmez.
- CTR, CPC, ROAS, CPS, abandoned, profit, margin ve rate'ler raw Dataset V2 facts değildir.
- Derived cache gerekirse Dataset V2'den ayrı olur ve `formula_engine_version` taşır.
- Direct/Others final analytical Dataset grain'ine girmez.

#### Organic account mapping contract

GA4 Organic satırın analytical `platform_account_id` değeri GA4 Property ID değildir; deterministic olarak eşleşmiş AdsTable Meta/Google/TikTok/Klaviyo platform hesabıdır. Gerçek GA4 Property ID `provenance.ga4_property_id` olarak ayrı kalır. Deterministic match yoksa canonical Organic row yazılmaz; unmapped evidence olarak tutulur.

#### V1/V2 transition contract

- Migration boyunca refresh, Legacy Snapshot ve Canonical Dataset V2'ye kontrollü dual-write yapabilir.
- Snapshot capture evidence, job/debug history ve legacy compatibility rolünü korur.
- Dataset V2 Funnel, Paid/Organic/Blend, Compare, Intent ve Export'un yeni source-of-truth'udur.
- Operational Dashboard/Auth/Connect/Account/Refresh/Job lifecycle cutover'dan etkilenmez.
- V1 read ve legacy analysis yalnız parity, consumer-zero ve rollback süresi tamamlanınca E14'te emekli edilir.

#### Dataset kabul/test/rollback kapısı

- Same-key upsert, different date/entity isolation ve concurrent retry testleri zorunludur.
- Organic platform-account/property ayrımı ve unmatched rejection test edilir.
- Raw tabloda derived KPI veya snapshot-version duplication bulunamaz.
- Dual-write run'ında V1 no-change ve V1/V2 raw parity evidence üretilir.
- V2 read flag kapatılabilir; legacy yol stabilizasyon boyunca korunur. Destructive retirement yalnız E14 kapsamındadır.

### 2.5 Backend analysis boundary ve deferred capability — V4 freeze

- Funnel browser'dan Supabase'e doğrudan bağlanamaz; authenticated Funnel API tek analysis boundary'dir.
- API user/account ownership, query bounds, Dataset V2 read, scope-aware aggregate, Formula, Compare ve Intent orchestration'ın sahibidir.
- UI ve Export hazır backend contract'ını tüketir; business anlamını değiştiremez.
- Intent yalnız Paid scope'tur; Organic/Blend facts Intent oranlarına karıştırılmaz.
- Top Selling/Ranking core acceptance değildir. Ürün kararıyla açılırsa ayrı backend Ranking Engine Dataset V2'yi capability-aware tüketir; farklı entity tiplerini sahte Ad üreterek aynı leaderboard'a zorlamaz.
- Klaviyo automatic Email Spend ayrı ürün/araştırma kararıdır; mevcut manual/estimated değer yalnız provenance'ı açık fallback olabilir ve provider gerçek spend ile karıştırılamaz.

Bu boundary'lerden sapma yeni provider ihtiyacı gerekçesiyle epic içinde yapılamaz; versionlı contract/decision log ve bütün consumer parity'si gerekir.

### 2.6 V3 contract → V4 enforcement matrisi

| V3 teknik standardı | V4 freeze/gate | Uygulama epic'leri |
|---|---|---|
| §10.1 Platform/source/channel | Canonical envelope invariants | E0, E4–E8 |
| §10.2 Capability-aware hierarchy | Hierarchy matrix, deterministic identity ve sentetik seviye yasağı | E0, E2, E4–E12 |
| §10.3 Metric Support/NULL | Envelope invariant, support propagation ve UI state | E2, E4–E12 |
| §10.4 Organic account mapping | Deterministic AdsTable account + ayrı GA4 provenance | E2, E8, E9 |
| §10.5–10.7 Klaviyo channel/spend | Ortak envelope, channel ve provenance/fallback ayrımı | E7 |
| §11 Time Engine | Provider/Property timezone ve UTC fallback yasağı | E4–E9 |
| §12 FX Engine | Formula öncesi dört monetary fact için tek rate | E4–E9 |
| §13 Analysis/Formula | Backend-only Paid/Organic/Blend ve versionlı formulas | E11–E12 |
| §14 Aggregation | Aggregate-first, ratio-average ve double-count yasağı | E4–E12 |
| §15 Dataset V2 | Raw source-of-truth, canonical grain ve unique upsert | E2, E4–E11 |
| §16 V1/V2 transition | Dual-write, parity, operational compatibility | E4–E9, E13–E14 |
| §17 Funnel API | Authenticated backend analysis boundary | E11–E12 |
| §18 Compare | Aynı engine, previous-zero `null`, versionlı period policy | E11–E12 |
| §19 Intent/Ranking | Paid-only Intent; Ranking deferred ve capability-aware | E11–E12 |

Bu matris V3 standardının yalnız “referans” olarak kalıp execution task'larında unutulmasını engeller. Bir V3 standardı uygulanırken ilgili V4 gate'in kabul, test, rollback ve evidence maddeleri task aynasına kopyalanır.

## 3. Revize epic mimarisi

| Epic | İçerik | Başlangıç şartı | Bitiş şartı |
|---|---|---|---|
| **E0** | Plan freeze, baseline ve mimari sınırlar | Mutabakat | V4 plan ve modül sınırları onaylı |
| **E1** | OAuth ve session güvenliği | E0 | Güvenlik testleri geçiyor |
| **E2** | Dataset V2 canlı kabulü | E1 | DB/RLS evidence paketi tamam |
| **E3** | Backend modularization foundation | E1 | Yeni işler `server.js` dışında geliştirilebiliyor |
| **E4** | Meta vertical slice | E2 + E3 | Dual-write ve parity kabulü |
| **E5** | Google Standard/PMax adapter | E4 | Google parity kabulü |
| **E6** | TikTok adapter | E4 | TikTok parity kabulü |
| **E7** | Klaviyo adapter | E4 | Campaign/Flow/channel kabulü |
| **E8** | GA4 Organic adapter | E4 | Organic provenance kabulü |
| **E9** | Backfill ve data readiness | İlgili adapter | Coverage/parity eşikleri sağlanmış |
| **E10** | Shopify Public Embedded Foundation | E9 implementation + ürün GO kararı | Review-ready install/auth/tenant/billing/embedded foundation |
| **E11** | Funnel API | E10 + E2 + E3 + gerçek V2 veri | Shopify-aware API security/contract kabulü |
| **E12** | Shopify Embedded dashboard ve Funnel UI binding | E10 + E11 + parity | Embedded UI canary kabulü |
| **E13** | Production cutover | E4–E12 | Full production GO |
| **E14** | Legacy retirement ve monolit kapanışı | Stabilizasyon dönemi | V1 consumer sıfır; legacy yüzey kaldırılmış |

### 3.1 Bağımlılık grafiği

```text
E0 → E1 ─┬→ E2 ───────────────┬→ E4 → E5/E6/E7/E8 → E9 ─┐
         └→ E3 ───────────────┤                           │
                              └─────────────────────────────┴→ E10 Shopify Foundation
                                                                  ↓
                                                         E11 Funnel API → E12 Embedded UI
                                                                  ↓
                                                              E13 → E14
```

E5–E8, Meta referans vertical slice kabul edildikten sonra kapasiteye göre paralel yürütülebilir. E9 her provider için ayrı cursor ve readiness durumu taşır.

## 4. E0 — Plan freeze, baseline ve mimari sınırlar

**Durum:** `Done` — V4 mutabakatıyla baseline oluşturuldu; repository kabulü commit/PR ile kanıtlanacaktır.

### Planlanan işler

- **E0-T1:** V3, Final Rapor ve V4 belge hiyerarşisini freeze et.
- **E0-T2:** Epic/task durum sözlüğünü ve zorunlu task şablonunu kabul et.
- **E0-T3:** `server.js` ve `dashboard.html` sorumluluk envanterini çıkar.
- **E0-T4:** Hedef backend/frontend modül sınırlarını karar kaydına bağla.
- **E0-T5:** Feature flag, evidence ve decision-log isimlendirmesini belirle.
- **E0-T6:** V3 §10.2 yedi bloklu canonical provider envelope ve capability-aware hierarchy matrixini V4 cross-cutting contract olarak freeze et.
- **E0-T7:** Time/FX pipeline, Analysis Scope/Formula/Aggregation, Dataset grain/transition ve backend analysis boundary contract'larını V4 enforcement matrisine bağla.

### Kabul kriterleri

- Tek execution takip belgesi V4'tür.
- V3 teknik referans, Final Rapor baseline olarak korunur.
- Epic bağımlılıkları ve GO/NO-GO sahipleri bellidir.
- Monolit büyütmeme ve dokunurken çıkarma kuralları onaylıdır.
- Her iş zorunlu task aynasını kullanır.
- Her adapter için aynı `identity/entity/raw_metrics/metric_support/currency/time/provenance` envelope'u zorunludur.
- Her provider branch için root/parent/leaf, deterministic key ve yasak sentetik şekiller bellidir.
- V3 §10.1–§19 arasındaki her cross-cutting standardın sahibi, uygulama epic'i ve acceptance gate'i bellidir.

### Test / kontrol

- Markdown link ve başlık kontrolü.
- V3 fazlarının V4 epic'lerinde karşılığı olduğunun izlenebilirlik kontrolü.
- V3 §10.1–§19 contract→V4 enforcement matrisi completeness kontrolü.
- Epic'lerde kabul, test, rollback ve bağımlılık alanlarının varlık kontrolü.

### Rollback

Belge geri alınabilir; V3 ve Final Rapor değişmediği için teknik baseline kaybolmaz. V4 değişikliği yeni sürüm ve decision log ile yapılır, geçmiş sessizce yeniden yazılmaz.

### Bağımlılıklar

Mutabakat dışında bağımlılık yoktur.

### Evidence

- V4 dosyasının repository commit'i.
- PR incelemesi ve mutabakat kaydı.

## 5. E1 — OAuth ve session güvenliği

**Durum:** `Done` — E1-T1–E1-T7 tamamlandı; production OAuth/session güvenliği, fail-closed config, encrypted-only token runtime, plaintext retirement ve CI security regression kapıları kabul edildi.

### Planlanan işler

- **E1-T1 — `Done` — OAuth route envanteri ve threat model:** Tüm start/callback yolları, identity kaynakları, state, replay ve token yazma noktaları çıkarıldı; executable current-state baseline eklendi.
- **E1-T2 — `Done` — Bearer-bound identity:** Aktif OAuth başlangıçları doğrulanmış bearer kullanıcıya bağlandı; legacy query `user_id` reddedildi ve dashboard bearer-authenticated JSON handshake'e geçirildi.
- **E1-T3 — `Done` — Transaction store:** Kısa ömürlü, tek kullanımlık, atomik tüketilen OAuth transaction store; SHA-256 state özeti, 10 dakika TTL, provider/redirect/user bağları ve Klaviyo PKCE taşımasıyla kuruldu.
- **E1-T4 — `Done` — Session elimination:** E1-T3 sonrasında runtime session consumer kalmadığı doğrulandığı için kullanılmayan shared store eklemek yerine Express session katmanı tamamen kaldırıldı. Böylece MemoryStore, known fallback secret, session cookie ve multi-instance affinity riski ortadan kaldırıldı.
- **E1-T5 — `Done` — Unsafe default guard:** Review/test hard-route ve sandbox varsayılanları kapatıldı. Production'daki `UNSAFE_PRODUCTION_CONFIG`, Production scope'undaki `TIKTOK_SANDBOX_ACCESS_TOKEN` değişkeninden kaynaklandı; PR #15'in secret-free structured diagnostic'i yalnız değişken adını gösterdi. Değişken kaldırılıp yeni deployment alındıktan sonra site ve login normale döndü; hiçbir secret değeri loglanmadı ve guard beklendiği gibi fail-closed çalıştı.
- **E1-T6 — `Done` — Token protection:** E1-T6A vault, E1-T6B canlı schema/RLS/grant acceptance, E1-T6C Production activation, E1-T6D backfill ve orphan cleanup ve E1-T6E plaintext nulling tamamlandı. Final Production değerleri encryption enabled = `true`, legacy read enabled = `false`; final DB acceptance 7 connected, 7 encrypted, 0 auth-orphan, 0 missing encrypted, 0 plaintext access ve 0 plaintext refresh sonucunu verdi. Site ve login çalışıyor; legacy read kapalıyken Refresh Completed ve encrypted-only provider runtime acceptance tamamlandı. Plaintext kolonların fiziksel drop işlemi E14 Legacy Retirement kapsamına taşındı.
- **E1-T7 — `Done` — Security regression suite:** Auth, IDOR, tamper, replay ve expiry regression suite hazırlandı; dedicated `test:security` komutu eklendi. Security CI pull request ve `main` push üzerinde production secret veya environment kullanmadan security ve full regression testlerini çalıştırır.

### Kabul kriterleri

- OAuth user kimliği yalnız doğrulanmış bearer context'ten gelir.
- State ve transaction user/provider/redirect bağlamına bağlıdır; bir kez tüketilir ve sürelidir.
- Callback replay, state mismatch, expired transaction ve cross-user tamper reddedilir.
- Uygulama session secret'a ihtiyaç duymaz; unsafe review/sandbox production config fail-fast olarak reddedilir.
- OAuth callback'leri shared transaction store ile multi-instance çalışır ve session affinity gerektirmez.
- Token değerleri response, log ve test artefaktlarında görünmez.

### Test planı

- OAuth start için unauthenticated `401`.
- Query/body user ID tamper negatif testi.
- State mismatch, replay, expiry ve provider mismatch testleri.
- User A transaction'ının User B tarafından tüketilememe testi.
- Session middleware, cookie, secret fallback ve dependency elimination testi.
- OAuth transaction store TTL ve atomic consume integration testi.
- Log redaction testi.

### Rollback planı

- Provider bazlı OAuth feature flag kullanılır.
- Yeni transaction store sorununda yeni bağlantı başlatma kontrollü kapatılır; güvenli olmayan eski identity yoluna dönülmez.
- Mevcut geçerli connection kayıtları korunur.
- Bilinmeyen kritik bir session consumer bulunursa merge durdurulur; merge sonrası yeniden ekleme yalnız açık evidence ve yeni security review ile değerlendirilir. Known fallback secret hiçbir rollback senaryosunda geri getirilmez.

### Bağımlılıklar

- E0.
- Provider callback URL envanteri.

### Evidence

- Threat model.
- Route matrisi.
- Security test çıktıları.
- Redacted production-like OAuth trace.
- Config startup testleri.
- E1-T1 gerçekleşen evidence: `security/oauth-route-inventory.js`, `tests/oauth-security-baseline.test.js` ve `docs/security/E1_T1_OAUTH_SECURITY_BASELINE.md`.
- E1-T2 gerçekleşen evidence: `security/oauth-access.js`, bearer/tamper/unauthenticated acceptance testleri ve `public/dashboard.html` authenticated OAuth handshake'i.
- E1-T5 corrective evidence: `UNSAFE_PRODUCTION_CONFIG` nedeninin Production scope'undaki `TIKTOK_SANDBOX_ACCESS_TOKEN` olduğu PR #15'in secret-free structured diagnostic'iyle, secret değeri loglanmadan belirlendi. Değişken Production'dan kaldırıldı; yeni deployment sonrasında site ve login normale döndü. E1-T5 guard doğru şekilde fail-closed çalıştı.
- E1-T3 gerçekleşen evidence: `security/oauth-transaction-store.js`, atomik transaction migration'ı ve replay/expiry/provider/redirect/PKCE testleri.
- E1-T4 gerçekleşen evidence: session-elimination runtime/package guard'ları, pasif Pinterest redirect regresyonu ve güncellenmiş security baseline.

### E1-T4 task aynası

**Planlanan:**
- Production secret fail-fast ve shared TTL session store.

**Gerçekleşen:**
- Aktif runtime session consumer kalmadığı doğrulandı.
- Express session dependency/middleware/cookie bütünüyle kaldırıldı.

**Sapma gerekçesi:**
- Kullanılmayan bir shared store eklemek gereksiz altyapı ve saldırı yüzeyi oluşturacaktı.
- Session elimination aynı güvenlik hedefini daha güçlü ve daha basit biçimde sağlıyor.

**Rollback:**
- OAuth transaction store geri alınmaz; query-controlled identity veya session-bound OAuth state geri getirilmez.
- Kritik bir legacy consumer tespit edilirse değişiklik merge edilmez.
- Merge sonrasında bilinmeyen session consumer bulunursa yalnız açık evidence ve yeni security review ile session altyapısı yeniden değerlendirilir.
- Known development secret fallback hiçbir rollback senaryosunda geri getirilmez.

## 6. E2 — Dataset V2 canlı kabulü

**Durum:** `Done` — E2-T1–E2-T7 canlı veri, constraint, upsert, RLS, service-role ve cleanup kabul kapıları tamamlandı. E2-T8 açık insan iş kararıyla `Deferred` edildi ve E2 kapanış kapısından çıkarıldı. E1, E2 ve E3 bu tek closeout merge'iyle kapalıdır.

### Planlanan işler

- **E2-T1 — `Done`:** Canlı column/type/nullability introspection.
- **E2-T2 — `Done`:** Constraint, index, policy ve grant drift karşılaştırması.
- **E2-T3 — `Done`:** Canlı canonical round-trip acceptance yönetim sonucu tamamlandı.
- **E2-T4 — `Done`:** Same-key gerçek PostgreSQL upsert ve duplicate kontrolü tamamlandı.
- **E2-T5 — `Done`:** V2 preflight 18/18 PASS; 35 vakalı rollback-only canlı rejection acceptance PASS; mandatory postcheck 15/15 PASS; transaction ve postcheck retry edilmedi, production no-change korundu.
- **E2-T6 — `Done`:** V4 preflight 21/21 PASS; corrected canonical 16-case rollback-only RLS transaction evidence PASS; mandatory postcheck zero-baseline SQL shape nedeniyle fail-closed oldu; root cause redacted diagnostic ile doğrulandı; zero-safe corrective diagnostic 19/19 PASS. Transaction/postcheck retry edilmedi, production no-change korundu ve açık insan iş kabulü PR #81 sonrasında verildi.
- **E2-T7 — `Done`:** Baseline, tek final request, corrective named-baseline 19/19 PASS evidence, PR/CI ve açık insan iş kabulü tamamlandı.
- **E2-T8 — `Deferred`:** Schema-only restore'un müşteri/reklam verisini geri getirmediği ve Dataset V2 canlı kabulüne iş değeri katmadığı açık insan kararıyla kabul edildi; yeni inventory/capture/restore operationu yapılmayacaktır.

#### E2-T8 task aynası — fresh-project restore readiness

**Amaç:** Eksik historical SQL’i uydurmadan application-owned current-state baseline capture, disposable Supabase restore ve normalized acceptance için fail-closed repository hazırlığı sağlamak.

**Mevcut durum:** E2-T1–T7 `Done`; bilinen/açık Dataset V2 database acceptance açığı yoktur. E2-T8 iş değeri olmadığı yönündeki açık insan kararıyla `Deferred` edilmiştir.

**E2-T8 iş değeri ve kapanış kararı:** E2-T8 tek pakettir; alt paketlere bölünmez. Schema-only fresh-project restore müşteri/reklam verisini geri getirmediği için Dataset V2 canlı kabulüne anlamlı iş değeri sağlamaz. Bu çalışma durdurulmuş, `Deferred` edilmiş ve E2 kapanış kapısından çıkarılmıştır.

**İş çıktısı:** Dataset V2'nin production şeması, veri kuralları, idempotent upsert'i, RLS/ownership sınırları, service-role erişimi ve test kalıntısı temizliği kabul edilmiştir. E2'nin işi budur ve tamamlanmıştır.

**Kapanış:** E1 `Done`, E2 `Done`, E3 `Done`. Ownership diagnostic ilerletilmez; E2-T8 için yeni production operation veya yeni takip programı açılmaz.

**Planlanan durum:** Ayrı insan onaylı capture ve disposable Supabase restore sonrasında normalized parity evidence’ının review edilmesi; o zamana kadar `Verification`.

**Kapsam:** Scope contract, altı migration classification manifest’i, schema-only capture planı, artifact validator, source inventory, target preflight/acceptance, redacted evidence converter ve executable contract testleri.

**Kapsam dışı:** Supabase/Management API bağlantısı; production schema veya row capture; baseline SQL; target provisioning; restore operatorü/çalıştırması; migration replay; `db push`; deployment ve secret/environment değişikliği.

**Bağımlılıklar:** Onaylı main/checksum, DB ledger baseline manifest, object-by-object capture classification, ayrı capture/restore insan onayları, environment-only credential ve managed primitives doğrulanmış disposable Supabase target.

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

**Kapsam dışı:** Production Meta request, gerçek kur sağlayıcı çağrısı, Dataset V2 write, refre…38243 tokens truncated…or.
- Evidence metin/Markdown ve test çıktılarıyla sınırlıdır; E1-T6 sıradaki pakettir.
- Production incident'ında `TIKTOK_SANDBOX_ACCESS_TOKEN` değişken adı PR #15'in secret-free diagnostic'iyle güvenli biçimde belirlendi; değer kaldırılıp deployment yenilendiğinde site/login düzeldi ve hiçbir secret loglanmadı.

### E1-T6 task aynası — Provider token protection

**Planlanan:** Provider access/refresh token'larını application-level envelope encryption ile korumak; key rotation, backfill, rollback ve plaintext retirement kapılarını tanımlamak.

**Gerçekleşen (E1-T6A foundation):** AES-256-GCM token vault eklendi. Ciphertext; `user_id`, `platform` ve `token_type` AAD bağlamına bağlıdır. Raw token envelope içine yazılmaz. Version ve key ID envelope'da tutulur; önceki key'ler read-only keyring içinde kalabilir ve active key dışındaki envelope'lar rotation adayı olarak işaretlenir.

**Gerçekleşen (E1-T6B — canlı schema/grant acceptance tamamlandı):** İlk `platform_connection_tokens` migration'ı canlıda uygulandı; kolon, primary key, foreign key, envelope constraint, DDL, RLS ve grant acceptance'ı yapıldı. İlk kabulde `service_role` için gerekli CRUD'a ek `REFERENCES`, `TRIGGER` ve `TRUNCATE` yetkileri saptandı. PR #10 ile forward-only corrective migration merge edildi ve canlıda uygulandı. Post-migration kabulünde `service_role` üzerinde yalnız `SELECT`, `INSERT`, `UPDATE`, `DELETE` kaldığı; `anon`, `authenticated` ve `PUBLIC` tablo grantlerinin bulunmadığı; RLS'in enabled ve forced kaldığı doğrulandı. Böylece E1-T6B canlı schema/grant acceptance tamamlandı.

**Gerçekleşen (E1-T6C — Production activation tamamlandı):** Encrypted-only provider runtime kabulü tamamlandı. Final Production değerleri `PROVIDER_TOKEN_ENCRYPTION_ENABLED=true` ve `PROVIDER_TOKEN_LEGACY_READ_ENABLED=false` durumundadır.

**Gerçekleşen (E1-T6D — backfill ve orphan cleanup tamamlandı):** Final kabul 7 connected, 7 encrypted, 0 auth-orphan ve 0 connected-without-encrypted-token sonucunu verdi.

**Gerçekleşen (E1-T6E — plaintext nulling tamamlandı):** Global plaintext access token 0, plaintext refresh token 0 ve herhangi bir plaintext token 0 olarak kabul edildi. Encrypted envelope'lar korunmuştur. Fiziksel plaintext kolon drop işlemi E14 Legacy Retirement kapsamına taşındı.

**Sapma:** İlk backfill'de iki auth-orphan bağlantı görüldü ve guarded cleanup ile kaldırıldı. Production config incident'ı Production scope'undaki `TIKTOK_SANDBOX_ACCESS_TOKEN` nedeniyle oluştu; PR #15 secret-free diagnostic yalnız variable ismini gösterdi ve variable kaldırıldı. Plaintext kolonların fiziksel drop işlemi T6'dan E14 Legacy Retirement kapsamına taşındı.

**Kabul:** Final Production kabulü 7 connected, 7 encrypted, 0 auth-orphan, 0 connected-without-encrypted-token, 0 plaintext access ve 0 plaintext refresh sonucunu verdi. Encryption enabled = `true`, legacy read enabled = `false`; site/login başarılıdır ve legacy read kapalıyken Refresh Completed sonucu alınmıştır. Ciphertext/AAD tamper reddedilir; token veya secret log ve evidence artefaktlarına girmez.

**Rollback:** Güvenli olmayan query-controlled/session-bound OAuth identity yolu geri getirilemez ve active encryption key silinemez. Gerekli eski keyler rotation/rollback süresi boyunca keyring'de korunur; encrypted envelope'lar rollback amacıyla silinmez ve plaintext tokenlar geri yüklenmez. Runtime sorunu olursa yeni bağlantı/refresh kontrollü durdurulur; plaintext identity/token yoluna dönülmez. Fiziksel kolon drop E14 stabilizasyon kapısına kadar uygulanmaz.

**Evidence:** E1-T6B schema/RLS/grant acceptance tamamlandı. Production dry-run 9 eligible; controlled write 7 written / 2 auth-orphan failure verdi ve guarded orphan cleanup sonrasında final 7 connected / 7 encrypted / 0 auth-orphan / 0 missing encrypted kabulü alındı. Plaintext nulling sonrasında global plaintext access ve refresh sayıları 0 oldu. Production encryption `true`, legacy read `false` durumunda encrypted-only Refresh Completed sonucu alındı. PR #15 secret-free diagnostic production config incident'ında yalnız unsafe variable ismini raporladı. E1-T7 dedicated `test:security` komutu ve secretsiz CI kapısı security ve full regression paketlerini başarıyla çalıştırır.

**E1 kapanış evidence:** Production OAuth/session güvenlik kontrolleri, fail-closed production config, encrypted-only token runtime, plaintext retirement ve CI security regression tamamlandı. Site ve login çalışıyor; legacy read kapalıyken Refresh Completed sonucu alındı. E1-T7 security suite deterministik `test:security` komutunda toplandı; CI production secret/environment kullanmadan security ve full regression paketlerini çalıştırır. E1 `Done`.

**Sonraki adım:** Önce DB–Execution Plan drift kontrolü, ardından E2 Dataset V2 canlı acceptance.

**E1-T6D production dry-run acceptance ve write artefaktı (2026-08-19):** `production-token-backfill` GitHub Environment oluşturuldu; 3 variable ve 3 secret provision edildi. `main` üzerindeki `f97f1934a98016f129a1bc79263629c2ec8384fa` commit'i için **Provider token production dry-run** run `32245732566` genel, validation ve production dry-run sonuçları success oldu. Redacted sonuç: 9 scanned, 9 eligible, 0 written, 0 already encrypted, 0 rotation candidate, 0 empty, 0 failed ve `nextCursor=null`; dry-run acceptance tamamlandı. Daha sonraki controlled write 7 kayıt yazdı ve iki auth-orphan kayıtta fail-closed oldu; bu kayıtlar guarded cleanup ile kaldırıldı. Güncel production kabulü 7 connected/encrypted, 0 orphan ve 0 missing encrypted'dır; encrypted runtime refresh kabulü de tamamlanmıştır.


### E2-T6 task aynası — rollback-only Dataset V2 RLS acceptance hazırlığı

**Mevcut durum:** E2-T1/T2 `Done`; E2-T3/T4/T5/T6/T7/T8 `Verification`. E2-T6 repository preparation tamamlandı, canlı acceptance yapılmadı.

**Planlanan durum:** Ayrı insan onayından sonra exact read-only preflight, iki izole eligible kullanıcıyla tek intact rollback-only User A/User B/anon/service-role transaction, redacted evidence conversion ve read-only postcheck; review tamamlanana kadar E2-T6 `Verification`.

**Kapsam:** Exact 16-case RLS matrix, symbolic fixture contract, aggregate-only preflight/postcheck, transaction-local role/JWT claim emülasyonu, yalnız `pg_temp` evidence, ayrı nested authenticated mutation denial blokları, tek redacted response ve zorunlu final `ROLLBACK`.

**Kapsam dışı:** Canlı SQL/RLS acceptance, Management API, migration/schema/policy/grant/ledger/data değişikliği, persistent DDL, cleanup, auth/subscription/connection mutation, environment, deployment ve E2-T7.

**Test planı:** Dedicated E2-T6 artifact/converter testi; E2-T3/T4/T5, metadata ve ledger regression'ları; full/security suite; JavaScript syntax, SQL safety, allowlist ve secret/PII kontrolleri. Static testler canlı PostgreSQL acceptance değildir.

**Rollback:** Repository preparation canlı sistemi değiştirmez. Gelecekteki kontrollü operation'ın koşulsuz normal sonu `ROLLBACK`; residue halinde ad hoc cleanup ve automatic retry yasaktır.

**Gerçekleşen:** Repository preparation tamamlandı. Canlı preflight çalıştırılmadı; canlı fixture yazılmadı; User A/User B/anon/service-role canlı matrisi çalıştırılmadı; canlı postcheck çalıştırılmadı; Management API kullanılmadı; data/schema/policy/grant/ledger/deployment değişmedi.

**Durum:** `Verification` — canlı operation, postcheck ve redacted evidence insan review'ı tamamlanmadan E2-T6 `Done` değildir.


### E2-T7 task aynası — fixture cleanup ve no-change acceptance hazırlığı

**Amaç:** E2-T3–T6 outer rollback işlemleri sonrasında sıfır aggregate fixture residue ve Dataset V2/V1/snapshot ile ledger/OAuth/token/schema/RLS/policy/grant exact no-change kanıtı üretmek.

**Mevcut durum:** E2-T1–T7 `Done`; yalnız E2-T8 `Verification`. T7 named-baseline 19/19 PASS evidence, PR #89 merge ve açık insan iş kabulüyle kapandı.

**Planlanan durum:** Ayrı insan onaylı baseline, rollback-only operation serisi, final read-only parity check ve redacted evidence review; tamamlanana kadar `Verification`.

**Kapsam:** Exact T3/T4 ve escaped-prefix T5/T6 aggregate residue, V2/V1/snapshot parity, ledger/OAuth/token/schema/index/RLS/policy/grant ve persistent-object kontrolleri.

**Kapsam dışı:** Canlı SQL, otomatik/ad hoc DELETE, fixture recovery, Management API, data/schema/policy/grant/ledger/environment/deployment değişikliği ve E2-T8 restore doğrulaması.

**Bağımlılıklar:** Merge edilmiş E2-T3–T6 SQL/runbook'ları, metadata evidence, ledger baseline, exact approved main/checksum ve her canlı adım için ayrı insan onayı.

**Uygulama adımları:** Exact source doğrula; baseline gates'i çalıştır; sayımları operator-local tut; ayrı onaylı rollback-only seriyi yürüt; üç placeholder'ı lokal doldur; final check ve converter çalıştır; insan review'ı al.

**Kabul kriterleri:** Dört residue ve total sıfır; V2/V1/snapshot exact; tüm security/metadata parity PASS; persistent object sıfır; redacted evidence PASS.

**Test planı:** Dedicated artifact/converter, önceki E2, metadata, ledger, full/security, syntax, SQL safety, diff ve leak taramaları. Static testler canlı kabul değildir.

**Rollback planı:** Repository değişikliği commit revert ile geri alınır. Canlı acceptance'ın tek normal cleanup'ı transaction outer `ROLLBACK`tır; residue halinde STOP, cleanup yoktur.

**Gözlemlenebilirlik:** Yalnız allowlisted check kodları, boolean sonuçlar ve aggregate residue; production count/row/identity committed evidence'a girmez.

**Güvenlik ve veri etkisi:** Preparation-only; credential/PII/raw row yoktur. Canlı data, schema, policy, grant, ledger veya deployment etkisi oluşmadı.

**Planlanan:** İnsan onaylı canlı baseline, rollback-only seri, final no-change evidence ve review.

**Gerçekleşen:** Repository preparation tamamlandı. Canlı baseline, fixture cleanup ve final check çalıştırılmadı. Management API kullanılmadı. Data/schema/policy/grant/ledger/deployment değişmedi.

**Sapmalar:** Yok; canlı execution bilinçli olarak ayrı onaya bırakıldı.

**Evidence:** Fixture inventory, no-change contract, iki read-only SQL, redacted converter, runbook ve static regression testleri.

**Durum:** `Verification` — canlı final evidence ve insan review'ı olmadan `Done` değildir.

### E2-C1 — Captured provider-token security parity corrective kararı

**Planlanan:** E2-T3–T7 acceptance artefaktlarındaki E1 kapanış anından kalan hardcoded 7/7 provider nüfusunu, production sayısı disclosure etmeden operator-local capture ve exact parity ile değiştirmek; missing/orphan/plaintext kontrollerini zero tutmak.

**Gerçekleşen:** Management API bağlantısı HTTP 201 ile doğrulandı. E2-T3 read-only preflight çalıştı. `CONNECTED_CONNECTIONS` ve `ENCRYPTED_TOKEN_ROWS` hardcoded 7 beklentileri başarısız oldu. Diğer preflight güvenlik/schema kapıları geçti. Actual production sayıları evidence'a veya repository'ye alınmadı.

**Sapmalar:** Değişebilir production provider nüfusu nedeniyle fixed population kabulü güvenlik sözleşmesinden çıkarıldı. Bu corrective paket production data correction değildir; transaction, INSERT ve postcheck çalıştırılmadı, production değişmedi.

**Evidence:** Paylaşılabilir response yalnız captured-baseline parity sonuçlarını ve `missing_encrypted_unchanged`, `orphan_encrypted_unchanged`, `plaintext_unchanged` boolean sonuçlarını taşır. Operator-local connected/encrypted baseline source control'a alınmaz.

**Durum:** E2-T1/T2 `Done`; E2-T3–T8 `Verification` olarak korunur.

### E2-C2 — E2-T3 ordered read-back v2 corrective preparation

**Durum:** E2-T3 `Verification`; E2-T4–T8 durumları değişmedi.

**Gerçekleşen (safe/redacted):** Management API transport HTTP 201 ve updated preflight 17/17 PASS oldu. v1 transaction HTTP 201 döndü; insert/contract PASS, read-back/overall FAIL oldu. PostgreSQL same-statement snapshot semantiği nedeniyle v1 read-back tasarımı geçersizdi. v1 postcheck invalid aggregate projection nedeniyle HTTP 400 döndürdü. v1 transaction retry edilmedi. Ayrı insan-onaylı recovery sorgusu HTTP 201 ve 13/13 PASS verdi; fixture residue zero ve production no-change doğrulandı. Actual count/identity paylaşılmadı.

**Corrective hazırlık:** `e2_t3_static_v2` yeni namespace'i ve `E2_T3_TRANSACTION_V2` operation code'u kullanılır. Tek intact transaction payload'ı ordered top-level temp baseline, INSERT ve ayrı target-table read-back statement'ları ile zorunlu final `ROLLBACK` taşır. Postcheck scalar actual/expected sorgularına çevrildi. v2 eski operation'ın retry'ı değildir; yeni preflight ve ayrı insan onayı zorunludur. Bu corrective task canlı SQL çalıştırmaz; static testler live PostgreSQL acceptance yerine geçmez.

### E2-T4 corrective V2 kaydı — integer evidence ve scalar postcheck

**Durum:** E2-T3 `Done`; E2-T4 `Verification`; E2-T5–T8 durumları değişmedi.

**Canlı v1 bulguları:** v1 preflight HTTP 201 ve 16/16 PASS. v1 transaction HTTP 201; initial write, same-key upsert, final fixture row, updated contract ve duplicate-group PASS; duplicate-excess evidence contract FAIL. Final statement `ROLLBACK`; transaction retry: no. v1 postcheck HTTP 400 ve retry edilmedi.

**Recovery:** read-only recovery HTTP 201 ve recovery 11/11 PASS; fixture residue zero, Dataset V2 zero ve production no-change. Actual production counts and identities were not shared.

**Corrective kapsam:** v2 corrective preparation; `E2_T4_TRANSACTION_V2`, `e2_t4_same_key_v2` ve `e2-t4-upsert-v2`; duplicate excess explicit bigint ve postcheck tamamen scalar actual/expected bigint sözleşmesi. Bu repository taskında canlı SQL veya Management API çalıştırılmadı. E2-T4 `Verification` kalır.

### E2-C3 — E2-T6 fail-closed recovery kaydı

**Durum:** E2-T6 `Verification`; canlı PASS iddiası yoktur.

**Gerçekleşen:** İnsan onaylı E2-T6 v1 transaction bir kez gönderildi; CLI fail-closed durdu. Capsule `CONSUMED`, transaction intent ve postcheck intent kayıtlıdır; ikisi de retry edilmedi. Ayrı insan onaylı distinct read-only recovery sorgusu 19/19 PASS verdi. E2-T6 residue, total E2 residue ve persistent evidence object sıfır; Dataset V2/V1/snapshot, OAuth/token/ledger ve RLS/policy/grant korunan baseline kapıları değişmedi. Production count ve identity paylaşılmadı.

**Sapma:** v1 CLI güvenli kategorik terminal sonucu kalıcılaştırmadığı için transaction evidence failure ile original mandatory postcheck failure birbirinden sonradan ayrıştırılamadı. Recovery production no-change kanıtıdır; 16-case RLS acceptance PASS yerine geçmez. v1 transaction tekrar edilemez. Yeni canlı deneme ancak ayrı namespace/version, düzeltilmiş terminal observability, yeni preflight ve ayrı production onayıyla yapılabilir.

**Evidence:** `artifacts/dataset-v2-acceptance/e2-t6-rls/recovery-v1.json`.

### E2-C4 — E2-T6 corrective v2 terminal observability hazırlığı

**Durum:** Repository preparation; E2-T6 `Verification`, canlı işlem yapılmadı.

**Corrective kapsam:** `e2_t6_rls_v2` ayrı namespace/version; checksum-bound 21-gate preflight, intact rollback-only 16-case transaction ve 19-gate postcheck. V2, transaction evidence ve postcheck sonuçlarını `PASS`, `TRANSACTION_EVIDENCE_FAILED_POSTCHECK_PASS`, `POSTCHECK_FAILED` veya `TRANSACTION_AND_POSTCHECK_FAILED` kapalı safe-code sözleşmesiyle ayrı outcome sidecar'ında kalıcılaştırır. Sidecar repository dışındadır, `0600` modundadır ve raw hata/identity/count içermez. v1 capsule veya namespace tekrar kullanılamaz.

**Canlı sınır:** V2 preparation SQL, Management API veya production işlemi çalıştırmaz. Yeni preflight ve transaction ayrı production onaylarına tabidir.

### E2-C5 — E2-T6 v2 fail-closed recovery kaydı

**Durum:** E2-T6 `Verification`; canlı PASS iddiası yoktur.

**Gerçekleşen:** İnsan onaylı v2 transaction ve mandatory postcheck birer kez gönderildi; terminal outcome `TRANSACTION_AND_POSTCHECK_FAILED` olarak güvenli sidecar'a yazıldı. İkisi de retry edilmedi. Ayrı insan onaylı distinct read-only recovery 19/19 PASS verdi; v2/total residue ve persistent evidence object sıfır, korunan baseline ve RLS/policy/grant kapıları değişmedi.

**Karar:** Recovery production no-change kanıtıdır ve 16-case acceptance PASS yerine geçmez. E2-T7 inventory ve read-only selector'ları v1/v2 T6 namespace'lerini ayrı izler. Yeni canlı deneme hazırlanmayacaktır; transaction SQL root cause repository/static ve disposable ortamda çözülmeden production E2-T6 tekrarına izin verilmez.

**Evidence:** `artifacts/dataset-v2-acceptance/e2-t6-rls/recovery-v2.json`.

### E2-C6 — E2-T6 repository/static root-cause audit

**Durum:** `Verification`; production retry hazırlığı veya canlı PASS iddiası değildir.

**Gerçekleşen:** V2 transaction içindeki iki fixture'ın `entity_key` değerlerinin frozen canonical hierarchy üzerinden üretilmesi gereken anahtarlarla eşleşmediği repository/static olarak doğrulandı. Bu bulgu iki fixture'ı da etkiler ve transaction tasarımında kesin bir contract ihlalidir. Güvenli terminal sidecar ham database/transport hatası taşımadığından tüketilmiş transaction ile postcheck'in terminal hata nedeni geriye dönük olarak kesinleştirilemez; recovery yalnız production no-change durumunu kanıtlar.

**Karar:** Production retry yasağı korunur. Yeni bir namespace/operation hazırlanmasından önce canonical anahtarlı düzeltme ve disposable PostgreSQL ortamında transaction/postcheck reproduksiyonu zorunludur. Bu taskta SQL, Management API, production data, schema, policy, grant, ledger, environment veya deployment değişikliği yapılmadı.

**Evidence:** `artifacts/dataset-v2-acceptance/e2-t6-rls/static-root-cause-v1.json` ve `tests/e2-t6-static-root-cause.test.js`.

### E2-C7 — E2-T6 disposable PostgreSQL root-cause reproduction

**Durum:** Repository/disposable reproduction `Done`; E2-T6 production acceptance hâlâ `Verification`.

**Gerçekleşen:** PostgreSQL 16 disposable şeması historical V2 transaction'ı gerçek parser, role switching, RLS ve nested exception davranışıyla yeniden çalıştırdı. V2 final payload SQL'inde `jsonb_build_object` kapanış parantezinin ve case aggregate'in `pg_temp.e2_t6_rls_evidence` source ifadesinin eksik olduğu doğrulandı. C6'daki canonical `entity_key` ihlali de yeni V3 disposable fixture'larında düzeltildi. Corrected V3 disposable transaction 16/16 case PASS, zero unexpected allow, overall PASS ve final outer `ROLLBACK` verdi.

**Reproduction kapısı:** Redacted runner PostgreSQL 16 ile yerel/disposable ortamda çalıştırıldı; executable schema ve runner repository'de tutulur. Disposable şema yalnız sembolik iki kullanıcı ve boş korunan tablolar içerir; production credential, identity, count veya bağlantı kullanmaz.

**Karar:** Repository/static ve disposable root-cause şartı tamamlandı. Bu sonuç production acceptance değildir. Yeni production preflight/transaction operatörü hazırlanması ve çalıştırılması ayrı task, yeni namespace, review ve açık insan production onayına tabidir.

**Evidence:** `artifacts/dataset-v2-acceptance/e2-t6-rls/disposable-reproduction-v1.json`, `tests/fixtures/e2-t6-disposable-schema.sql` ve `scripts/e2-t6-disposable-reproduction.js`.

### E2-C8 — E2-T6 V3 production operator preparation

**Durum:** Repository preparation; E2-T6 production acceptance `Verification`, canlı işlem yapılmadı.

**Kapsam:** Disposable ortamda doğrulanan canonical V3 transaction; yeni `e2_t6_rls_v3` namespace'i; checksum-bound 21-gate preflight; 16-case rollback-only transaction; 19-gate postcheck; tek kullanımlık `0600` state/outcome sidecar; exact confirmation ve fail-closed terminal outcome sözleşmesi.

**Gerçekleşen:** V3 preflight/transaction/postcheck, fixture contract, evidence converter, operator/CLI ve regression testleri repository'de hazırlandı. Approved-main binding PR #44 merge commit'ine sabitlendi. Bu taskta Management API, production SQL, credential, data, schema, policy, grant, ledger, environment veya deployment değişikliği yapılmadı.

**Canlı sınır:** Preflight dahil hiçbir production isteği review ve açık insan production onayı olmadan çalıştırılamaz. Repository testleri ve disposable PASS production acceptance yerine geçmez.

### E2-C9 — E2-T6 V3 preflight fail-closed diagnostic revizyonu

**Durum:** Repository revision; production preflight retry edilmedi, E2-T6 `Verification`.

**Bulgu:** İnsan onaylı V3 read-only production preflight tek istek sonrasında genel `STOPPED_FAIL_CLOSED` ile durdu. Approval-ready state ve outcome sidecar oluşmadı; transaction/postcheck gönderilmedi ve production mutation olmadı. Genel kod query/transport-response aşaması ile 21-gate validation aşamasını ayırmadığı için kör retry yasaklandı.

**Düzeltme:** V3 preflight, raw hata/count/identity taşımayan kapalı safe-code sözleşmesiyle `PREFLIGHT_QUERY_FAILED` ve `PREFLIGHT_GATES_FAILED` aşamalarını ayırır. Her iki hata state/outcome oluşturmadan durur. Bu repository taskında Management API veya production isteği çalıştırılmadı.

**Merge sonrası yürütme sırası:** PR #46 `main` üzerine merge edilmiştir. Bu merge E2-T6 kabulünü tamamlamaz; yalnız bir sonraki read-only production preflight'in hata aşamasını güvenli biçimde ayırt edebilecek operator revizyonunu hazırlar. E2 ana iş hattındaki sıradaki karar kapısı, yeni V3 diagnostic production preflight için açık insan production onayıdır. Onay verilmeden preflight gönderilmez; preflight PASS olmadan transaction/postcheck aşamasına geçilmez; transaction/postcheck PASS ve evidence review olmadan E2-T6 `Done` olmaz; E2-T6 tamamlanmadan E2-T7 final no-change kabulü kapatılamaz.

**Paralel iş ayrımı:** E3-T1 repository characterization hazırlığı E0+E1'e bağlı bağımsız bir koruma işidir. E2-T6 veya E2-T7'nin yerine geçmez, bu iki taskın durumunu değiştirmez ve E2 ana iş hattında yeni bir aşama tamamlandığı anlamına gelmez.

### E3-T1 task aynası — kritik V1 route characterization baseline

**Amaç:** E3 extraction başlamadan önce kritik public, config, auth ve fail-closed HTTP davranışlarını executable baseline ile sabitlemek.

**Mevcut durum:** Kök `server.js` app creation, dependency construction, route registration, provider/job orchestration, listener ve export sorumluluklarını birlikte taşır; seçili V1 HTTP sözleşmeleri için dedicated characterization testi yoktu.

**Planlanan durum:** Ephemeral loopback listener üzerinden production/provider/Supabase çağrısı yapmadan kritik status, content-type ve response shape sözleşmelerini koruyan test ve responsibility map.

**Kapsam:** Landing/login, public config, unauthenticated account status, disabled TikTok test yüzeyi, unknown API 404 ve mevcut sorumluluk envanteri.

**Kapsam dışı:** Route extraction, composition root, canlı HTTP/provider/DB isteği, environment/deployment, business logic, response contract değişikliği ve production mutation.

**Bağımlılıklar:** E0 mimari sınırları, tamamlanmış E1 güvenlik baseline'ı ve mevcut V1 entrypoint.

**Uygulama adımları:** Kritik yüzeyler seçildi; exported Express app `VERCEL=1` altında ephemeral loopback porta bağlandı; public/auth/fail-closed assertions ve extraction guard dokümante edildi; test full/security CI kapsamına alındı.

**Kabul kriterleri:** Seçili route status/body/content-type sözleşmeleri deterministik PASS; harici servis çağrısı yok; listener test sonunda kapanır; full ve security regression PASS.

**Test planı:** Dedicated E3-T1 testi, full suite, security suite, JavaScript syntax ve diff kontrolü.

**Rollback planı:** Test/doküman/package script commit'i revert edilir; runtime ve production state değişmediği için data rollback yoktur.

**Gözlemlenebilirlik:** Yalnız route adı, HTTP status ve public response shape; credential, gerçek user/account identity veya provider payload yok.

**Güvenlik ve veri etkisi:** Read-only local characterization; production, database, OAuth/token, schema, policy, grant ve deployment etkisi yok.

**Planlanan:** E3-T1 characterization baseline ve responsibility map.

**Gerçekleşen:** Repository artefaktları ve CI entegrasyonu PR #47 ile review edilmiş, bütün kontrolleri geçmiş ve `main` üzerine merge edilmiştir.

**Sapmalar:** E2-T6 production acceptance açık insan onayı gerektirdiği için üretim işlemi yapılmadı. E3-T1 yalnız bağımsız/paralel repository hazırlığı olarak yürütüldü; E2 ana iş hattının sıradaki adımı veya E2 kabulünün ikamesi değildir.

**Evidence:** `tests/e3-t1-critical-route-characterization.test.js`, `docs/architecture/e3-t1-characterization-baseline.md`, full/security test çıktıları ve PR CI.

**Durum:** `Done` — PR #47 merge commit `8728f5934005b01c0605ed7f60253127a3e4d3c2`; characterization, full/security CI ve review kapıları tamamlandı.

### E3-T2 task aynası — composition root

**Amaç:** Express application oluşturma ve process listener başlatma sorumluluklarını kök monolitten test edilebilir bir composition sınırına taşımak.

**Mevcut durum:** E3-T1 `Done`. Kök `server.js` Express instance, base middleware/static yüzey ve `listen()` lifecycle'ını doğrudan kuruyordu.

**Planlanan durum:** `src/app.js` port dinlemeden application oluşturur; listener yalnız explicit `startApplication` sınırıyla açılır; kök entrypoint mevcut route registration davranışını koruyarak bu sınıra delegation yapar.

**Kapsam:** Express app creation, trust proxy, JSON middleware, disabled TikTok static guard, public static middleware, listener başlangıcı, dependency validation ve lifecycle testleri.

**Kapsam dışı:** Route extraction, shared Supabase/provider client extraction, OAuth/job business logic taşıma, response contract, environment, deployment, database ve production işlemleri.

**Bağımlılıklar:** E3-T1 characterization baseline ve E1 production configuration guard.

**Uygulama adımları:** Application/listener factory eklendi; kök server delegation'a geçirildi; app-without-listener, explicit listener ve missing dependency negatif testleri security/full regression kapsamına alındı.

**Kabul kriterleri:** App port açmadan oluşturulabilir; yalnız explicit start listener açar; mevcut base middleware sırası ve kritik V1 characterization değişmez; invalid composition input fail-closed olur; full/security CI PASS.

**Test planı:** Dedicated E3-T2 lifecycle testi, E3-T1 characterization, full suite, security suite, JavaScript syntax ve diff kontrolü.

**Rollback planı:** `src/app.js` delegation commit'i revert edilerek önceki kök app/listener kurulumu geri alınır; data veya schema rollback yoktur.

**Gözlemlenebilirlik:** Listener başlangıç mesajı ve test lifecycle sonucu; request body, credential, identity veya provider payload loglanmaz.

**Güvenlik ve veri etkisi:** Repository/runtime composition refactor; production request, data/schema/policy/grant/token veya deployment değişikliği yoktur.

**Planlanan:** E3-T2 composition-root sınırı ve executable lifecycle evidence.

**Gerçekleşen:** Repository implementasyonu ve test entegrasyonu PR #49 ile review edilmiş, bütün kontrolleri geçmiş ve `main` üzerine merge edilmiştir.

**Sapmalar:** Shared Supabase/provider client construction E3-T4 kapsamı olarak yerinde bırakıldı; E3-T2 route/business logic taşımadı.

**Evidence:** `src/app.js`, `tests/e3-t2-composition-root.test.js`, E3-T1/full/security test çıktıları ve PR CI.

**Durum:** `Done` — PR #49 merge commit `ea4f6f8233dfb6aabe7d082881c15d7c74cf19d9`; composition-root, characterization, full/security CI ve review kapıları tamamlandı.

### E3-T3 task aynası — runtime config boundary

**Amaç:** App boot için gereken environment/configuration değerlerini tek, immutable ve fail-closed runtime sınırında toplamak.

**Mevcut durum:** E3-T1/T2 `Done`. Port, public path ve production security flags kök `server.js` içinde ayrı ayrı kuruluyordu; production güvenlik doğrulaması zaten `security/production-config.js` içinde korunuyordu.

**Planlanan durum:** `src/config/runtime-config.js` port, public directory ve mevcut production security contract'ını tek typed/immutable nesne olarak üretir; kök entrypoint yalnız bu nesneyi tüketir.

**Kapsam:** PORT default/normalization/range validation, absolute root/public path, production config delegation, immutable runtime object ve root composition delegation.

**Kapsam dışı:** Provider credential okumalarının tamamını taşıma, shared client creation, feature flag redesign, route/OAuth/job extraction, environment/deployment veya production değişikliği.

**Bağımlılıklar:** E3-T2 composition root ve E1 fail-closed production config contract'ı.

**Uygulama adımları:** Runtime config loader ve port parser eklendi; server boot yeni immutable config'e geçirildi; default/valid/invalid/unsafe-production testleri full/security suite'e bağlandı.

**Kabul kriterleri:** Runtime config immutable; default port deterministik; malformed/out-of-range port ve invalid dependency fail-closed; absolute public path deterministik; E1 unsafe-production rejection korunur; E3-T1/T2 ve full/security CI PASS.

**Test planı:** Dedicated E3-T3 config testi, production-config regression, E3-T1/T2 testleri, full/security suite, syntax ve diff kontrolü.

**Rollback planı:** Runtime config delegation commit'i revert edilerek E3-T2 sonrası server boot kurulumu geri alınır; data/schema rollback yoktur.

**Gözlemlenebilirlik:** Yalnız güvenli config error code/variable name ve test sonucu; environment value, credential veya identity loglanmaz.

**Güvenlik ve veri etkisi:** Fail-closed startup doğrulamasını merkezileştirir; production request, database/provider, data/schema/policy/grant/token veya deployment etkisi yoktur.

**Planlanan:** E3-T3 immutable runtime config ve executable validation evidence.

**Gerçekleşen:** Repository implementasyonu ve test entegrasyonu PR #50 ile review edilmiş, bütün kontrolleri geçmiş ve `main` üzerine merge edilmiştir.

**Sapmalar:** Provider-specific URL/version/account değerleri bu küçük extraction'da taşınmadı; shared/provider config kapsamı sonraki kontrollü tasklarda ele alınacaktır.

**Evidence:** `src/config/runtime-config.js`, `tests/e3-t3-runtime-config.test.js`, production-config/E3/full/security test çıktıları ve PR CI.

**Durum:** `Done` — PR #50 merge commit `38a70cab16d3d5bddbb9a9b0f368e1d0bda44e10`; runtime config, characterization, full/security CI ve review kapıları tamamlandı.

### E3-T4 task aynası — shared clients

**Amaç:** Supabase admin, OAuth transaction store ve provider-token vault/store creation'ını kök monolitten tek test edilebilir dependency graph sınırına taşımak.

**Mevcut durum:** E3-T1/T2/T3 `Done`. Shared server-side client ve store nesneleri kök `server.js` içinde doğrudan ve birbirine bağlı ifadelerle kuruluyordu.

**Planlanan durum:** `src/clients/shared-clients.js` bütün shared client/store nesnelerini explicit factory dependency'leriyle bir kez üretir; kök entrypoint yalnız immutable graph sonucunu tüketir.

**Kapsam:** Supabase service-role client options, OAuth transaction store, provider-token vault/store composition, optional dependency davranışı, factory validation ve immutable graph.

**Kapsam dışı:** Provider API client extraction, credential veya feature-flag redesign, route/OAuth/job business logic taşıma, schema/data/policy/grant ve production işlemi.

**Bağımlılıklar:** E3-T3 runtime config boundary ve E1 OAuth/provider-token güvenlik contract'ları.

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

