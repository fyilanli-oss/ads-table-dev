Warning: truncated output (original token count: 94129)
Total output lines: 3726

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
| R6 | Embedded provider runtime → canonical V2 | E4 + E5 + E7 | `In progress / R6-D2 Klaviyo live PASS; R6-D3 Meta full lifecycle live PASS; R6-D4-A0 Google Sheets park production PASS; R6-D4 Google Ads next` — Google Sheets'in eski Dataset V1 export hattında yeni OAuth, token refresh, manuel/otomatik sync ve disconnect mutation production'da fail-closed kapalıdır; tarihsel bağlantı, encrypted credential ve spreadsheet korunmuştur. Google Ads yaşam döngüsü ayrı R6-D4 analist brief'i ve onayıyla başlar. |
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
- **R6-A — Tamamlandı / R6-B workspace runtime gate:** 23 Eylül 2026 salt-okunur canlı preflight `PASS_PREPARATION_ONLY` verdi: `1` workspace, `0` workspace settings/reporting currency, `0` canonical connection, `0` Dataset V2 satırı, `0` aktif legacy schedule/job, `6` legacy freeze trigger ve `1` revoked embedded Klaviyo. Workspace foreign key ve unique index yerinde; Dataset V2 `user_id` hâlâ `NOT NULL`. Provider teması, migration veya database write yapılmadı. Versionlı contract `contracts/r6-workspace-provider-runtime-v1.json`, analist kararı `docs/R6_WORKSPACE_PROVIDER_RUNTIME.md` ve salt-okunur kapı `docs/security/sql/R6_WORKSPACE_RUNTIME_PREFLIGHT.sql` içindedir. R6 canlı kabulü reporting currency ve canonical connection istediği, bunları R7 oluşturduğu için eski R6↔R7 döngüsü alt kapılara ayrıldı: R6-B ortak workspace runtime, R6-C aktivasyon migration hazırlığı, R7-A currency/Connect foundation, R6-D gerçek provider kabulü, R3-C tenant sıkılaştırması, R7-B final Connected/Disconnect deneyimi. İlk aktif dilim yalnız Meta, Google Ads ve Klaviyo'dur; TikTok/Pinterest production path'e kaydedilmez.
- **R6-B — Tamamlandı / R6-C activation migration gate:** Ortak runtime boundary server-resolved workspace, aynı workspace için canonical `connected` provider kaydı ve yalnız `merchant_selected` reporting currency bulunmadan provider runner veya Dataset V2 yazısına geçmez. Request içindeki tenant alanları reddedilir; yalnız Meta/Google Ads/Klaviyo allowlist'tedir; access/refresh token sonuçta ifşa edilmez. Provider runner'lar production composition root'a kaydedilmediği için canlı çalışma kapalıdır. Repository testleri authority→connection→currency→runner→workspace write sırasını, eksik currency/connection fail-closed davranışını ve TikTok/Pinterest reddini doğrular. Provider teması, migration ve database write yapılmadı.
- **R6-C — Tamamlandı / R7-A Ready:** Açık production onayıyla preflight `PASS_PREPARATION_ONLY` verdi; `r6_dataset_v2_workspace_activation` canlı ledger'a `20260923154503` sürümüyle uygulandı ve postcheck `PASS` oldu. Dataset V2 `user_id` nullable; `workspace_id` R3-C'ye kadar staged nullable; legacy unique index/SELECT policy ve dört workspace indexi yerinde; Dataset V2 `0` satır. Advisor kontrolünde R6-C kaynaklı yeni WARN/ERROR yoktur; boş tablodaki üç workspace indexinin unused kaydı beklenen INFO'dur. Provider runtime aktive edilmedi; OAuth/provider teması/veri yazımı yapılmadı. Kanıt `docs/security/evidence/R6C_DATASET_ACTIVATION_LIVE.json` içindedir. Sıradaki iş R7-A reporting currency ve canonical Connect foundation'dır.
- **R7:** Connect kartı önce açıklama modalı açar; modal dışı close/Cancel provider teması yapmaz. OAuth top-level'dır. Account selection ve Klaviyo plan cost/currency Shopify-native modal/form ile tamamlanır. Connected kartında Disconnect bulunur; Cancel non-destructive, tarihsel analytics korunur. Resmi Shopify componentleri, session token ve mobil acceptance zorunludur.
- **R7-A1 — Repository complete / R7-A2 next:** Shopify session ile çözülen workspace için reporting currency ilk kez merchant tarafından seçilir; Shopify currency okunmaz/default yapılmaz. Currency kaydı olmadan Data Sources ve OAuth start fail-closed durur. Meta/Google Ads/Klaviyo aktif, TikTok/Pinterest Parked'tır. Connect kartı provider teması yapmadan açıklama modalı açar; yalnız modal içindeki açık eylem top-level OAuth başlatır. Callback artık eski Shopify connection tablosuna değil `workspace_provider_connections` tablosuna `pending_account_selection` yazar. Klaviyo seçim ve maliyet kaydı canonical store'a yönlendirilmiştir. Production deployment/provider teması yapılmadı. Meta ve Google Ads verified account discovery/seçimi R7-A2'de tamamlanacaktır; R6-D ve R7-B kapıları kapalıdır.
- **R7-A2 — Corrected account-cardinality contract / production acceptance gate:** Meta `me/adaccounts` ve Google Ads `ListAccessibleCustomers + customer_client` üzerinden provider-supplied hesap kimliği/adı/source currency kaydetme anında yeniden doğrulanır; kullanıcı aynı OAuth grant'i altında en az 1, en fazla 3 reklam hesabı seçer. Klaviyo Accounts API üzerinden tek doğrulanmış hesap seçilir ve plan cost bu hesabın verified source currency'siyle ayrı adımda kaydedilir. Browser workspace, account adı veya currency authority değildir. `active_account_id` geriye uyumluluk alanıdır; tam bağlantı kapsamı `selected_accounts`, Funnel için tek aktif hesap tercihi ise ayrı Settings kararıdır. TikTok/Pinterest parked kalır. Additive cardinality migration'ı, production deployment, merchant reporting currency seçimi, canlı provider teması ve canonical connection ayrı kabul kapısındadır; ardından R6-D gelir.
- **R7-A production corrective — OAuth route/UI readiness:** İlk merchant kabulünde Klaviyo status ve OAuth start rotalarının `404` verdiği production runtime loguyla doğrulandı. Kök neden, R7-A2'de Google Ads account discovery için eklenen developer-token şartının yanlışlıkla bütün provider route composition'ını kapatması; presentation function'ın ise yalnız genel feature flag'e bakarak Connect göstermesiydi. Provider readiness artık provider bazında izole edilir: eksik Google Ads hazırlığı Klaviyo veya Meta rotalarını kapatamaz; bilinen fakat hazır olmayan provider `503 SHOPIFY_PROVIDER_NOT_CONFIGURED` döndürür. İlk currency seçimi ve kısa Connect açıklaması `small-100` Shopify-native modallara taşınır; generic hata yerine güvenli hata kodu gösterilir. Corrective deployment ve merchant retest tamamlanmadan R7-A production acceptance verilmez; R6-D/R7-B kapıları kapalı kalır.
- **R7-A account-cardinality activation — migration ve application deployment PASS / merchant acceptance next:** Açık production onayıyla salt-okunur database preflight `PASS` verdi: canonical connection tablosu `0` satır, connected/invalid kayıt `0`, RLS ve FORCE RLS açık. Yalnız additive `20260924120453_add_workspace_provider_selected_accounts` migration'ı uygulandı. Postcheck `PASS`: kolon `NOT NULL DEFAULT []`, üç cardinality constraint validated, browser grant `0`, canonical satır `0`. PR #242 merge commit `a94201bf74c76cc57b5a357782c06a3b48bb3f67` Vercel production'da `READY`; `dev.adstable.app` alias'ı aynı deployment'a bağlı ve deploy sonrası runtime error taraması temizdir. Meta/Google için 1–3, Klaviyo için tek hesap veritabanı sınırında korunur. Canlı provider teması, OAuth veya connection yazısı yapılmadı; sıradaki kapı merchant acceptance'tır. Redacted database kanıtı `docs/security/evidence/R7A_ACCOUNT_CARDINALITY_LIVE.json` içindedir.
- **R7-A merchant acceptance — PASS / R6-D next:** İlk canlı Klaviyo OAuth callback'i provider token değişiminden sonra `CANONICAL_CONNECTION_WRITE_FAILED` ile durdu. Supabase PostgreSQL logu kesin kök nedeni `workspace_provider_timestamp_order` ihlali olarak gösterdi: application `updated_at` değeri, ağ gecikmesi sonrasında database tarafından üretilen `created_at` değerinden eski kaldı. İlk insert artık `updated_at` göndermez; database iki varsayılan timestamp'i aynı transaction anında üretir. Focused regression 34/34 PASS; production rollback-only insert `timestamp_constraint_pass=true`, `database_defaults_same_timestamp=true`; zorunlu rollback sonrası canonical/Klaviyo/pending satırları `0/0/0`. PR #244 merge commit `3206563365219cfe1b9c8483a52294752f186fc7` production'da READY olduktan sonra merchant akışı canlıda PASS verdi: Connect→OAuth→tek verified account→source-currency plan cost→Save→Connected→warning-modal Disconnect. Disconnect endpoint'i `200`; canonical Klaviyo satırı `disconnected`, access/refresh token, selected account ve plan cost temiz, `disconnected_at` doludur. R7-A kabulü tamamlandı; sıradaki kapı R6-D, R7-B/R3-C kapalıdır.
- **R6-D iş aynası — In progress / R6-D1 Done; R6-D2 Klaviyo live PASS; R6-D3 Meta full lifecycle live PASS; R6-D4-A0 Google Sheets park production PASS; R6-D4 Google Ads next:** R6-D tek bir belirsiz production açılışı değildir. Sabit sıra: **R6-D1** ortak fail-closed kabul koşucusu; **R6-D2** Klaviyo workspace tam yaşam döngüsü; **R6-D3** Meta workspace tam yaşam döngüsü; **R6-D4-A0** eski Google Sheets Dataset V1 export utility'sinin güvenli parkı; **R6-D4** Google Ads workspace tam yaşam döngüsü; **R6-D5** provider bazlı kontrollü aktivasyon kararı. Meta Connect → OAuth → verified account selection → data acceptance → Disconnect → temiz Reconnect zinciri canlıda tamamlanmıştır. Google Sheets parkı production'da doğrulanmıştır. TikTok/Pinterest parked kalır. R6-D bütün provider kapıları tamamlanmadan `Done` olmaz; R7-B yalnız son ortak tutarlılık kapısıdır.
- **R6-D1 — Done / R6-D2 in progress:** Ortak runtime seçilmiş hesap kapsamı, provider/platform, source/target currency, timezone/business date, sentetik satır yasağı ve doğrulanmış empty/non-empty sonucunu Dataset V2 yazısından önce fail-closed doğrular; dış sonuç token/account ID/metrik taşımaz ve `production_activation=false` kalır. Klaviyo workspace mapper/runner ile `2026-07-15` API revision kullanan Accounts + Campaign/Flow Reporting client hazırdır. `flows:read` OAuth scope'a eklenmiştir; explicit conversion metric ID zorunludur. Klaviyo `text_message_spend` yalnız USD hesapta gerçek spend olarak kabul edilir; başka account currency altında yanlış etiketlenmez. Email monthly plan cost yalnız canonical connection kaydından kullanım payıyla uygulanır. Tamamlanmış adapter, mapping, formula ve Dataset V2 writer yeniden geliştirilmez; R6-D2 yalnız canlı kabul kapılarıyla ilerler.
- **R6-D2 production preflight attempt — FAIL-CLOSED / PASS verilmedi:** Canonical Klaviyo reconnect merchant tarafından doğrulandı. PR #250 merge commit `3b8ce7f06e6cfcdf254d459f7e16ed54542c95e4` production'da Shopify-session-bound salt-okunur kabul rotasını açtı. İlk canlı çalıştırma `KLAVIYO_PREFLIGHT_NOT_CONFIGURED` döndürdü; route/session authority çalıştı fakat global `KLAVIYO_PLACED_ORDER_METRIC_ID` eksik olduğu için provider çağrısı başlamadı. Dataset V2 ve connection metadata yazısı yapılmadı.
- **R6-D2-C1 workspace/account metric binding corrective — migration + application deployment PASS / C2 next:** Tek global Klaviyo metric ID çok-workspace ve gelecekteki WooCommerce adapter'ı için reddedildi. Metrics API bütün sayfaları provider origin sınırında tarar; yalnız tam `Placed Order` adayları integration name/category provenance ile kabul yüzeyine gelir. Sıfır veya birden fazla aday otomatik seçilmez. Seçim provider'da tekrar doğrulanarak aynı workspace, canonical Klaviyo account ve optimistic connection version ile `workspace_provider_connections` kaydına bağlanır; disconnect/reconnect/account değişimi bağı temizler. Additive migration production'da PASS olmuş, PR #251 merge commit `b482ed7bd6506225ec9b95ca0cd9c5eef243ee50` production deployment'ında `READY` olmuş ve `dev.adstable.app` alias'ı doğrulanmıştır; deploy sonrası runtime error taraması temizdir. Provider discovery, canonical metric yazısı ve Dataset V2 yazısı yapılmamıştır. Dataset V2 write bundan sonra da ayrı onaydır.
- **R6-D2-C1 production migration preflight — PASS / mutation yapılmadı:** 2026-09-25 salt-okunur sorgusu canonical tablo, RLS/FORCE RLS ve browser grant sınırını doğruladı; hedef metric kolon/constraint sayısı `0/0`, canonical connection `1`, connected Klaviyo `1`, sonuç `PASS`. Provider çağrısı, database mutation veya Dataset V2 yazısı yapılmadı. Redacted kanıt `docs/security/evidence/R6D2C1_KLAVIYO_METRIC_BINDING_PREFLIGHT_LIVE.json`. Bu PASS yalnız migration önkoşuludur; migration uygulaması ayrı açık onay bekler.
- **R6-D2-C1 production migration — PASS / application deployment PASS:** Açık onayla additive migration production ledger'a `20260925072801_add_workspace_provider_conversion_metric` olarak uygulandı. Postcheck: kolon `5`, validated constraint `1`, RLS/FORCE RLS açık, browser grant `0`, bound metric `0`, invalid binding `0`, sonuç `PASS`. Mevcut connection satırına metric değeri yazılmadı; provider teması ve Dataset V2 yazısı yapılmadı. Security/performance advisors çalıştı; C1'e bağlı yeni bulgu yok, mevcut proje-geneli uyarılar bu taskta değiştirilmedi. Redacted kanıt `docs/security/evidence/R6D2C1_KLAVIYO_METRIC_BINDING_MIGRATION_LIVE.json`. Sonraki kapı R6-D2-C2 ürün sözleşmesi ve ardından ayrı onaylı salt-okunur discovery'dir.
- **R6-D2-C1 application deployment — PASS / C2 next:** PR #251 merge commit `b482ed7bd6506225ec9b95ca0cd9c5eef243ee50` GitHub `main` ile eşleşir. Vercel deployment `READY`, target `production`, `dev.adstable.app` alias'ı bağlı ve `aliasError=null`dır; son bir saatlik runtime error taramasında hata bulunmamıştır. Bu PASS yalnız C1 uygulama deployment'ını kapatır; provider discovery, canonical metric yazısı, Klaviyo rapor okuması veya Dataset V2 yazısı değildir.
- **R6-D2-C2 Klaviyo satış kaynağı ürün sözleşmesi — PASS / C3 next:** Kullanıcı teknik metric ID seçmez. Tek doğrulanmış `Placed Order` adayı varsa ayrı seçim ekranı açılmaz; satış kaynağı özeti Email Monthly Plan Cost adımındaki final onayda gösterilir. Birden fazla doğrulanmış aday varsa yalnız integration/store provenance taşıyan Shopify-native tek seçim kontrolü açılır; sıfır adayda bağlantı fail-closed kalır. Seçim yalnız ilk bağlantı, reconnect/account değişimi, bağın provider'da geçersizleşmesi veya Settings'teki açık değişiklikte yeniden istenir. Plan cost değişikliği tek başına satış kaynağı seçimini tekrar açmaz. C2 yalnız analist/UI sözleşmesidir; provider çağrısı, database write, Dataset V2 write veya yeni adapter/mapping geliştirmesi yapılmadı.
- **R6-D2-C3 salt-okunur satış kaynağı keşfi — Live PASS:** Mevcut canonical Klaviyo grant'i ile exact `Placed Order` adayları provider Metrics API'den okundu; tek aday kullanıcıya gösterildi. Supabase connection metadata ve Dataset V2 değiştirilmedi; teknik metric ID UI'a taşınmadı.
- **R6-D2-C4 canonical satış kaynağı bağı — Live PASS:** Kullanıcının onayladığı provider-doğrulanmış `Placed Order` adayı mevcut workspace + Klaviyo account kapsamlı connection satırına optimistic version sınırıyla yazıldı. Dataset V2 yazısı yapılmadı; reconnect/account değişimi korumaları korunuyor.
- **R6-D2-C5 salt-okunur Klaviyo runtime preflight — Live PASS:** Account, Campaign, Flow, Time ve FX doğrulandı. Campaign endpoint'inin ilk HTTP `429` yanıtı provider `Retry-After` süresi sonrasında kontrollü retry ile toparlandı; outer endpoint HTTP `200` verdi. Canonical connection `1`, connected `1`, metric binding `1`, Klaviyo Dataset V2 satırı `0` kaldı; C5 provider/DB write üretmedi.
- **R6-D2-C6 kontrollü Dataset V2 canlı kabulü — C6-A+C6-C failed closed / C6-B diagnostics PASS / Dataset V2 rows `0`:** C6-A ilk açık onaylı production isteği genel HTTP `503` ile durdu; C6-B güvenli aşama teşhisi production'a alındı. R3-C1 tenant enforcement PASS sonrasında ayrı açık onayla yapılan C6-C ikinci deneme `KLAVIYO_DATASET_ACCEPTANCE_FAILED_PROVIDER_ACCOUNT` döndürdü. Shopify authority, canonical connection, reporting currency ve Dataset zero-row guard başarılıydı; hata Account API aşamasında, campaign/flow, FX, normalization ve persistence öncesinde oluştu. Supabase upsert oluşmadı; bağlantı ve mevcut metric binding korundu. Dataset V2 satırı `0` kaldı ve otomatik retry yapılmadı.
- **R6-D2-C6-D connected Klaviyo token lifecycle corrective — Live PASS / verified empty / rows `0`:** PR #259 merge commit `9039180442768a2dab9b80c09d24217d8249588f` post-merge Security/Full Regression PASS ve otomatik Vercel Production deployment SUCCESS ile production'a alındı. Açık production onayıyla salt-okunur preflight tek kez çalıştı ve Account, Campaign, Flow, Time ve FX kapılarında PASS verdi. Ardından tek kontrollü C6 isteği `PASS_R6_D2_C6_KLAVIYO_DATASET_WRITE`, `attempted 0`, `persisted 0`, `empty_provider_result true` döndürdü. Provider gerçek boş sonuç verdiği için sentetik satır oluşturulmadı. Supabase salt-okunur postcheck Dataset V2 toplam/Klaviyo satırını `0/0`; connected Klaviyo, access expiry, encrypted refresh ve metric binding sayılarını `1/1/1/1` doğruladı. Beklenmeyen provider `401` yalnız bir refresh + bir retry üretir; ikinci auth hatası yeniden yetkilendirme ister. Bu sonuç token yaşam döngüsünün merchant'ı yeniden OAuth'a zorlamadan çalıştığını ve controlled acceptance'ın fail-safe boş sonuç sözleşmesini karşıladığını kanıtlar. Metric seçimi, mapping ve Dataset writer değiştirilmedi; tamamlanmış E4/E5/E7 yeniden geliştirilmedi. Redacted kanıt `docs/security/evidence/R6D2C6D_KLAVIYO_LIVE_ACCEPTANCE_2026-09-25.json` içindedir.
- **R6-D2-C7 postcheck ve Klaviyo PASS kararı — PASS / R6-D2 Done / R6-D3 approval gate:** Workspace/account kapsamı, provider provenance, source/reporting currency, Time/FX, sentetik veri yasağı ve provider-doğrulanmış boş sonuç canlı preflight ile doğrulandı. Supabase postcheck canonical bağlantı ve token metadata'sını korurken Dataset V2'nin `0` kaldığını doğruladı. Klaviyo canlı kabulü tamamlandı; sıradaki ayrı paket Meta için 1–3 verified hesap brief'i ve production onayıdır. Klaviyo PASS sonucu Meta/Google aktivasyonu değildir.
- **R6-D3-A Meta bağlantı ve token yaşam döngüsü — Done / contract only / R6-D3-B implementation gate:** Canlı salt-okunur envanterdeki legacy Meta kaydı durum olarak connected olsa da access expiry geçmiş ve refresh token yoktur; kayıt/history silinmez fakat canonical workspace'e taşınmaz, token yeniden kullanılmaz ve schedule pasif kalır. Yeni bağlantı temiz Shopify embedded OAuth ile başlar. Authorization code sonrası supported long-lived access-token modeli server-side uygulanır; token validity, configured Meta app, required scope ve gelecekteki expiry doğrulanmadan canonical save yapılmaz. Meta için uydurma refresh-token akışı kurulmaz; desteklenmeyen yenileme veya geçersiz token kontrollü `Reconnect Meta` ister. Callback `pending_account_selection` durumunda durur; server'ın provider'dan yeniden doğruladığı en az 1, en fazla 3 reklam hesabı seçilmeden `Connected` olmaz. Bu paket provider teması, production OAuth, Dataset V2 yazısı, schedule/backfill, Disconnect/revoke veya E4 yeniden geliştirmesi içermez. Sözleşme `contracts/r6d3a-meta-connection-lifecycle-v1.json`, analist kararı `docs/R6D3A_META_CONNECTION_LIFECYCLE_DECISION.md` içindedir.
- **R6-D3-B Meta token doğrulama uygulaması — Done / repository only / R6-D3-C account-selection acceptance gate:** Embedded Meta callback artık authorization code'u server-side kısa token'a, ardından supported long-lived user token'a çevirir. Long-lived token debugger ile `is_valid`, configured app ID, `ads_read` scope ve gelecekteki expiry doğrulanmadan canonical store çağrılmaz. Yalnız doğrulanmış long-lived access token encrypted envelope'a gider; Meta refresh token beklenmez veya üretilmez. Güvenli validation hataları Meta'ya bağlı `Reconnect Meta` sunumuna döner. Graph version authorize, exchange ve account discovery hattında aynı environment kaynağını kullanır. Başarılı callback yine yalnız `pending_account_selection` olur. Production deployment/provider teması, Dataset V2, schedule/backfill, Disconnect/revoke ve E4 değişikliği yapılmadı. Sözleşme `contracts/r6d3b-meta-token-validation-v1.json`, analist kaydı `docs/R6D3B_META_TOKEN_VALIDATION.md` içindedir.
- **R6-D3-C Meta 1–3 hesap seçimi — Production merchant acceptance PASS / 1 verified account / data runtime gate:** R7-A2'de kurulmuş canonical seçim hattı eski `server.js + dashboard.html` referansıyla yeniden karşılaştırılmış ve yeni runtime yazılmamıştır. Shopify session-bound liste/seçim rotaları Meta `/me/adaccounts` listesini server-side yeniden doğrular; browser yalnız ID gönderir. Sıfır, üçten fazla, duplicate veya listede olmayan seçim reddedilir. PR #261, merge commit `d7dfaa4baf8ba89af65873d1f00718ae3de5258d` ile production'a alınmış; security ve Vercel kontrolleri geçmiş, immutable deployment ile `dev.adstable.app` içerik eşleşmesi doğrulanmıştır. Salt-okunur preflight `PASS` sonrasında gerçek merchant Connect→OAuth→modal→1 hesap→Save→reload zinciri tamamlanmış ve kullanıcı `Connected · 1 account` görünümünü doğrulamıştır. Salt-okunur postcheck canonical Meta `1`, connected `1`, pending/invalid `0`, browser grant `0`; legacy Meta `1 → 1`; Dataset V2 Meta/schedule/job `0` sonucuyla `PASS` vermiştir. Bu PASS yalnız bağlantı ve hesap seçimidir: Meta Insights/performance, Time/FX, Dataset V2, schedule/backfill, Disconnect/revoke veya tamamlanmış E4/E5/E7 yeniden geliştirmesi yapılmamıştır. Sözleşme `contracts/r6d3c-meta-account-selection-acceptance-v1.json`, analist kaydı `docs/R6D3C_META_ACCOUNT_SELECTION_ACCEPTANCE.md`, redacted aggregate kanıt `docs/security/evidence/R6D3C_META_ACCOUNT_SELECTION_LIVE_ACCEPTANCE_2026-09-25.json` içindedir. Sıradaki kapı kodlama değil, R6-D3 Meta data runtime analist brief'idir.
- **R6-D3-D Meta workspace salt-okunur preflight — Production read-only acceptance PASS / verified empty / Dataset V2 gate:** Tamamlanmış E4 Meta Client, Campaign→AdSet→Ad mapper, sabit metrik sözleşmesi ve Time/FX katmanı yeniden geliştirilmeden workspace authority'ye bağlandı. PR #263 merge commit `e558ec69d25b6f5aeee314d592cd5cac231662a8` production'a dağıtıldı. Gerçek merchant kabulü `1` hesabın Account API ile doğrulandığını, önceki kapanmış gün Insights isteğinin başarıyla çalıştığını, sonucun provider-doğrulanmış boş (`0` satır) ve Time/FX kontrollerinin başarılı olduğunu gösterdi; Dataset V2 yazısı `0` kaldı. Supabase postcheck canonical Meta `1`, connected/selected account `1`, pending/invalid `0`; Dataset V2 Meta/schedule/job, browser grant `0`; legacy Meta `1` sonucuyla PASS verdi. Bu PASS kalıcı Dataset V2 yazısı, schedule, backfill veya aktivasyon değildir. Sözleşme `contracts/r6d3d-meta-read-only-preflight-v1.json`, analist kaydı `docs/R6D3D_META_READ_ONLY_PREFLIGHT.md`, kanıt `docs/security/evidence/R6D3D_META_READ_ONLY_LIVE_ACCEPTANCE_2026-09-25.json` içindedir. Sıradaki kapı ayrı analist brief'i ve açık onay gerektiren kontrollü Dataset V2 kabul kararıdır.
- **R6-D3-E Meta kontrollü Dataset V2 kabulü — Production verified-empty PASS / data gate closed / lifecycle gate open:** Onaylanan analist brief'i doğrultusunda yeni metrik, mapper veya legacy runtime geliştirilmemiştir. Mevcut Meta workspace runner, ortak provider-result doğrulaması, workspace canonical write boundary ve Workspace Supabase repository tek exact-confirmation işleminde birleştirilmiştir. Shopify session dışındaki workspace/tarih/currency alanları authority değildir. Seçilmiş 1–3 hesap için yakın business-date penceresinde mevcut Meta satırı varsa provider temasından önce fail-closed durur. PR #265 merge commit `ef0d2cd240b87b626b141111ed5712404fff816d` production'a dağıtıldıktan sonra açık onaylı tek kontrollü kabul `attempted 0`, `persisted 0`, `verified empty true`, `synthetic 0` sonucu verdi. Supabase salt-okunur postcheck canonical Meta `connected · 1 account` durumunu korurken Dataset V2 toplam/Meta, aktif Meta schedule, açık Meta job ve browser grant sayılarını `0` doğruladı; legacy Meta kaydı `1` kaldı. Bu sonuç provider-doğrulanmış güvenli boş-yol kabulüdür; non-empty fiziksel UPSERT henüz gözlenmemiştir. Schedule, backfill, V1 write ve aktivasyon açılmamıştır. Sözleşme `contracts/r6d3e-meta-controlled-dataset-acceptance-v1.json`, analist kaydı `docs/R6D3E_META_CONTROLLED_DATASET_ACCEPTANCE.md`, redacted kanıt `docs/security/evidence/R6D3E_META_CONTROLLED_DATASET_LIVE_ACCEPTANCE_2026-09-25.json` içindedir. R6-D3-E veri kapısı kapanmıştır; tam Meta yaşam döngüsü R6-D3-F olmadan kapanmaz.
- **R6-D3-F Meta bağımsız Disconnect yaşam döngüsü — Production Disconnect and clean Reconnect PASS / R6-D3 complete:** Eski `server.js + dashboard.html` referansındaki revoke-first davranış embedded yapıya taşındı. PR #268 merge commit `2f7014a6ed89eccc85e2c38d76ef518734493597` production'da READY oldu ve `dev.adstable.app` alias'ı doğrulandı. Merchant `Connected · 1 account` durumunda modal Cancel'ın non-destructive olduğunu, açık Disconnect sonrası `Not connected` durumunu ve temiz OAuth/tek verified hesap ile yeniden `Connected · 1 account` durumunu doğruladı. Salt-okunur Supabase postcheck Meta token/account cleanup ve reconnect'i PASS verirken Klaviyo connected, reporting currency `TRY`, legacy Meta geçmişi `1`, Dataset V2 toplam/Meta `0`, aktif Meta schedule/job `0` ve browser grant `0` kaldı. Migration yapılmadı. Sözleşme `contracts/r6d3f-meta-independent-disconnect-v1.json`, analist kaydı `docs/R6D3F_META_INDEPENDENT_DISCONNECT.md`, redacted kanıt `docs/security/evidence/R6D3F_META_DISCONNECT_RECONNECT_LIVE_ACCEPTANCE_2026-09-26.json` içindedir. R6-D3 tamamlandı; sıradaki kapı R6-D4 Google Ads analist brief'idir.
- **R6-D4-A0 Google Sheets park kararı — Production PASS / R6-D4 Google Ads gate:** Google Sheets mevcut yapıda provider data source değil, eski `performance_dataset_rows` Dataset V1 içeriğini spreadsheet'e taşıyan standalone export utility'sidir. PR #270 merge commit `1a9fa1d69d8a9552108e693cd4366d8852108a2a` Security/Full Regression PASS sonrasında Vercel production'da `READY` olmuş ve `dev.adstable.app` alias'ı aynı deployment'a bağlanmıştır; bir saatlik runtime error taraması temizdir. Production'da yeni OAuth ve callback exchange, token refresh, manuel/otomatik sync, Dataset V1/V2 export ve disconnect mutation fail-closed kapalıdır; status yüzeyi `Parked` olur. Salt-okunur postcheck tarihsel connected kayıt, encrypted access/refresh credential ve spreadsheet bağının korunduğunu; plaintext token ile Dataset V2 toplam/Google Sheets satırının `0/0/0` kaldığını doğrulamıştır. Google grant revoke, Supabase mutation, provider çağrısı veya spreadsheet değişikliği yapılmamıştır. Contract `contracts/r6d4a0-google-sheets-park-v1.json`, analist kaydı `docs/R6D4A0_GOOGLE_SHEETS_PARK.md`, redacted kanıt `docs/security/evidence/R6D4A0_GOOGLE_SHEETS_PARK_LIVE_2026-09-26.json` içindedir. R6-D4 Google Ads ayrı analist brief'i ve onayıyla başlayabilir.
- **R6-D4-A Google Ads bağlantı ve token yaşam döngüsü — Done / contract only / R6-D4-B implementation gate:** Execution Plan, eski `server.js + dashboard.html` referansı ve embedded canonical yapı birlikte karşılaştırıldı. Mevcut embedded akış top-level offline OAuth, `pending_account_selection`, `ListAccessibleCustomers + customer_client` keşfi ve provider tarafından yeniden doğrulanmış 1–3 hesap seçimini sağlar; ancak hesap başına manager `login_customer_id` canonical kayıtta kaybolur, canonical refresh-token yenilemesi ve Google Ads'e özel Disconnect yoktur. R6-D4-B; geçerli refresh token + access expiry olmadan canonical save yapmayacak, server-side refresh/rotation ve tek kontrollü auth retry uygulayacak, her seçilmiş non-manager hesaba doğrulanmış `login_customer_id` yazacak ve invalid grant'te `Reconnect Google Ads` isteyecektir. Disconnect provider global revoke yapmadan yalnız Google Ads canonical credential/hesap bağını temizleyecek, tarihsel analytics'i koruyacaktır; bunun nedeni aynı Google OAuth client'ın geçmişte Ads, parked Sheets ve parked GA4 tarafından kullanılmış olmasıdır. E5 Standard/PMax, mapping, Time/FX ve Dataset V2 writer yeniden geliştirilmez; Google Sheets ve GA4 parked kalır. Bu paket provider teması, production OAuth, Supabase mutation veya Dataset V2 yazısı yapmadı. Contract `contracts/r6d4a-google-ads-connection-lifecycle-v1.json`, analist kararı `docs/R6D4A_GOOGLE_ADS_CONNECTION_LIFECYCLE_DECISION.md` içindedir.
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

- **E1-T1 — `Done` — OAuth route envanteri ve threat mod…54129 tokens truncated…_t6_rls_v3` namespace'i; checksum-bound 21-gate preflight; 16-case rollback-only transaction; 19-gate postcheck; tek kullanımlık `0600` state/outcome sidecar; exact confirmation ve fail-closed terminal outcome sözleşmesi.

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

