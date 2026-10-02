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
| R6 | Embedded provider runtime → canonical V2 | E4 + E5 + E7 | `In progress / R6-D2 Klaviyo live PASS; R6-D3 Meta full lifecycle live PASS; R6-D4-E Google Ads verified-empty live PASS; R6-D4-F repository PASS / production acceptance pending` — Google Sheets ve GA4 parked kalır. Google Ads bağlantı, üç hesap, read-only ve kontrollü verified-empty veri kapıları canlıda geçmiştir. Son yaşam döngüsü kapısı bağımsız local Disconnect, Cancel ve temiz Reconnect kabulüdür. |
| R7 | Currency-first ve Shopify-native Connect/Disconnect UX | E10-T5 + E10-T6-C2I-V10 | `R7-A merchant acceptance PASS; provider-specific Disconnect acceptance moved into each R6-D provider gate; R7-B final consistency after R6-D` — Klaviyo için tam yaşam döngüsü canlıda doğrulandı. Meta/Google için Connect → Connected → Disconnect → temiz Reconnect zinciri ilgili provider'ın R6-D kabulü kapanmadan tamamlanır. R7-B bütün aktif provider'ların son ortak deneyim ve tutarlılık kapısıdır; eksik Disconnect uygulamalarının ilk teslim noktası değildir. |
| R8 | V1 tarihsel geçiş ve resumable backfill | E9 | `B6C_DATASET_ACTIVATION_LIVE.json` içindedir. Sıradaki iş R7-A reporting currency ve canonical Connect foundation'dır.
- **R7:** Connect kartı önce açıklama modalı açar; modal dışı close/Cancel provider teması yapmaz. OAuth top-level'dır. Account selection ve Klaviyo plan cost/currency Shopify-native modal/form ile tamamlanır. Connected kartında Disconnect bulunur; Cancel non-destructive, tarihsel analytics korunur. Resmi Shopify componentleri, session token ve mobil acceptance zorunludur.
- **R7-A1 — Repository complete / R7-A2 next:** Shopify session ile çözülen workspace için reporting currency ilk kez merchant tarafından seçilir; Shopify currency okunmaz/default yapılmaz. Currency kaydı olmadan Data Sources ve OAuth start fail-closed durur. Meta/Google Ads/Klaviyo aktif, TikTok/Pinterest Parked'tır. Connect kartı provider teması yapmadan açıklama modalı açar; yalnız modal içindeki açık eylem top-level OAuth başlatır. Callback artık eski Shopify connection tablosuna değil `workspace_provider_connections` tablosuna `pending_account_selection` yazar. Klaviyo seçim ve maliyet kaydı canonical store'a yönlendirilmiştir. Production deployment/provider teması yapılmadı. Meta ve Google Ads verified account discovery/seçimi R7-A2'de tamamlanacaktır; R6-D ve R7-B kapıları kapalıdır.
- **R7-A2 — Corrected account-cardinality contract / production acceptance gate:** Meta `me/adaccounts` ve Google Ads `ListAccessibleCustomers + customer_client` üzerinden provider-supplied hesap kimliği/adı/source currency kaydetme anında yeniden doğrulanır; kullanıcı aynı OAuth grant'i altında en az 1, en fazla 3 reklam hesabı seçer. Klaviyo Accounts API üzerinden tek doğrulanmış hesap seçilir ve plan cost bu hesabın verified source currency'siyle ayrı adımda kaydedilir. Browser workspace, account adı veya currency authority değildir. `active_account_id` geriye uyumluluk alanıdır; tam bağlantı kapsamı `selected_accounts`, Funnel için tek aktif hesap tercihi ise ayrı Settings kararıdır. TikTok/Pinterest parked kalır. Additive cardinality migration'ı, production deployment, merchant reporting currency seçimi, canlı provider teması ve canonical connection ayrı kabul kapısındadır; ardından R6-D gelir.
- **R7-A production corrective — OAuth route/UI readiness:** İlk merchant kabulünde Klaviyo status ve OAuth start rotalarının `404` verdiği production runtime loguyla doğrulandı. Kök neden, R7-A2'de Google Ads account discovery için eklenen developer-token şartının yanlışlıkla bütün provider route composition'ını kapatması; presentation function'ın ise yalnız genel feature flag'e bakarak Connect göstermesiydi. Provider readiness artık provider bazında izole edilir: eksik Google Ads hazırlığı Klaviyo veya Meta rotalarını kapatamaz; bilinen fakat hazır olmayan provider `503 SHOPIFY_PROVIDER_NOT_CONFIGURED` döndürür. İlk currency seçimi ve kısa Connect açıklaması `small-100` Shopify-native modallara taşınır; generic hata yerine güvenli hata kodu gösterilir. Corrective deployment ve merchant retest tamamlanmadan R7-A production acceptance verilmez; R6-D/R7-B kapıları kapalı kalır.
- **R7-A account-cardinality activation — migration ve application deployment PASS / merchant acceptance next:** Açık production onayıyla salt-okunur database preflight `PASS` verdi: canonical connection tablosu `0` satır, connected/invalid kayıt `0`, RLS ve FORCE RLS açık. Yalnız additive `20260924120453_add_workspace_provider_selected_accounts` migration'ı uygulandı. Postcheck `PASS`: kolon `NOT NULL DEFAULT []`, üç cardinality constraint validated, browser grant `0`, canonical satır `0`. PR #242 merge commit `a94201bf74c76cc57b5a357782c06a3b48bb3f67` Vercel production'da `READY`; `dev.adstable.app` alias'ı aynı deployment'a bağlı ve deploy sonrası runtime error taraması temizdir. Meta/Google için 1–3, Klaviyo için tek hesap veritabanı sınırında korunur. Canlı provider teması, OAuth veya connection yazısı yapılmadı; sıradaki kapı merchant acceptance'tır. Redacted database kanıtı `docs/security/evidence/R7A_ACCOUNT_CARDINALITY_LIVE.json` içindedir.
- **R7-A merchant acceptance — PASS / R6-D next:** İlk canlı Klaviyo OAuth callback'i provider token değişiminden sonra `CANONICAL_CONNECTION_WRITE_FAILED` ile durdu. Supabase PostgreSQL logu kesin kök nedeni `workspace_provider_timestamp_order` ihlali olarak gösterdi: application `updated_at` değeri, ağ gecikmesi sonrasında database tarafından üretilen `created_at` değerinden eski kaldı. İlk insert artık `updated_at` göndermez; database iki varsayılan timestamp'i aynı transaction anında üretir. Focused regression 34/34 PASS; production rollback-only insert `timestamp_constraint_pass=true`, `database_defaults_same_timestamp=true`; zorunlu rollback sonrası canonical/Klaviyo/pending satırları `0/0/0`. PR #244 merge commit `3206563365219cfe1b9c8483a52294752f186fc7` production'da READY olduktan sonra merchant akışı canlıda PASS verdi: Connect→OAuth→tek verified account→source-currency plan cost→Save→Connected→warning-modal Disconnect. Disconnect endpoint'i `200`; canonical Klaviyo satırı `disconnected`, access/refresh token, selected account ve plan cost temiz, `disconnected_at` doludur. R7-A kabulü tamamlandı; sıradaki kapı R6-D, R7-B/R3-C kapalıdır.
- **R6-D iş aynası — In progress / R6-D1 Done; R6-D2 Klaviyo live PASS; R6-D3 Meta full lifecycle live PASS; R6-D4-E Google Ads verified-empty live PASS; R6-D4-F repository PASS / production acceptance pending:** R6-D tek bir belirsiz production açılışı değildir. Sabit sıra: **R6-D1** ortak fail-closed kabul koşucusu; **R6-D2** Klaviyo workspace tam yaşam döngüsü; **R6-D3** Meta workspace tam yaşam döngüsü; **R6-D4-A0** eski Google Sheets Dataset V1 export utiazmadan `next(error)` ile canonical boundary'ye gider; invalid dependency fail-closed; full/security CI PASS.

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
