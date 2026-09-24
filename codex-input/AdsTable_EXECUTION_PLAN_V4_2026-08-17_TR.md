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
- **R3 — In progress / R3-A Done; R3-B runtime verification:** 23 Eylül 2026 canlı preflight Dataset V2'nin `0` satır içerdiğini, canonical workspace'in bulunduğunu, `workspace_id` kolonunun ve `backfill_checkpoints` tablosunun bulunmadığını doğruladı. Açık production onayıyla additive `2026092308373   assert.match(html, new RegExp(`data-provider="${provider}"`));
  }
  assert.doesNotMatch(html, /data-provider="tiktok"/);
  assert.doesNotMatch(html, /data-provider="pinterest"/);
  assert.match(html, /TikTok connection is parked for a later release/);
  assert.match(html, /Pinterest connection is not available in this release/);
  assert.match(html, /<s-paragraph>Parked<\/s-paragraph>/);
  assert.doesNotMatch(html, /Connect (?:Meta|Google Ads|Klaviyo|TikTok|Pinterest) to this Shopify workspace/);
  assert.match(html, /fetch\("\/api\/shopify\/providers\/klaviyo\/accounts" \+ path/);
  assert.match(html, /request\("\/status"\)/);
  assert.match(html, /window\.shopify\.idToken/);
  assert.match(html, /<div id="currency-setup" hidden>[\s\S]*heading="Finish setup"/);
  assert.match(html, /currencySetup\.hidden = true;[\s\S]*providerSections\.hidden = true;[\s\S]*Open AdsTable from Shopify Admin/);
  assert.doesNotMatch(html, /id="reporting-currency-summary"/);
  assert.match(html, /id="platforms-currency-modal" heading="Choose reporting currency" size="small-100"/);
  assert.match(html, /\/api\/shopify\/workspace\/reporting-currency/);
  assert.match(html, /<s-modal id="klaviyo-connect-modal" heading="Connect Klaviyo to AdsTable\?" size="small-100">/);
  assert.match(html, /<s-modal id="klaviyo-account-modal" heading="Finish Klaviyo setup">/);
  assert.match(html, /commandFor="klaviyo-connect-modal" command="--show"/);
  assert.match(html, /\/api\/shopify\/providers\//);
  assert.match(html, /open\(body\.authorization_url, "_top"\)/);
  assert.match(html, /cdn\.shopify\.com\/shopifycloud\/polaris-1\.js/);
  assert.match(html, /<s-app-nav>/);
  assert.match(html, /<s-page heading="Data sources">/);
  assert.equal((html.match(/data-provider=/g) || []).length, 3);
  assert.doesNotMatch(html, /<style>|<iframe/i);
  assert.doesNotMatch(html, /workspace[_-]id|shop[_-]id|user[_-]id/i);
});

test("embedded Platforms keeps Connect actions disabled until runtime activation", () => {
  const html = renderEmbeddedPlatforms({clientId: "client-id", providerOAuthEnabled: false});
  assert.equal((html.match(/ disabled/g) || []).length, 5);
  assert.match(html, /Connection setup unavailable/);
  assert.doesNotMatch(html, /Checking connection status/);
});

test("provider token exchanges normalize object and nested TikTok token responses", () => {
  assert.deepEqual(normalize({access_token: "access", refresh_token: "refresh"}), {accessToken: "access", refreshToken: "refresh"});
  assert.deepEqual(normalize({data: {access_token: "access"}}), {accessToken: "access", refreshToken: null});
  assert.throws(() => normalize({data: {}}), /INVALID_PROVIDER_TOKEN_RESPONSE/);
});

test("provider token exchange clients keep credentials server-side and use exact endpoints", async () => {
  const calls = [];
  const exchanges = createEmbeddedProviderTokenExchanges({fetchImpl: async (url, options) => {
    calls.push({url, options});
    return {ok: true, json: async () => ({access_token: "access", refresh_token: "refresh"})};
  }});
  for (const exchange of Object.values(exchanges)) {
    await exchange({code: "code", redirectUri: "https://app.test/callback", pkceVerifier: "verifier", clientId: "client", clientSecret: "secret"});
  }
  assert.equal(calls.length, 5);
  assert.equal(calls.every(call => !call.url.includes("secret")), true);
  assert.deepEqual(calls.map(call => new URL(call.url).hostname), ["graph.facebook.com", "oauth2.googleapis.com", "a.klaviyo.com", "business-api.tiktok.com", "api.pinterest.com"]);
});

test("provider OAuth remains off unless the explicit activation flag is true", () => {
  assert.equal(enabled(undefined), false);
  assert.equal(enabled(""), false);
  assert.equal(enabled("true"), true);
});

test("Google Ads account-discovery configuration cannot disable Klaviyo OAuth routing", () => {
  const env = {
    PROVIDER_TOKEN_ACTIVE_KEY_ID: "v1",
    PROVIDER_TOKEN_ENCRYPTION_KEYS: JSON.stringify({v1: Buffer.alloc(32, 1).toString("base64")}),
  };
  assert.equal(providerRuntimeReady({env, oauthTransactionStore: {}}), true);
  assert.equal(env.GOOGLE_ADS_DEVELOPER_TOKEN, undefined);
});

test("incomplete provider activation stays isolated without crashing Shopify App Home", () => {
  const routes = {};
  const app = {
    get: (path, handler) => { routes[`GET ${path}`] = handler; },
    post: (path, handler) => { routes[`POST ${path}`] = handler; },
  };
  const result = registerShopifyRuntime({
    app,
    env: {
      SHOPIFY_API_KEY: "key",
      SHOPIFY_API_SECRET: "secret",
      SHOPIFY_APP_URL: "https://dev.adstable.app",
      SHOPIFY_DEV_STORE: "store.myshopify.com",
      SHOPIFY_EMBEDDED_PROVIDER_OAUTH_ENABLED: "true",
    },
    supabaseAdmin: {from: () => ({})},
  });
  assert.deepEqual(result, {enabled: true, providerOAuthEnabled: false, providerOAuthRequested: true, providerAvailability: {meta: false, google_ads: false, klaviyo: false}});
  assert.equal(typeof routes["GET /"], "function");
  assert.equal(typeof routes["GET /shopify/app/platforms"], "function");
  assert.equal(routes["POST /api/shopify/providers/meta/oauth/start"], undefined);
});
