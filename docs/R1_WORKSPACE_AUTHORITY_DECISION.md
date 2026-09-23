# R1 — Workspace authority kararı ve geçiş sırası

## İş çıktısı

AdsTable'ın canonical tenant kimliği `workspace_id` olarak donduruldu. Shopify bir tenant değildir; doğrulanmış bir commerce installation adapter'ı üzerinden workspace'e bağlanır. `user_id` ve `shopify_user_id` kullanıcı/actor kimliğidir; email, shop veya provider account benzerliği workspace sahipliği kuramaz.

Versionlı executable karar: `contracts/r1-workspace-authority-v1.json`.

R1 yalnız contract ve envanter paketidir. Runtime kodu, Supabase şeması, canlı veri, provider grant/token veya deployment değiştirilmemiştir. R2 ayrı açık onay almadan başlayamaz.

## Mevcut durum envanteri

### `user_id` tenant authority kullanan hat

| Alan | Mevcut authority | Kanıt | R paketi kararı |
|---|---|---|---|
| Canonical envelope | `identity.user_id` | `funnel-core/canonical-contract.js` | R3'te versionlı `identity.workspace_id` |
| Dataset V2 tablo, unique key, index ve RLS | `user_id` | `20260816101220_create_performance_dataset_rows_v2.sql` | R3 additive geçiş; fiziksel compatibility kaldırma R10 |
| In-memory ve Supabase repository | `user_id` | `funnel-core/dataset-repository.js`, `funnel-core/supabase-dataset-repository.js` | R3 workspace-scoped API |
| Funnel Query Service | `user_id` | `funnel-core/funnel-query-service.js` | R3 workspace-scoped query |
| Meta/Google/TikTok/Klaviyo mapper/writer | `context.userId` veya `userId` | `src/providers/**` | Meta/Google/Klaviyo R6; TikTok parked |
| Snapshot/refresh sınırı | `user_id` | `src/jobs/refresh-job-boundary.js` | R6 workspace job ownership |
| Backfill checkpoint tablo ve runtime | `user_id` | `20260908074500_create_backfill_checkpoints.sql`, `src/backfill/**` | R3 workspace checkpoint identity |
| Standalone OAuth transaction | `user_id` | `20260818090000_create_oauth_transactions.sql` | R4 yeni write/refresh freeze; R10 retirement |
| Standalone connection/token | `user_id + platform` | `platform_connections`, `platform_connection_tokens` | R4 legacy read-only; R5 Klaviyo consolidation; R10 retirement |

Eski SQL runbook ve evidence dosyaları tarihsel kanıttır. R2/R3 migration'ı olarak tekrar çalıştırılmaz; R10'a kadar legacy doğrulama ve rollback referansı olarak korunur.

### `workspace_id` kullanan Shopify embedded hat

| Alan | Mevcut authority | Kanıt | Karar |
|---|---|---|---|
| Shopify installation | `shop_id → workspace_id` | `20260910100000_create_shopify_managed_installations.sql` | Shopify adapter binding olarak korunur; workspace ayrı canonical tabloya bağlanır |
| Embedded OAuth transaction | `workspace_id + shop_id`; `shopify_user_id` actor | `20260911130000_add_embedded_oauth_authority.sql` | `workspace_id` tenant; shop doğrulanmış adapter context; actor audit/auth context |
| Embedded provider connection | `workspace_id + provider` | `20260911150000_create_shopify_workspace_provider_connections.sql` | R4'te Shopify isminden bağımsız canonical store'a taşınır |
| Session authority | verified shop binding → `workspace_id` | `src/shopify/tenant-model.js`, `src/shopify/embedded-auth.js` | Shopify authority adapter olarak korunur |

## Nihai authority sözleşmesi

1. Bütün analytics, provider connection, reporting currency, query, job ve backfill sahipliği `workspace_id` ile belirlenir.
2. Shopify `shop_id`, WooCommerce installation ID veya gelecekteki başka commerce kimliği yalnız installation adapter kimliğidir.
3. `user_id` ve provider tarafından doğrulanan kullanıcı ID'leri authentication, authorization girdisi ve audit amacıyla tutulabilir; tenant key olamaz.
4. Browser query/body/header içinden gelen `workspace_id`, `shop_id`, `user_id` veya account ID authority değildir. Workspace server-side doğrulanmış installation/session/binding üzerinden çözülür.
5. Workspace eşleştirmesi email, domain, shop adı veya provider account ID benzerliğiyle otomatik kurulamaz.
6. İlk release'te bir Shopify installation tek workspace'e bağlıdır. Gelecekte WooCommerce ayrı `woocommerce_installations → workspace_id` adapter'ıyla aynı çekirdeğe bağlanabilir.
7. Aktif provider kapsamı Meta, Google Ads ve Klaviyo'dur. TikTok ve Pinterest parked olduğundan R2–R7 production path'ine alınmaz.

## Şema hedefi ve sahiplik sınırları

R2'nin minimum platformdan bağımsız taban modeli:

- `workspaces`: canonical tenant yaşam döngüsü.
- `workspace_settings`: `workspace_id` başına kullanıcı seçilmiş reporting currency ve version bilgisi.
- `shopify_installations.workspace_id`: `workspaces.id` foreign key; Shopify tokenları installation adapter'ında kalır.
- Gelecekteki `woocommerce_installations.workspace_id`: aynı workspace çekirdeğine ayrı adapter; R11'e kadar implementation yok.

R4'ün minimum provider modeli:

- canonical store adı Shopify içermeyecek ve `workspace_id + provider` sahipliğinde olacaktır;
- encrypted access/refresh envelope tek authoritative kopya olacaktır;
- selected provider account server-side ownership doğrulamasından sonra aktifleşecektir;
- provider source currency workspace reporting currency'den ayrı tutulacaktır;
- Klaviyo monthly plan cost kendi source currency'si ile saklanacaktır;
- OAuth callback tek başına `Connected` durumu üretmeyecektir.

## Değiştirilemez migration ve release sırası

### R2 — Workspace ve reporting currency tabanı

1. Canlıya yazmadan önce salt okunur tablo/kolon/constraint/grant/RLS envanteri, row count ve collision raporu alınır.
2. `workspaces` ve `workspace_settings` additive migration ile oluşturulur. Public şemada RLS + force RLS uygulanır; `public`, `anon` ve genel `authenticated` mutation yetkileri kapalıdır.
3. Var olan `shopify_installations.workspace_id` değerleri canonical workspace satırı olarak seed edilir. Email, domain veya provider account ile otomatik birleştirme yapılmaz.
4. `shopify_installations.workspace_id → workspaces.id` foreign key doğrulanır. Mevcut one-shop/one-workspace uniqueness ilk release boyunca korunur.
5. Reporting currency yalnız kullanıcı seçimiyle `workspace_settings` içine yazılır. Ayar yoksa Data Sources ve provider OAuth başlamaz; Shopify currency okunmaz veya default edilmez.

### R3 — Dataset V2 ve çalışma sınırları

6. `performance_dataset_rows_v2` ve `backfill_checkpoints` tablolarına önce nullable `workspace_id` eklenir; legacy `user_id` hemen silinmez.
7. Canonical contract yeni major version ile `identity.workspace_id` kullanır. Repository, Query Service, provider context, job ve backfill interface'leri aynı release çizgisinde workspace-scoped yapılır.
8. Dataset V2 boşluğu canlı read-only kanıtla doğrulanır. Satır varsa yalnız açık `legacy user → workspace` binding ile backfill edilir; belirsiz satır bloklanır.
9. Yeni workspace unique/index yapısı oluşturulur ve doğrulanır. Workspace kapsamlı RLS/policy, server authority modeliyle birlikte kabul edilir; yalnız `TO authenticated` kullanımı authorization sayılmaz.
10. Yeni writer workspace contract'a geçtikten ve parity sağlandıktan sonra `workspace_id NOT NULL` doğrulanır. `user_id` compatibility alanının fiziksel kaldırılması R10'a kalır.

### R4–R5 — Provider authority birleşimi

11. Shopify isminden bağımsız `workspace_provider_connections` authority'si additive olarak oluşturulur; tokenlar yalnız encrypted envelope halinde ve service-role sınırında tutulur.
12. Standalone OAuth, connection ve refresh hatlarına yeni write kapatılır. Legacy kayıtlar read-only migration kaynağı olarak korunur.
13. Doğrulanmış embedded connection kayıtları token decrypt/re-encrypt gerektirmeden canonical store'a kopyalanır; her workspace/provider için tek authority doğrulanır.
14. Aynı Klaviyo account'un eski ve embedded kaydı explicit legacy-user→workspace binding ve server-side account doğrulamasıyla uzlaştırılır.
15. R5 kabulünden önce Klaviyo revoke yapılmaz. Kabul sonrasında eski local kayıt `migrated/disabled` olur; token deletion ve provider revoke R10 retention kararına kalır.

### R6–R7 — Runtime ve kullanıcı akışı

16. Meta, Google Ads ve Klaviyo runtime'ları canonical workspace connection → Time → FX → Dataset V2 hattına geçirilir. V2 hatasında sessiz V1 fallback yasaktır.
17. Currency-first embedded UX açılır: currency seçimi → Data Sources → açıklama modalı → OAuth → server-verified account seçimi → yalnız Klaviyo için monthly cost + source currency → Connected.
18. Disconnect uyarı modalı gerektirir. Cancel non-destructive'tir; onaylanan disconnect connection'ı kapatır fakat historical analytics silmez. Provider revoke ancak provider semantiği ve R5/R10 güvenlik kapıları izin verirse çalışır.

## R2 başlamadan zorunlu kapı

- İnsan tarafından açık R2 onayı.
- Repository migration ve rollback çiftinin incelenmesi.
- Canlı salt-okunur preflight ile Dataset V2, workspace, legacy connection ve duplicate Klaviyo durumunun doğrulanması.
- Migration sonrası schema/constraint/index/RLS/grant sorguları.
- Supabase security ve performance advisor sonuçları.
- Cross-workspace IDOR, browser-supplied authority, same-key UPSERT, currency ve rollback testleri.

## R1 sonucu

R1 kararı tamamlandı. Sonraki uygulanabilir paket R2'dir; fakat R2 şema değişikliği ve canlı preflight yetkisini R1 onayı otomatik olarak vermez.
