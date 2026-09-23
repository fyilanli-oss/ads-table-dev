# R2 — Workspace ve reporting currency foundation

## Durum

`Done — live migration applied; postcheck PASS`.

R1'de dondurulan `workspace_id` tenant kararı için additive Supabase migration, versionlı contract, read-only preflight/postcheck ve fail-closed rollback artefaktları hazırlandı. Canlı Supabase'de yalnız salt okunur preflight çalıştırıldı; migration uygulanmadı.

20 Eylül 2026 ilk salt okunur canlı preflight, canlı şema ile migration ledger arasında iki eksik geçmiş kaydı buldu. Kullanıcının ayrı açık onayıyla R2-A uzlaştırması tamamlandı ve preflight `PASS` verdi. İkinci ayrı açık onayla workspace/currency foundation migration'ı canlıya uygulandı; postcheck `PASS` verdi.

## İş çıktısı

- `public.workspaces` canonical tenant registry olarak tanımlandı.
- `public.workspace_settings` kullanıcı tarafından seçilen reporting currency'yi workspace başına saklar.
- Mevcut doğrulanmış `shopify_installations.workspace_id` değerleri workspace registry'ye seed edilir.
- Shopify installation, workspace'e foreign key ile bağlanır; Shopify tenant'ın kendisi değildir.
- Reporting currency için hiçbir Shopify store/presentment currency default'u yoktur.
- `workspace_settings` otomatik seed edilmez: currency kullanıcı seçmeden Data Sources açılmamalıdır.
- Provider source currency ayrı kalır. Klaviyo `USD`, workspace reporting currency `TRY` olabilir.

## Güvenlik sınırı

Her iki tablo da public şemada olduğundan RLS ve force RLS açıktır. `PUBLIC`, `anon` ve `authenticated` rollerinin bütün doğrudan yetkileri kaldırılır. Yalnız server-side `service_role` için explicit CRUD grant vardır. Browser'dan gelen workspace ID authority olamaz; Shopify embedded session doğrulaması backend'de workspace'i çözer.

## Artefaktlar

- Migration: `supabase/migrations/20260920090105_create_workspace_currency_foundation.sql`
- Contract: `contracts/r2-workspace-currency-v1.json`
- Preflight: `docs/security/sql/R2_WORKSPACE_CURRENCY_PREFLIGHT.sql`
- Postcheck: `docs/security/sql/R2_WORKSPACE_CURRENCY_POSTCHECK.sql`
- Rollback: `docs/security/sql/R2_WORKSPACE_CURRENCY_ROLLBACK.sql`
- Static regression: `tests/r2-workspace-currency-foundation.test.js`

## Canlı kabul kapısı

1. Ayrı açık production onayı alınır.
2. Preflight salt okunur çalıştırılır; `PASS`, row counts ve duplicate Klaviyo bağlamı incelenir.
3. Migration ve rollback aynı review kapsamında onaylanır.
4. Migration bir kez uygulanır.
5. Postcheck `PASS` vermelidir.
6. Supabase security ve performance advisor çalıştırılır; yeni bulgular sınıflandırılır.
7. Currency seçilmeden Data Sources/OAuth'ın açılmadığı R7'de runtime ve UI ile doğrulanır.

Bu kapılar tamamlanmadan R2 `Done` değildir ve R3 başlamaz.

## Canlı preflight sonucu — 20 Eylül 2026

Kimlik, token veya hesap adı alınmadan yalnız toplu sayım ve şema varlığı kontrol edildi:

- 1 Shopify installation ve 1 farklı workspace; null workspace yok.
- Dataset V2 satır sayısı 0.
- 1 embedded provider connection; bu kayıt Klaviyo.
- Legacy modelde 2 Klaviyo connection bulunuyor.
- `backfill_checkpoints` canlıda yok ve `20260908074500` migration kaydı da yok. Bu, R3 öncesinde henüz production'a aktive edilmemiş paket olarak sınıflandırıldı; R2 migration'ını tek başına bloklamaz.
- Embedded OAuth workspace kolonları ve workspace provider table canlıda var; fakat bunlara karşılık gelen `20260911130000` ve `20260911150000` migration kayıtları ledger'da yok.
- Klaviyo completion şeması canlıyla uyumlu. Repository migration dosyası canlı ledger sürümü `20260919194248` ile eşleştirildi.

İlk sonuç: `BLOCK_EMBEDDED_MIGRATION_LEDGER_DRIFT`.

## R2-A uzlaştırma sonucu — 20 Eylül 2026

- Kullanıcının açık onayı yalnız `20260911130000` ve `20260911150000` geçmiş kayıtları için alındı.
- İşlem öncesinde iki migration'ın temsil ettiği kolon, constraint, function, primary key, foreign key, RLS ve grant fingerprint'leri yeniden doğrulandı.
- Yalnız bu iki sürüm migration ledger'a eklendi; embedded tablolar/kolonlar yeniden oluşturulmadı.
- E9/backfill sürümü `20260908074500` kaydedilmedi.
- Shopify installation, OAuth transaction, embedded/legacy Klaviyo connection ve Dataset V2 satır adetleri değişmedi.
- Tekrar çalışan R2 preflight sonucu `PASS`; `workspaces` ve `workspace_settings` hâlâ mevcut değil.

R2-A tamamlandı ve ardından ayrı açık production onayıyla foundation migration uygulandı.

## Canlı foundation sonucu — 20 Eylül 2026

- Canlı migration sürümü: `20260920090105_create_workspace_currency_foundation`.
- `public.workspaces` ve `public.workspace_settings` oluşturuldu.
- Doğrulanmış Shopify installation'daki tek workspace ID, canonical workspace registry'ye seed edildi.
- `workspace_settings` satır sayısı `0`: reporting currency Shopify'dan veya provider'dan türetilmedi; kullanıcı seçimi bekleniyor.
- Shopify installation foreign key'i doğrulandı; orphan installation yok.
- İki yeni tabloda RLS ve force RLS açık; `PUBLIC`, `anon`, `authenticated` kapalı; `service_role` yalnız CRUD yetkili.
- OAuth transaction, provider connection ve Dataset V2 satır adetleri değişmedi.
- Canlı postcheck sonucu `PASS`.
- Advisor taramasında R2'ye ait yeni `WARN` veya performans bulgusu oluşmadı. “RLS enabled, no policy” bilgi notu server-only/no-browser-grant tasarımının beklenen sonucudur.

R2 `Done` durumundadır. Bir sonraki execution paketi R3 — Dataset V2 workspace tenant dönüşümüdür ve ayrı kapsam/onay kapısıyla başlatılır.
