# R2-A — Embedded migration ledger uzlaştırması

## Durum

`Done — 20 Eylül 2026`. Kullanıcının açık onayıyla yalnız iki doğrulanmış migration geçmiş kaydı canlı ledger'a eklendi. R2 foundation migration'ı uygulanmadı.

## İş çıktısı

Canlı Shopify embedded şeması ile repository migration geçmişinin yeniden aynı sıraya getirilmesi. Bu işlem iş verisini, tokenları, bağlantı durumunu veya provider grant'ini değiştirmez; yalnız canlıda zaten var olduğu doğrulanan iki migration'ın migration ledger durumunu düzeltir.

## Salt okunur doğrulama sonucu

- `20260911130000_add_embedded_oauth_authority.sql` tarafından beklenen workspace OAuth kolonları, authority constraint'i, shop/workspace foreign key'i ve consume function canlıda mevcut.
- `20260911150000_create_shopify_workspace_provider_connections.sql` tarafından beklenen tablo, primary key, provider/status/token kontrolleri, foreign key, RLS ve server-only erişim canlıda mevcut.
- Bu iki version `supabase_migrations.schema_migrations` ledger'ında kayıtlı değil.
- Klaviyo completion migration'ı canlı ledger'da `20260919194248` olarak kayıtlıdır; repository dosyası aynı version'a getirildi.
- E9 backfill migration'ları canlıda yoktur; `applied` olarak işaretlenmeyecek ve R2-A kapsamında oluşturulmayacaktır.

## Uygulanan kontrollü işlem

1. `20260911130000` ve `20260911150000` için schema fingerprint tekrar kontrol edildi.
2. Tek transaction içinde yalnız bu iki version migration ledger'a eklendi.
3. Migration listesinde iki version ve doğru adları doğrulandı.
4. Tablo/constraint/grant/RLS fingerprint tekrar çalıştırıldı; iş şeması ve satır adetleri değişmedi.
5. R2 preflight tekrar çalıştırıldı ve `PASS` verdi.

## Yasaklar

- Eksik E9/backfill migration'larını `applied` gibi göstermek.
- Embedded tabloları veya kolonları yeniden oluşturmak.
- Token, OAuth transaction veya provider connection satırlarını değiştirmek.
- Klaviyo revoke/refresh/OAuth çağrısı yapmak.
- Toplu `db push` ile bekleyen bütün migration'ları çalıştırmak.

## Sonuç ve sonraki onay kapısı

Kanıt: `docs/security/evidence/R2A_EMBEDDED_MIGRATION_LEDGER_RECONCILIATION_2026-09-20.json`.

R2-A tamamlandı. `public.workspaces` ve `public.workspace_settings` henüz oluşturulmadı. R2 foundation migration'ının canlıya uygulanması ayrı açık kullanıcı onayı gerektirir.
