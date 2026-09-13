# E10-T6-B Development Store smoke — yeni task talimatı

PR #186 (`codex/e10-t6b-development-smoke-blocked`) üzerinden devam et. Bu task yalnız `PROVIDER_TOKEN_ENCRYPTION_KEYS` secret'ının bağlı olduğu AdsTable Codex development environment/workspace içinde, secret injection'lı **yeni process** olarak açılmalıdır. Mevcut canonical keyring'i yeniden üretme, değiştirme, rotate etme veya değerini/uzunluğunu gösterme.

## Zorunlu sıra

1. Repository ve PR #186 head durumunu kontrol et; inline review comment varsa önce ele al.
2. Herhangi bir environment değeri göstermeden `npm run e10:t6b:smoke:preflight` çalıştır.
3. Yalnız aşağıdaki redacted sonuçların tamamı sağlanırsa devam et:
   - `shopify.configured: true`
   - `shopify.visible_count: 4`
   - `shopify.required_count: 4`
   - `shopify.contract_valid: true`
   - `token_keyring.valid_in_active_process: true`
   - `token_keyring.visible_count: 2`
   - `token_keyring.required_count: 2`
   - `remote_database.credentials_visible: true`
   - `ready: true`
4. Preflight PASS ise Development Store embedded App Home üzerinden gerçek session/ID token akışını başlat ve yalnız development kapsamındaki şu kabul sırasını tamamla:
   - session token doğrulaması;
   - offline token exchange;
   - Admin API Shop identity doğrulaması;
   - `shop → workspace` binding;
   - encrypted token persistence;
   - aynı shop için reopen/reinstall idempotency.
5. Secret, token, shop domain/id, workspace/user identity, URL, header, payload, ciphertext veya değer uzunluğu loglama ya da repository'ye yazma. Evidence yalnız allowlisted redacted durumlar ve boolean/count kapıları içersin.
6. İlk gerçek hata veya eksik önkoşulda fail-closed dur; sonucu ölçebildiğin katmanla sınırla. Control-plane secret varlığı hakkında yalnız `process.env` görünürlüğünden çıkarım yapma.
7. Production store/credential, billing, webhook, ShopifyQL/attribution query, initial sync, scope genişletme veya production veri işlemi yapma.
8. Başarılı kabul sonrasında redacted smoke artifact'ını, E10-T6-B dokümanını ve Execution Plan'ı güncelle; focused/full/security guard'ları çalıştır; commit'i aynı PR #186 branch'ine push et ve PR metadata'sını güncelle.

## Başarı tanımı

E10-T6-B ancak gerçek embedded Development Store session'ı üzerinden token exchange, verified Shop identity, atomic binding, encrypted persistence ve reopen/reinstall idempotency birlikte PASS olursa `Done` yapılabilir. Bunlardan biri çalıştırılmadıysa veya yalnız reachability alındıysa `Done` yazma ve E10-T6-C'yi açma.
