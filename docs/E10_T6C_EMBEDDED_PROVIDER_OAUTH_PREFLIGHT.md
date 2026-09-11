# E10-T6-C — Embedded provider OAuth preflight

**Durum:** `In progress — C1/C2A development migration PASS; C2C runtime gate hazır`

E10-T6-B Development Store install/session kabulü PASS olduktan sonra E10-T6-C için repository sınırı incelendi. Canlı provider consent başlatılmadı. Mevcut provider OAuth başlangıcı standalone AdsTable kullanıcısını `requireConnectAccess` ile doğruluyor; OAuth transaction yalnız `user_id/provider/redirect_uri` taşıyor, provider callback'leri bağlantıyı aynı standalone `user_id` ile kaydediyor ve `/dashboard` yüzeyine dönüyor.

Shopify embedded sözleşmesi ise doğrulanmış session'dan gelen `shop_id/workspace_id/shopify_user_id` authority'sini; `surface=shopify_embedded` ve sabit `/shopify/app/platforms` dönüş hedefiyle aynı tek-kullanımlık transaction'a bağlamayı gerektiriyor. `workspace_id` bir Supabase auth user kimliği değildir ve birbirlerinin yerine kullanılamaz. Query/body içindeki shop, workspace, user, surface veya return URL authority kabul edilemez.

Bu nedenle mevcut `/auth/:provider` route'larını App Home'dan doğrudan açmak IDOR ve yanlış tenant'a provider token yazma riski taşır. E10-T6-C canlı smoke fail-closed durduruldu. Sıradaki repository slice şunları birlikte teslim etmelidir:

1. Shopify session ile doğrulanan workspace-scoped OAuth start boundary;
2. transaction persistence içinde shop/workspace/Shopify user/provider/surface/allowlisted return target;
3. callback'te bu transaction'ın tek kullanımlık tüketimi;
4. workspace-scoped encrypted provider connection persistence;
5. callback sonrası yalnız canonical embedded Platforms dönüşü;
6. connect/account-selection/status için tenant-isolation ve replay testleri.

Bu sınırlar tamamlanmadan provider consent, provider token exchange, production provider hesabı veya production store teması yapılmaz. Executable preflight kararı `contracts/shopify/e10-t6c-provider-oauth-preflight.json` içindedir.


## E10-T6-C1 — Transaction authority bridge

Repository hazırlığı, standalone `auth.users.id` yetkisini zayıflatmadan ikinci ve ayrık bir `shopify_embedded` transaction authority şekli ekler. Embedded transaction yalnız doğrulanmış Shopify session context'inden `shop_id`, `workspace_id` ve `shopify_user_id` alır; `user_id` yerine workspace kimliği geçirilmez. Shop/workspace çifti mevcut server-only installation kaydıyla foreign key üzerinden bağlıdır ve dönüş hedefi yalnız `/shopify/app/platforms` olabilir. Atomic consume bütün authority alanlarını taşır.

Migration onaylanan development Supabase ortamına uygulandı ve authority kolonları ile atomic consume dönüş sözleşmesi postcheck'te doğrulandı. Provider consent/token exchange çalıştırılmadı.


## E10-T6-C2A — Workspace provider connection boundary

Consumed embedded transaction içindeki doğrulanmış shop/workspace/provider authority'si, standalone kullanıcı bağlantılarından ayrı server-only tabloya yazılır. Tokenlar yazılmadan önce mevcut AES-256-GCM vault ile workspace-bound AAD kullanılarak şifrelenir. İlk durum `pending_account_selection`dır; server-side doğrulanmış aktif hesap olmadan `connected` olunamaz. Development migration postcheck'i forced RLS, browser-role grant reddi, service-role erişimi ve boş başlangıç tablolarını doğruladı. Redacted sonuç `artifacts/e10-shopify/e10-t6c-development-migration-acceptance.json` içindedir.


## E10-T6-C2B — Embedded start/callback adapter

Start yalnız doğrulanmış Shopify session context’inden embedded transaction üretir ve top-level consent navigasyonu döndürür. Callback, state’i atomik tüketip surface/provider/sabit return target doğrulamasından önce token exchange yapmaz; tokenlar workspace store’a yazılır ve sonuç yalnız canonical Platforms yüzeyine döner. Bu paket runtime route açmaz ve provider teması yapmaz.

C2C, embedded provider route'larını varsayılan olarak kapalı ve katı boolean feature flag arkasında tanımlar. Start authority yalnız Authorization bearer Shopify session üzerinden taşınır; callback yalnız state/code alanlarını adapter'a geçirir ve başarı veya hata halinde canonical Platforms yüzeyinden çıkamaz. Bilinmeyen provider'lar reddedilir; eksik/replayed state adapter'ın atomic consume sınırında fail-closed kalır.

Runtime flag'in açılması beş provider adapter'ının da eksiksiz server-side composition ile verilmesini gerektirir; kısmi wiring uygulamayı başlangıçta durdurur. Development migration onayı bu flag'i açma, provider consent, provider hesabı veya production deployment onayı olarak yorumlanmaz.
