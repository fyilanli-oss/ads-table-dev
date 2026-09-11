# E10-T6-C — Embedded provider OAuth preflight

**Durum:** `In progress — C1 transaction authority bridge hazır; C2 workspace connection boundary açık`

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

Migration yalnız repository artefaktıdır; remote Supabase'e uygulanmadı. Provider consent/token exchange çalıştırılmadı. Sıradaki repository dilimi C2 workspace-scoped encrypted provider connection persistence ve callback adapter'ıdır.
