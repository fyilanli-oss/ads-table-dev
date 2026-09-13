# E10-T3-A — Install ve embedded authentication güvenlik çekirdeği

Bu paket E10-T3'ün credential ve route bağımsız güvenlik çekirdeğidir. Install callback HMAC doğrulaması, shop-bound tek kullanımlık state tüketimi, embedded session token imza/audience/zaman/destination/issuer doğrulaması ve server-side tenant çözümlemesi sağlar.

Browser `shop`, workspace veya user id'si authority değildir. Callback ancak authentic HMAC ve tüketilmiş state birlikte geçerse; embedded istek ancak authentic kısa ömürlü session token ile active tenant binding eşleşirse ilerler. Ham token/code/secret hiçbir çıktı sözleşmesinde bulunmaz.

Token exchange/persistence, HTTP route registration, gerçek Shopify CLI/API sürümü ve AdsTable-user↔Shopify-user policy'si E10-T3-B'de resmi sürüm yeniden doğrulamasıyla tamamlanacaktır. Bu paket E10-T3 parent'ını kapatmaz.

Production credential, Partner Dashboard, migration, scope, billing, webhook veya Shopify isteği çalıştırılmadı.
