# E10-T2 — Shop/workspace tenant modeli

> **20 Eylül 2026 R1 düzeltmesi:** Bu belge E10-T2'nin tarihsel Shopify adapter kararını kaydeder. Canonical AdsTable tenant anahtarı artık `workspace_id` olarak dondurulmuştur; `shop_id` yalnız doğrulanmış Shopify installation identity'sidir. Güncel ve üstün sözleşme `contracts/r1-workspace-authority-v1.json` ile `docs/R1_WORKSPACE_AUTHORITY_DECISION.md` dosyalarındadır.

## Dondurulan ilk sürüm modeli

- Bir doğrulanmış Shopify shop kimliği tam bir AdsTable workspace'e bağlanır; bir workspace ikinci shop kabul etmez.
- `shop_id` immutable Shopify installation kimliğidir ve server-side `workspace_id` binding'ini çözmekte kullanılır. `*.myshopify.com` domain'i doğrulanmış yeniden kurulumda değişebilir ve geçmişi tutulur; ne domain ne de `shop_id` canonical AdsTable tenant primary key'idir.
- Reinstall aynı `shop_id` ve aynı `workspace_id` bağını korur, install generation'ı ilerletir. Farklı shop kimliği mevcut bağın üzerine yazılamaz.
- Browser query/body içindeki `shop_id` veya `workspace_id` authority değildir. Server-side context yalnız Shopify tarafından doğrulanmış shop identity ile kalıcı binding eşleşmesinden türetilir; çelişen caller claim fail-closed reddedilir.
- Agency/multi-store model ertelenmiştir. Gelecekte genişleme, bu one-to-one sözleşmeyi sessizce gevşetmek yerine versionlı yeni model gerektirir.

## Executable contract

`src/shopify/tenant-model.js` persistence-independent domain sınırıdır. Binding creation, verified reinstall/domain reconciliation, one-to-one registry doğrulaması ve tenant context türetimini kapsar. `tests/e10-t2-shop-workspace-tenant.test.js` immutable identity, domain değişimi, reinstall, uniqueness ve caller-claim saldırılarını doğrular.

## Kapsam dışı ve güvenlik kapıları

Bu task migration veya production tablo oluşturmaz; install callback/session-token doğrulaması E10-T3, uninstall/privacy ve destructive retention E10-T4 kapsamındadır. Shopify Partner Dashboard, credential, scope, billing, webhook, provider veya production işlemi yapılmaz.

## Çıkış

E10-T2 repository tenant contract'ı `Done`; parent E10 `In progress`. Sıradaki uygulanabilir iş **E10-T3 — Install ve embedded authentication** hazırlığıdır.
