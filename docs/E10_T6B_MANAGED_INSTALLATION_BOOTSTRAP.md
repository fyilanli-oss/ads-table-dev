# E10-T6-B — Shopify-managed installation bootstrap

## Durum

`In progress / Blocked — encryption key map not injected into the smoke process`.

Kullanıcı Development App ve Development Store'u oluşturduğunu ve dört Shopify değerini Codex ortamına eklediğini onayladı. Bu çalışma sırasında değerler veya uzunlukları yazdırılmadı. Çalışan process'te `SHOPIFY` isim alanında hiçbir değişken görünmediği için credential ile Development Store smoke yapılmadı; bu sonuç değerlerin yeniden girilmesi gerektiği anlamına gelmez.

Runtime dört değeri Codex ortamında tanımlanan exact adlarla yükler: `SHOPIFY_API_KEY`, `SHOPIFY_API_SECRET`, `SHOPIFY_APP_URL` ve `SHOPIFY_DEV_STORE`. Dördü de yoksa Shopify yüzeyi kapalıdır; yalnız bir kısmı görünürse server eksik yapılandırmayla devam etmez. `SHOPIFY_DEV_STORE` değeri canonical `*.myshopify.com` domain'i olarak doğrulanır. Güvenli durum kontrolü yalnız `configured`, `visible_count` ve `required_count` döndürür; isim dışındaki değerleri veya uzunluklarını döndürmez.

## Düzeltilen kurulum sınırı

Güncel Shopify-managed installation modelinde uygulama `/admin/oauth/authorize` yönlendirmesi veya uygulamaya ait install callback route'u üretmez. Shopify kurulumu ve scope onayını yönetir. Embedded App Home, kısa ömürlü imzalı ID/session token ile backend bootstrap endpoint'ini çağırır; backend token exchange yapar.

Yeni bootstrap sırası şöyledir:

1. Session token imzası, audience, süre, destination ve issuer server-side doğrulanır.
2. Doğrulanmış token yalnız offline access token exchange için kullanılır.
3. Access token browser'a dönmeden Admin API üzerinden immutable Shop GID doğrulanır.
4. `shop → workspace` binding ile encrypted token persistence tek bir atomic install-service sınırında tamamlanır.
5. HTTP response yalnız redacted lifecycle durumu taşır.

Gerçek HTTP adapter'ları token exchange'i canonical shop'un `/admin/oauth/access_token` endpoint'ine form-encoded gönderir ve Admin identity sorgusunu sabit `2026-07` GraphQL endpoint'inde çalıştırır. Her iki adapter da Shopify hata/eksik response'unda fail-closed davranır; access token URL'ye veya response contract'ına taşınmaz.

`/auth/shopify/callback → /dashboard?shopify=installed` yolu Shopify-managed installation için yanlıştı ve route registration'dan kaldırıldı. Yerine yalnız `Authorization: Bearer <session token>` kabul eden `POST /api/shopify/bootstrap` getirildi. Query/body içindeki token, shop, workspace veya user authority değildir.

## Kalan T6-B smoke

Credential'ların yeni process'e aktarılması doğrulandıktan sonra Development Store'da yalnız şunlar çalıştırılacaktır:

- embedded App Home'dan session token alma;
- offline token exchange;
- Admin API shop identity doğrulaması;
- `shop → workspace` binding ve encrypted persistence;
- aynı shop için idempotent reopen/reinstall kontrolü.

Repository'de config loader, gerçek token-exchange/Admin API adapter'ı, server-only Supabase şeması, atomic install RPC/service ve application composition-root kaydı hazırdır. Açık insan onayı sonrasında migration uzak Supabase'e tek transaction ve migration-ledger kaydıyla uygulandı. Postcheck; boş Shopify tablosu, forced RLS, sıfır browser grant, sıfır plaintext token kolonu, tek invoker-security RPC ve tek ledger kaydı için PASS verdi. Redacted kanıt `artifacts/e10-shopify/e10-t6b-migration-acceptance.json` içindedir. Development Store kanıtı tamamlanmadan E10-T6-B `Done` veya Shopify entegrasyonu tamamlandı sayılmaz.

Scope genişletme, ShopifyQL attribution query, webhook, billing, production store veya production credential bu paketin parçası değildir.

## 2026-09-10 Development Store smoke denemesi

Dört Shopify değeri çalışan process'te, değerleri ve uzunlukları yazdırılmadan yapılan görünürlük kontrolünde 4/4 görünür durumdadır ve Development Store girdisi canonical `*.myshopify.com` doğrulamasını geçmiştir. Bu kontrol artık `npm run e10:t6b:smoke:preflight` ile tekrarlanabilir; yalnız redacted boolean/count alanları üretir ve herhangi bir gerekli secret eksikse non-zero çıkışla fail-closed olur. Remote Supabase salt-okunur postcheck; tablo, invoker RPC, migration ledger, forced RLS, sıfır browser grant ve sıfır plaintext token kolonu için yeniden PASS vermiştir.

Smoke preflight somut olarak başlatılmış ve fail-closed durdurulmuştur. Keyring'in iki gerekli girdisinden active key kimliği görünür, fakat encryption key map çalışan smoke process'ine enjekte edilmemiştir; redacted sonuç 1/2 görünürdür ve preflight/runtime bu nedenle hazır değildir. Vercel deployment ve CI PASS'tir; operator ortamından App URL route probe ağ geçidinde engellendiğinden önceki “App URL unreachable” ifadesi uygulama arızası olarak yorumlanmamalıdır. Ayrı, credentialsız Development Store reachability probe'u HTTP 4xx yanıtıyla ağ temasını doğrulamış fakat authentication veya install kabulü üretmemiştir. Bu nedenle gerçek session/ID token, offline exchange, Admin Shop identity, binding, encrypted persistence ve ikinci açılış/reinstall adımları çalıştırılmamıştır. Redacted sonuç `artifacts/e10-shopify/e10-t6b-development-store-smoke.json` içindedir. E10-T6-B `Done` değildir ve E10-T6-C kapalı kalır.
