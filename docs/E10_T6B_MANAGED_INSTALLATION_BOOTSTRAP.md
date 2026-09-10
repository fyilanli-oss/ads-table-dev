# E10-T6-B — Shopify-managed installation bootstrap

## Durum

`In progress / environment unavailable in this process`.

Kullanıcı Development App ve Development Store'u oluşturduğunu ve dört Shopify değerini Codex ortamına eklediğini onayladı. Bu çalışma sırasında değerler veya uzunlukları yazdırılmadı. Çalışan process'te `SHOPIFY` isim alanında hiçbir değişken görünmediği için credential ile Development Store smoke yapılmadı; bu sonuç değerlerin yeniden girilmesi gerektiği anlamına gelmez.

Runtime dört değeri `SHOPIFY_API_KEY`, `SHOPIFY_API_SECRET`, `SHOPIFY_APP_URL` ve `SHOPIFY_DEV_STORE_DOMAIN` adlarıyla fail-closed yükler. Dördü de yoksa Shopify yüzeyi kapalıdır; yalnız bir kısmı görünürse server eksik yapılandırmayla devam etmez. Güvenli durum kontrolü yalnız `configured`, `visible_count` ve `required_count` döndürür; isim dışındaki değerleri veya uzunluklarını döndürmez.

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

Repository'de config loader, gerçek token-exchange/Admin API adapter'ı, server-only Supabase şeması, atomic install RPC/service ve application composition-root kaydı hazırdır. Migration bu pakette yalnız versioned artifact olarak eklenmiştir; açık production onayı olmadan uzak Supabase projesine uygulanmamıştır. Development Store kanıtı tamamlanmadan E10-T6-B `Done` veya Shopify entegrasyonu tamamlandı sayılmaz.

Scope genişletme, ShopifyQL attribution query, webhook, billing, production store veya production credential bu paketin parçası değildir.
