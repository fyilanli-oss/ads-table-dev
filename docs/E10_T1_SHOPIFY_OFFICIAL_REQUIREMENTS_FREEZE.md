# E10-T1 — Shopify resmi gereksinimler karar kaydı

**Durum:** `Done (repository freeze)`  
**Doğrulama tarihi:** 2026-09-08  
**Kapsam:** Shopify Public Embedded App foundation için implementation öncesi gereksinim dondurma  
**Production etkisi:** Yok

## Karar

AdsTable, Shopify App Store üzerinden dağıtılacak embedded bir public app olarak tasarlanacaktır. Bu kayıt yalnız implementation sözleşmesini dondurur. Partner Dashboard ayarı, production credential, scope talebi, billing aktivasyonu, migration, webhook kaydı veya App Store gönderimi yapmaz.

Resmi belgede açıkça doğrulanmayan bir davranış güvenlik ya da ürün sözleşmesi sayılmaz. Shopify'ın yönetim ekranında uygulamaya özgü olarak belirlediği scope/protected-data/review sonucu da repository varsayımıyla ikame edilmez.

## Resmi kaynak matrisi

| Alan | Resmi Shopify kaynağı | Dondurulan AdsTable sözleşmesi |
|---|---|---|
| Dağıtım | [Select a distribution method](https://shopify.dev/docs/apps/launch/distribution/select-distribution-method) | Hedef App Store dağıtımıdır. Custom distribution veya admin-created custom app akışları public ürün install sözleşmesi yapılmaz. Nihai listing/distribution seçimi insan onay kapısıdır. |
| Embedded auth | [Session tokens](https://shopify.dev/docs/apps/build/authentication-authorization/session-tokens) ve [Token exchange](https://shopify.dev/docs/apps/build/authentication-authorization/access-tokens/token-exchange) | Browser girdisindeki shop/user/workspace yetki vermez. Backend, kısa ömürlü session token'ı doğrular; API erişimi server-side token lifecycle'ından geçer. Token exchange'ın uygulanabilirliği seçilen resmi app template/API sürümüyle implementation başlangıcında yeniden doğrulanır. |
| App Bridge | [App Bridge library](https://shopify.dev/docs/api/app-bridge-library) | Embedded shell Shopify'ın güncel App Bridge entegrasyonunu kullanır. Host/frame/navigation parametreleri tenant authority değildir; backend authorization ayrı doğrulanır. |
| Billing | [About app billing](https://shopify.dev/docs/apps/launch/billing) ve [Managed app pricing](https://shopify.dev/docs/apps/launch/billing/managed-pricing) | Shopify-origin merchant entitlement'ı yalnız server-side doğrulanmış billing state'ten doğar. Managed pricing ile Billing API seçimi, plan kabiliyetleri ve Partner Dashboard uygunluğu doğrulanmadan kodlanmaz. |
| Privacy webhooks | [Privacy law compliance](https://shopify.dev/docs/apps/build/privacy-law-compliance) | Zorunlu compliance topic'leri için gerçek HMAC doğrulama, hızlı acknowledgement, idempotent işleme ve denetlenebilir retention/deletion kararı gerekir. Sadece route varlığı compliance kabulü değildir. |
| Protected customer data | [Protected customer data](https://shopify.dev/docs/apps/launch/protected-customer-data) | İlk dilimde customer PII istenmez. Her scope/alan görünür bir ürün çıktısına bağlanır; gerekirse protected-data erişimi ayrı inceleme ve insan onayından geçer. |
| App gereksinimleri | [App requirements checklist](https://shopify.dev/docs/apps/launch/app-requirements-checklist) | Install, authentication, embedded UX, billing, support, privacy ve quality kapıları E10-T10 evidence paketinde test edilir. Checklist sürümü submission öncesi tekrar sabitlenir. |
| App review | [App review process](https://shopify.dev/docs/apps/launch/app-store-review/review-process) | Test store, çalışan reviewer erişimi, doğru talimatlar ve bağlı provider olmadan incelenebilir empty/demo state geliştirmeyle birlikte tutulur. Submission ayrı açık production onayıdır. |

## Implementation için fail-closed hükümler

1. **Authority:** Shopify tarafından doğrulanmamış shop, user veya workspace kimliği backend yetkisi üretmez. Session token doğrulaması tenant eşlemesinin yerine geçmez; ikisi de server-side zincirde kanıtlanır.
2. **Secrets:** Access token, webhook secret, session material ve müşteri verisi browser response'una, URL'ye, analytics event'e veya log'a yazılmaz. Token mevcut encrypted vault ilkelerine bağlanır.
3. **Scope:** Scope matrisi ürün çıktısı → gerekli resource/field → retention → deletion gerekçesi taşımadan scope eklenmez. "İleride gerekebilir" gerekçe değildir.
4. **Commerce provenance:** Shopify-reported platform attribution, provider-reported conversion ve gelecekteki AdsTable overlap/attribution sonucu ayrı provenance ile saklanır; tek fact gibi birleştirilmez. İlk intake yalnız platform Purchase Count ve Sales Value ile sınırlıdır.
5. **Lifecycle:** Uninstall, erişim kaybı, reauthorization ve compliance lifecycle yeni provider/Shopify çağrılarını fail-closed durdurur. Retention/deletion politikası E10-T4'te dondurulmadan destructive işlem yazılmaz.
6. **Billing:** Client bildirimi entitlement değildir. Active/frozen/cancelled/trial gibi durumlar E10-T7'de seçilen resmi billing modeline göre backend tarafından doğrulanır.
7. **Review:** Review readiness E10-T9 boyunca yaşayan bir artefakttır; uygulamanın sonunda geriye dönük hazırlanmaz.

## Açık kararlar ve yeniden doğrulama tetikleyicileri

Aşağıdakiler E10-T1 kapsamında tahmin edilmemiştir ve ilgili task başlamadan resmi doküman + uygulamaya özgü yönetim ekranı üzerinden yeniden doğrulanır:

- exact Admin API sürümü, Shopify CLI/app template sürümü ve App Bridge paket sürümü;
- exact access-token tipi ve token exchange yolu;
- minimum commerce scope/field listesi ve protected customer data sınıflandırması;
- Managed app pricing ile Billing API seçimi, trial ve plan geçiş davranışı;
- zorunlu webhook topic/URI seti, delivery retry ayrıntıları ve retention süreleri;
- listing kategorisi, review credential biçimi ve submission anındaki checklist.

Resmi belgede değişiklik, deprecation bildirimi, API version sunset, Partner Dashboard uyarısı veya review geri bildirimi görülürse bu freeze güncellenmeden implementation devam etmez.

## Task çıkış kararı

E10-T1 repository gereksinim freeze'i tamamlanmıştır. Sıradaki uygulanabilir iş **E10-T2 — Shop/workspace tenant modeli**dir. E10 parent `In progress` kalır; bu belge install/auth/billing/webhook implementation kabulü veya production onayı değildir.
