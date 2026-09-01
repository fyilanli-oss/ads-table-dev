# E6-T2 hazırlığı — TikTok account discovery ve sandbox denetimi

## Kesinleşen mevcut davranış

OAuth callback'in ürettiği connected access token, TikTok'un resmî OAuth advertiser discovery endpoint'ine gönderilir: `https://business-api.tiktok.com/open_api/v1.3/oauth2/advertiser/get/`. İstek `app_id`, `secret` ve `Access-Token` kullanır. Bu, resmî TikTok Business API SDK `AuthenticationApi.md` sözleşmesiyle aynıdır.

Canlı bağlantının redacted metadata denetimi de `platform_connections.access_token` ve `https://business-api.tiktok.com/open_api` sonucunu verdi. Dolayısıyla ekrandaki boş liste sandbox sorgusundan değil, OAuth token'ına bağlı resmî advertiser listesinin boş dönmesinden oluşmuştur.

## Sandbox ayrımı

Repository'deki sandbox yolu OAuth advertiser discovery'nin alternatifi değildir. Ayrı bir `sandbox-ads.tiktok.com/open_api` host'u ve ayrı manuel sandbox access token kullanır. E1-T5 güvenlik kararı gereği sandbox token ve sandbox/review fallback production runtime'da fail-closed yasaktır. OAuth token'ını sandbox host'una göndermek doğru bir düzeltme olmaz.

E6-T2/E6-T3 gerçek event karakterizasyonuna başlamadan önce iş kararı gerekir: mevcut OAuth review advertiser yetkisi TikTok tarafında düzeltilmeli veya ayrı, production olmayan sandbox deployment/token akışı açıkça onaylanmalıdır. Bu karar verilmeden kimlik hard-code edilmeyecek ve production güvenlik guard'ı gevşetilmeyecektir.

## Onaylanan non-production sandbox akışı

İnsan iş kararıyla ayrı non-production sandbox yolu seçildi. Yalnız `VERCEL_ENV=preview|development` ortamında, sandbox ve force-report switch'leri birlikte açıkken server-configured sandbox advertiser account picker'a döner. Token browser'a veya API cevabına girmez. Hesap seçildiğinde yalnız `reportBase=sandbox-ads.tiktok.com`, `tokenSource=server_sandbox_access_token` ve sandbox işareti connection metadata'sına yazılır; report runtime token'ı server environment'tan alır.

Production startup sandbox switch, token veya advertiser değişkenlerinden herhangi birini görürse fail-closed kalır. Normal OAuth advertiser discovery yolu kaldırılmadı ve sandbox switch'leri kapalıyken aynen çalışır.

## Preview doğrulama sonucu — başarısız

PR #130 Preview denemesi kabul edilmedi ve PR merge edilmeden kapatıldı. Preview hostname ayrı olmasına rağmen aynı Supabase projesini kullandığı için kullanıcı oturumu hostname değişiminde taşınmadı; girişten sonra da mevcut TikTok connection kaydı `Disconnect` olarak göründü. Sandbox readiness tamamlanmadığından `Connect` sandbox yoluna girmedi, OAuth fallback çalıştı ve aynı boş advertiser listesi döndü.

Bu sonuç, yalnız ayrı Vercel URL'sinin ayrı sandbox ortamı olmadığını kanıtlar. Gerçek izolasyon için ayrı non-production Supabase auth/data plane'i ve eksiksiz Preview sandbox yapılandırması gerekir. Bunlar olmadan yeni retry yapılmaz; shared production data üzerinde test connection üretmek ayrı ortam kabul edilmez.

## Onaylanan geçici read-only characterization

Ayrı veritabanı yerine insan onayıyla dar kapsamlı geçici yol seçildi. Authenticated `/api/tiktok/sandbox/characterize` yalnız non-production feature flag ile açılır; server-held sandbox token ve advertiser ID kullanarak event metric adaylarını tek günlük, Ad-level BASIC raporda ayrı ayrı probe eder. Endpoint connection, ownership, snapshot, refresh job veya Dataset V1/V2 yazmaz. Ham response, metric değeri, token ve advertiser kimliği dönmez; yalnız aday alanın provider tarafından kabul edilip edilmediği, cevapta bulunup bulunmadığı ve zero-row/non-empty şekli döner.

TikTok test sayfasındaki `Run Server Characterization` düğmesi bu endpoint'i çağırır. Karakterizasyon tamamlanınca route/flag kapatılır. Bu geçici review mekanizması production OAuth advertiser keşfinin veya nihai production TikTok bağlantısının yerine geçmez.

## Modal düzeltmesi

Advertiser listesi başarılı fakat boş döndüğünde reconnect URL parametreleri artık modal gösterilirken tüketilir. `Close` sayfayı yenilese bile aynı account-selection akışı tekrar açılmaz.
