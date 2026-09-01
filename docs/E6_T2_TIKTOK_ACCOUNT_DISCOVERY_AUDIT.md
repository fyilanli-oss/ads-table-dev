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

Sandbox token'ın Vercel'e eklenmesinden sonra yeni Preview deployment gerekir. Authenticated `/api/tiktok/status` cevabı credential değeri veya advertiser kimliği göstermeden yalnız `sandbox.non_production`, `sandbox.enabled` ve `sandbox.ready` boolean alanlarını döndürür. Böylece eksik dış ortam yapılandırması tahmin edilmeden doğrulanır.

Preview readiness `ready=true` olduğunda normal TikTok `Connect` düğmesi OAuth'a gitmez: server-side sandbox connection oluşturur ve doğrudan sandbox advertiser seçim modalına döner. Readiness false ise mevcut OAuth start handler aynen korunur. Sandbox token browser'a taşınmaz.

## Modal düzeltmesi

Advertiser listesi başarılı fakat boş döndüğünde reconnect URL parametreleri artık modal gösterilirken tüketilir. `Close` sayfayı yenilese bile aynı account-selection akışı tekrar açılmaz.
