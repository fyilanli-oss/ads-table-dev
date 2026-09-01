# E6-T2 hazırlığı — TikTok account discovery ve sandbox denetimi

## Kesinleşen mevcut davranış

OAuth callback'in ürettiği connected access token, TikTok'un resmî OAuth advertiser discovery endpoint'ine gönderilir: `https://business-api.tiktok.com/open_api/v1.3/oauth2/advertiser/get/`. İstek `app_id`, `secret` ve `Access-Token` kullanır. Bu, resmî TikTok Business API SDK `AuthenticationApi.md` sözleşmesiyle aynıdır.

Canlı bağlantının redacted metadata denetimi de `platform_connections.access_token` ve `https://business-api.tiktok.com/open_api` sonucunu verdi. Dolayısıyla ekrandaki boş liste sandbox sorgusundan değil, OAuth token'ına bağlı resmî advertiser listesinin boş dönmesinden oluşmuştur.

## Sandbox ayrımı

Repository'deki sandbox yolu OAuth advertiser discovery'nin alternatifi değildir. Ayrı bir `sandbox-ads.tiktok.com/open_api` host'u ve ayrı manuel sandbox access token kullanır. E1-T5 güvenlik kararı gereği sandbox token ve sandbox/review fallback production runtime'da fail-closed yasaktır. OAuth token'ını sandbox host'una göndermek doğru bir düzeltme olmaz.

E6-T2/E6-T3 gerçek event karakterizasyonuna başlamadan önce iş kararı gerekir: mevcut OAuth review advertiser yetkisi TikTok tarafında düzeltilmeli veya ayrı, production olmayan sandbox deployment/token akışı açıkça onaylanmalıdır. Bu karar verilmeden kimlik hard-code edilmeyecek ve production güvenlik guard'ı gevşetilmeyecektir.

## Modal düzeltmesi

Advertiser listesi başarılı fakat boş döndüğünde reconnect URL parametreleri artık modal gösterilirken tüketilir. `Close` sayfayı yenilese bile aynı account-selection akışı tekrar açılmaz.
