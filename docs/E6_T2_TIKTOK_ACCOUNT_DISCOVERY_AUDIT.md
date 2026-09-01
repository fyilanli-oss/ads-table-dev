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

## Preview giriş kapısı — production onayı verildi

1 Eylül 2026 tarihli insan kararıyla, shared Supabase Auth dönüş adreslerinin read-only denetimi ve gerekiyorsa yalnız AdsTable Vercel Preview adresini kapsayan sınırlı izin değişikliği onaylandı. Bu onay TikTok production aktivasyonu, Dataset write veya production deployment onayı değildir.

Uygulama Google sign-in çağrısında Preview origin üzerindeki `/dashboard` adresini açıkça `redirectTo` olarak gönderir. Gözlenen landing dönüşü bu çağrıdan sonra oluştuğu için yeni TikTok veya login tahmini yapılmayacaktır. Önce Supabase Auth `site_url` ve redirect allow-list gerçek değerleri okunacaktır; Preview adresi listede yoksa yalnız bu adres eklenip yeniden okunarak doğrulanacaktır.

Koordinatör ortamından Supabase Management API'ye proxy üzerinden ve proxy bypass ile yapılan read-only erişim denemeleri ağ katmanında başarısız oldu; herhangi bir auth ayarı değişmedi. Yönetim yüzeyi okunmadan kullanıcıdan yeni login denemesi istenmeyecektir.

İnsan tarafından gönderilen Supabase URL Configuration ekranı kesin nedeni doğruladı: allow-list yalnız `https://dev.adstable.app/*` içeriyordu; AdsTable Preview `/dashboard` dönüş adresi yoktu. Onaylanan dar kapsam doğrultusunda tam Preview `/dashboard` adresinin eklendiği 1 Eylül 2026 tarihinde insan tarafından bildirildi. Site URL ve mevcut production wildcard değiştirilmedi. Auth kapısı şimdi tek kontrollü Preview login doğrulamasını bekler; başarılı olmadan characterization çalıştırılmaz.

Tek kontrollü Google sign-in doğrulaması başarıyla Preview `/dashboard` sayfasına döndü. Auth kapısı kabul edildi; önceki landing dönüşünün nedeni eksik redirect allow-list kaydı olarak kapatıldı. Sıradaki ve tek açık insan doğrulaması, authenticated Preview test sayfasından `Run Server Characterization` çalıştırılarak yalnız redacted field-presence sonucunun alınmasıdır.

## Characterization sonucu ve geçici yüzeyin kapatılması

Tek read-only çalışma başarıyla tamamlandı. Dokuz aday metric'in tamamı provider tarafından kabul edildi; ancak tek günlük sandbox sorgusu sıfır satır döndürdüğü için hiçbir alan response içinde gözlenmedi. Bu sonuç metric adlarının sorguda kabul edildiğini kanıtlar, fakat ATC/Checkout/Purchase anlamını, count/value eşleşmesini veya gerçek değer davranışını kanıtlamaz. Bu nedenle event alanları `unknown` kalır; eksik alan sıfır veya purchase olarak yorumlanmaz. Redacted sonuç `artifacts/e6-tiktok/e6-t2-t3-sandbox-characterization-result.json` altında saklandı.

Kanıt alındıktan sonra geçici `/api/tiktok/sandbox/characterize` route'u, test sayfası düğmesi ve yalnız bu route'a ait feature flag wiring'i aynı PR üzerinde kaldırıldı. Böylece server-held sandbox token'ı kullanan geçici probe yüzeyi merge paketinde açık bırakılmaz. Sıradaki iş kararı, kontrollü sandbox event verisi üretip non-empty kanıt almak veya delivery-only mapping ile ilerleyip tüm event alanlarını unsupported/null bırakmaktır.
