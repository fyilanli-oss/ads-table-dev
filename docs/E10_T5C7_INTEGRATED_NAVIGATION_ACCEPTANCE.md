# E10-T5-C7 — Integrated navigation ve acceptance freeze'i

**Durum:** `Done — C1–C6 yüzeyleri tek Shopify embedded ürün akışında birleştirildi; UI/runtime implementasyonu yapılmadı`  
**Karar tarihi:** 2026-09-09  
**Production etkisi:** Yok

## İş çıktısı ve ilk kullanım

Merchant, kurulumdan rapora kadar tek Shopify embedded uygulama içinde ilerler. İlk kullanım `verified Shopify session → Currency → Platforms verified account → conditional Klaviyo Email Monthly Plan Cost → Funnel` sırasıdır. Eksik currency Currency'ye, hiç aktif account bulunmaması Platforms'a yönlendirir; browser route/query kapıları atlatamaz. C1'in Funnel açılış kararı korunur.

## Tek uygulama navigasyonu

Implementation anındaki güncel resmi Shopify embedded navigation/App Bridge yüzeyi şu sırayla kullanılır: `Funnel`, `Dashboard`, `Ad Analysis`, `Attribution Differences`, `Platforms`, `Settings`. Funnel içindeki Funnel/Table aynı backend sonucunun renderer switch'idir; global navigation öğesi değildir. Data Sources canonical Platforms route'una gider. Shopify Admin shell'i yeniden çizilmez; ayrı AdsTable global shell, login/dashboard dönüşü, mobil hamburger veya bottom-nav yapılmaz.

## Context taşıma

Server-authoritative shop/workspace, workspace currency ve tek aktif account ortaktır. Dashboard `View Funnel`; completed-day range, comparison, filters, currency, data sources ve account'u taşır. Attribution `View Funnel`; completed-day range, platform, currency ve account'u taşır. Funnel/Table; time, comparison, filters, currency, data sources, expanded hierarchy ve focus metric'i korur.

Direct navigation hedefin last-valid state'ini kullanır. Desteklenmeyen kaynak state sessizce dönüştürülmez; hedef onaylı default/last-valid state ve nötr notice kullanır. Currency/account Settings'te değişirse eski query/cache context invalidate edilir.

## OAuth ve route state

OAuth transaction server-side `shop/workspace/user/provider/surface/return_target` bağını taşır. Callback canonical Platforms route'una döner; session/status yenilenir ve yalnız allowlisted hedefe geçilir. Caller return URL, standalone `/login`/`dashboard`, token/code/raw error yasaktır. Disconnect aktif account'u geçersiz kılarsa stale analytics gösterilmez.

Her route `loading`, `empty`, `partial`, `stale`, `reauthorization_required`, `forbidden`, `not_found`, `error` durumlarını resmi Shopify componentleriyle gösterir. Unsupported/unknown `0`a dönüşmez; route değişimi frontend formula, aggregation, hierarchy veya attribution üretmez.

## Desktop/mobile/accessibility kabulü

Desktop ve mobile aynı bilgi mimarisini kullanır. Shopify responsive navigation; keyboard sırası, heading focus, modal focus return, App Bridge/browser back-forward ve deep link test edilir. 320px viewport'ta global yatay taşma, kesilmiş primary action, desktop iframe sıkıştırması, duplicate shell, supported context kaybı, silent coercion, iki aktif account, stale account sonucu, open redirect, custom mobile navigation, custom control framework veya secret/PII/identity/raw-error sızıntısı acceptance failure'dır.

Exact App Bridge navigation API'si, navigation component'i, deep-link/host/back davranışı ve responsive property'ler E10-T6-A'da güncel resmi Shopify kaynaklarıyla doğrulanır. Deprecated component taklit edilmez.

## Kapsam ve sıra

Bu freeze UI/runtime kodu, Partner Dashboard/Development Store, credential/scope/redirect/webhook, provider çağrısı, migration, deployment veya production işlemi yapmaz.

C7 ile E10-T5-C output/display ürün sözleşmesi `Done`dır. Sıradaki uygulanabilir repository işi **E10-T6-A — Official capability ve development-readiness**tir. E10-T6-A Shopify'a gerçek temas kurmaz; yalnız güncel resmi gereksinim matrisi ve PASS/BLOCKED readiness evidence üretir.
