# R6-D3-D — Meta workspace salt-okunur preflight

## Analist özeti

Bu paket yeni bir Meta veri motoru geliştirmez. E4'te tamamlanmış Meta Client, Campaign → AdSet → Ad mapper, metrik sözleşmesi ve Time/FX katmanını Shopify tarafından doğrulanan workspace ve canonical Meta bağlantısıyla çalıştıran ince bir runtime sınırı ekler.

## Kullanıcı açısından sonuç

Kullanıcı yeni bir metrik seçmez ve normal Data Sources ekranında yeni bir kontrol görmez. AdsTable, seçilmiş 1–3 Meta hesabının tamamını provider'dan yeniden doğrular; her hesabın kendi timezone ve source currency bilgisini kullanarak önceki kapanmış business date için günlük Ad Insights okur. Sonuç yalnız aggregate PASS/FAIL olarak gösterilir.

## Neden gerekli?

E4 Meta motoru legacy `user_id`, eski connection/ownership ve `server.js` manuel Refresh hattına bağlanmıştı. Shopify embedded bağlantısı ise `workspace_id`, `workspace_provider_connections` ve merchant-selected reporting currency kullanır. Eksik olan mapper veya metrik değil, mevcut E4 motorunun bu yeni authority zincirine bağlanmasıdır.

## Değişmeyen E4 kararları

- Campaign → AdSet → Ad hiyerarşisi korunur.
- `ad_click = link_click`; Meta `clicks` yalnız evidence alanıdır.
- Impressions, spend, add to cart, checkout ve purchase count/value alanları E4 sözleşmesinden gelir.
- Provider DTO doğrudan Dataset V2'ye geçmez.
- Derived KPI, sahte sıfır veya yeni metrik tahmini yapılmaz.

## Salt-okunur çalışma sırası

1. Shopify session server-side workspace authority üretir.
2. Aynı workspace'in canonical `connected` Meta kaydı okunur.
3. Seçilmiş hesap sayısı 1–3 olarak doğrulanır.
4. Token expiry ve `ads_read` kapsamı fail-closed doğrulanır.
5. Meta `/me/adaccounts` bütün seçilmiş hesapları yeniden doğrular.
6. Her hesabın currency ve timezone bilgisi canonical seçimle karşılaştırılır.
7. Workspace reporting currency yalnız `merchant_selected` kaydından okunur.
8. Her hesap için kendi timezone'undaki önceki kapanmış business date sorgulanır.
9. E4 mapper ve Time/FX katmanı sonucu doğrular.
10. Dış sonuç yalnız hesap/satır sayısı, empty/non-empty, Time/FX ve yazma durumunu taşır.

## Güvenlik ve veri etkisi

- Browser workspace, tarih, hesap adı, currency veya timezone sağlayamaz.
- Token, hesap ID'si, entity ID'si ve ham metrik değerleri response'a çıkmaz.
- Dataset V2 yazımı yoktur.
- V1 snapshot yazımı yoktur.
- Schedule, backfill ve production activation yoktur.
- Legacy connection ve ownership tabloları kullanılmaz.

Normal Data Sources görünümü değişmez. Kabul yüzeyi yalnız açık operatör parametresiyle `/shopify/app/platforms?acceptance=r6d3-meta` adresinde görünür.

## Repository kanıtı

- Workspace runner: `src/providers/meta/workspace-runner.js`
- Salt-okunur preflight: `src/providers/meta/read-only-preflight.js`
- Shopify session-bound route: `src/routes/shopify-ad-account-routes.js`
- Gizli operatör yüzeyi: `src/shopify/embedded-app-home.js`
- Kabul testleri: `tests/r6d3d-meta-read-only-preflight.test.js`

## Durum

`PASS repository only — production deployment ve gerçek Meta read-only acceptance bekliyor`
