# E10-T6-C2I-V9 — Real-device Shopify-native Acceptance

**Durum:** `Execution approved / human text confirmation pending`
**Provider teması:** Yasak

## Düzeltme: resim zorunlu değil

Bu kabul için ekran görüntüsü, PNG dönüşümü, SHA-256, metadata temizliği, JSON manifesti, validator veya GitHub yüklemesi istenmez. Önceki kanıt zinciri gereksizdi ve kaldırıldı. Görsel yalnız kullanıcı bir sorunu göstermek isterse isteğe bağlıdır.

## Tek işlem

1. Shopify Admin içinden AdsTable **App Home** ekranını aç.
2. `Manage data sources` ile **Data Sources / Platforms** ekranına geç.
3. Hiçbir provider `Connect` düğmesine basma.
4. İki ekran da Shopify-native görünüyorsa şu tek satır yeterlidir:

> App Home açıldı; Data Sources / Platforms açıldı; Connect'e basmadım.

Bir ekran açılmadıysa resim yüklemek yerine yalnız hangi ekranın açılmadığını yazmak yeterlidir.

## Kabul sınırı

Metin onayı gelmeden V9 `PASS` sayılmaz. Bu gözlem OAuth, provider consent, callback, account discovery, token exchange, provider API isteği, deployment veya Production mutation yetkisi vermez.

Shopify-native değerlendirmesinin resmi kaynakları [App Home](https://shopify.dev/docs/api/app-home), [`s-app-nav`](https://shopify.dev/docs/api/app-home/latest/app-bridge-web-components/app-nav), [Page](https://shopify.dev/docs/api/app-home/latest/web-components/structure/page) ve [Button](https://shopify.dev/docs/api/app-home/latest/web-components/actions/button) belgeleridir.

## Paket sırası

- **Tamamlanan paket:** V9 kabul sürecinin resim/hash/manifest zorunluluğunu kaldıran düzeltme.
- **Sıradaki paket:** V9 tek satırlık gerçek cihaz insan onayı.
- **PASS sonrası paketler:** V10-A Klaviyo Connect modalı; V10-B ayrı provider consent kararı; V10-C verified account selection; V10-D Email Monthly Plan Cost; V10-E Disconnect; V10-F ayrıca onaylı read-only Production API smoke.
