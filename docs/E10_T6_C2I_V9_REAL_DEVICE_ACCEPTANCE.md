# E10-T6-C2I-V9 — Real-device Shopify-native Acceptance

**Durum:** `FAIL — SHOPIFY_INSTALL_PERSISTENCE_FAILED`
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

13 Eylül 2026 gerçek cihaz sonucu uygulamanın Shopify Admin içinde açıldığını, ancak App Home yerine `SHOPIFY_INSTALL_PERSISTENCE_FAILED` gösterdiğini doğruladı. Referans kimliği ve ekran görüntüsü repository'ye alınmadı. V9 `PASS` değildir.

Bir sonraki çalışma yalnız bu persistence hatasını düzeltir. Bu sonuç OAuth, provider consent, callback, account discovery, provider API isteği veya Production mutation yetkisi vermez.

Shopify-native değerlendirmesinin resmi kaynakları [App Home](https://shopify.dev/docs/api/app-home), [`s-app-nav`](https://shopify.dev/docs/api/app-home/latest/app-bridge-web-components/app-nav), [Page](https://shopify.dev/docs/api/app-home/latest/web-components/structure/page) ve [Button](https://shopify.dev/docs/api/app-home/latest/web-components/actions/button) belgeleridir.

## Paket sırası

- **Tamamlanan paket:** V9 gerçek cihaz koşusu; sonuç `FAIL — SHOPIFY_INSTALL_PERSISTENCE_FAILED`.
- **Sıradaki paket:** Yalnız Shopify install persistence düzeltmesi.
- **PASS sonrası paketler:** V10-A Klaviyo Connect modalı; V10-B ayrı provider consent kararı; V10-C verified account selection; V10-D Email Monthly Plan Cost; V10-E Disconnect; V10-F ayrıca onaylı read-only Production API smoke.
