# E10-T5-C5-A — Attribution Differences Shopify ürün freeze'i

**Durum:** `Done — kullanıcı öneriyi onayladı; UI/runtime implementasyonu yapılmadı`  
**Karar tarihi:** 2026-09-09  
**Production etkisi:** Yok

## Ürün adı ve iddia sınırı

İlk dilimin kullanıcıya görünen adı **Attribution Differences**dır. Platform-level provider ve Shopify-attributed aggregate değerleri arasındaki fark, tek başına aynı satışın iki kez sayıldığına veya belirli bir Campaign/Ad Set/Ad'in hatalı olduğuna kanıt değildir. Bu nedenle `Overlap`, `Duplicate`, `Fix overlap`, `Decrease overlap`, `Correct sales` ve `Deduplicate` ilk dilimde kullanıcı eylemi veya kesin sonuç etiketi olamaz.

Repository'deki mevcut `overlap` isimleri T5-B intake'in geçici/internal adlarıdır; kullanıcıya görünen semantik değildir. Runtime isim değişikliği bu freeze'in kapsamı değildir.

## Ayrı ekran ve context

Attribution Differences, Dashboard ve Funnel'dan ayrı Shopify embedded sayfasıdır. Dashboard/Funnel'daki completed-day time range, workspace currency, platform ve Settings'te seçilmiş tek aktif reklam hesabı context'ini korur. Serbest compare dönemi veya comparison modu yoktur.

Provider tarafı seçili aktif reklam hesabının aggregate sonucudur; Shopify tarafı platform attribution aggregate sonucudur. Bu iki grain bire bir aynı olmayabilir. Ekran bu kapsam farkını açıklar; eşit sonuç event-level eşleşme, farklı sonuç duplicate veya veri kaybı kanıtı sayılmaz.

## Ana tablo

| Kolon | Sözleşme |
|---|---|
| Platform | Meta, Google veya TikTok; Klaviyo yalnız Shopify tarafı güvenilir eşleşiyorsa karşılaştırılır |
| Provider Purchase | Aktif provider account için provider-reported aggregate |
| Shopify-attributed Purchase | Shopify'ın aynı tamamlanmış dönem için platform aggregate'i |
| Purchase Difference | Backend `provider - shopify_attributed`; iki taraf karşılaştırılabilir değilse `—` |
| Provider Sales | Aktif provider account için provider-reported Sales |
| Shopify-attributed Sales | Shopify platform-attributed Sales |
| Sales Difference | Backend `provider - shopify_attributed`; currency eşit/normalize değilse `—` |
| Data Status | Allowlisted nötr availability durumu |
| Action | Yalnız `Review` |

Compare kontrolü, percent change, ranking ve performans yönü yoktur. Pozitif/negatif/sıfır fark iyi-kötü olarak yeşil/kırmızı renklendirilmez. Sıfır yalnız `No numeric difference`, fark ise `Difference found` demektir.

## Review modalı

Her satırın `Review` eylemi Shopify-native modal açar. Modal yalnız platform, seçili completed-day dönem, aktif account display label, iki provenance ailesinin Purchase/Sales aggregate'leri, signed difference, currency comparability, freshness ve data status gösterir. Account display label yetki üretmez; ham identity gösterilmez.

Zorunlu disclosure'ın iş anlamı: **Shopify ve reklam platformu satın almaları bağımsız biçimde ilişkilendirir; bu fark belirli bir Campaign veya Ad'in aynı satışı iki kez saydığını kanıtlamaz ve Funnel toplamlarını değiştirmez.**

İzinli modal eylemleri `Close` ve aynı context ile `View Funnel`dır. Apply/decrease/fix/deduplicate, Ad seçimi, manuel dağıtım veya adjustment yoktur.

## Data Status

İzinli durumlar:

- `No numeric difference`
- `Difference found`
- `Partial data`
- `Shopify attribution unavailable`
- `Provider data unavailable`
- `Currency mismatch`
- `Unsupported`
- `Stale`
- `Reauthorization required`

Ham Shopify/provider error, query, row, order/customer identity, click ID, OAuth/token veya credential kullanıcıya/loga taşınmaz.

## KPI, veri ve güvenlik sınırı

C5-A:

- Dataset V2 provider fact'lerini değiştirmez;
- Funnel, Dashboard, Revenue veya provider hierarchy toplamını değiştirmez;
- platform farkını Campaign/Ad Set/Ad'e dağıtmaz;
- kullanıcıya hangi Ad'den azaltma yapılacağını seçtirmez;
- Shopify/provider değerlerini toplamaz, kazanan kaynak seçmez veya reconciled truth üretmez;
- yeni order/customer/click-ID intake'i, scope, migration, storage veya production query açmaz.

## C5-B — Verified Reconciliation capability

C5-B `Deferred`dır. Ancak aynı Shopify order için privacy-safe stable evidence, provider click identity, server-side provider/account/Campaign/Ad Set-or-Ad Group/Ad çözümlemesi, exact leaf identity, attribution-window/time/currency semantiği, scope/protected-data, retention/deletion, idempotency ve rollback birlikte kanıtlanırsa ayrı ürün/veri kararıyla açılabilir.

Doğrulansa bile provider-reported fact overwrite edilmez. Adjustment ayrı provenance'li, versionlı ve geri alınabilir sidecar olur; yalnız kanıtlanan leaf Ad'e bağlanır ve parent roll-up backend'de yeniden hesaplanır. Platform farkını en büyük Ad'e, oransal veya kullanıcı seçimiyle dağıtmak yasaktır.

## Shopify-native component sınırı

Sayfa/layout, disclosure banner, nötr status badge, table, modal ve action kontrolleri implementation anındaki güncel resmi Shopify App Home/Polaris web componentleriyle kurulur; navigation App Bridge kullanır. Özel AdsTable control framework veya Shopify Admin shell taklidi yasaktır. Exact component/property, accessible table/modal, focus return ve responsive davranış E10-T6-A'da resmi kaynaklarla yeniden doğrulanır.

## Sıra

E10-T5-C5-A `Done`; C5-B `Deferred`. İlk Attribution ürünü C5-A ile kapanır. Sıradaki ürün paketi **E10-T5-C7 Integrated navigation/acceptance**dır; parent T5-C, C7 onaylanmadan `Done` olmaz.
