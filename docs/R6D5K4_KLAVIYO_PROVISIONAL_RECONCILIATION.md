# R6-D5-K4 — Klaviyo provisional reconciliation corrective

## Analist brief

### Sorun

AdsTable, Klaviyo Campaign ve Flow raporunu provider kuralı olmayan sabit `now-48h` kapısıyla iki gün gizliyordu. Provider arayüzünde görünen teslimat, açılma, tıklama ve conversion verisi AdsTable tanısında görünmüyordu.

### Kullanıcı sonucu

- Bugün ve dün gönderilen Campaign/Flow verisi beklemeden okunur.
- Attribution penceresi içindeki tarih açıkça `PROVISIONAL` görünür.
- Provisional gün saatte bir yeniden okunur ve canonical anahtarda idempotent uzlaştırılır.
- Tarih yalnız configured attribution penceresi kapandıktan sonra `FINALIZED` olur.
- Gelecek tarih reddedilir; sentetik değer oluşturulmaz.

### Finality sözleşmesi

- Mevcut doğrulanmış Klaviyo hesabı için attribution penceresi: 5 gün.
- Beş gün Klaviyo'nun evrensel sabiti değildir; workspace/account ayarı olarak değiştirilebilir.
- Tarih seviyesinde güvenli kapanış kullanılır: business date + pencere tamamen geçmeden finalization yapılmaz.
- Public Reporting API'de account attribution-window ayarını okuyan doğrulanmış endpoint bulunmadığı için production aktivasyonunda canonical ayar zorunludur.
- Bu paket production SnapshotJob schedule'ını açmaz; E9 execution kapısına sözleşme bırakır.

## Shopify embedded UI exact eşlemesi

| Kullanıcı ihtiyacı | Shopify bileşeni | Exact davranış |
|---|---|---|
| Tarih seçmek | `s-date-field` | `allow="--YYYY-MM-DD"`; üst sınır bugün |
| Salt-okunur kontrolü başlatmak | `s-button variant="primary"` | Mevcut read-only endpoint |
| Sonucu ve finality'yi göstermek | `s-paragraph aria-live="polite"` | Tarihle birlikte `PROVISIONAL` veya `FINALIZED` |
| Bölüm yapısı | `s-section` + `s-stack` | Mevcut gizli acceptance yüzeyi |

Raw HTML kontrolü, inline CSS, özel button/card/badge/modal taklidi, literal renk veya Shopify dışı görsel katman eklenmez.

## Durum ve metinler

- Loading: `Reading Campaign and Flow performance for <date> without writing Dataset V2…`
- Success: `PASS — <date> (PROVISIONAL|FINALIZED)… Dataset V2 writes: 0.`
- Gelecek tarih: `KLAVIYO_PROVIDER_DATE_IN_FUTURE`
- Invalid/out-of-range tarih: mevcut güvenli hata kodları.

## Değişmeyen sınırlar

OAuth, token lifecycle, metric binding, account selection, Dataset V2 şeması, FX motoru, production schedule ve automatic retry değişmez. Kontrollü Dataset write ayrı açık onay kapısında kalır.

## Kabul kapısı

Repository testleri tek başına görsel kabul değildir. Desktop ve gerçek mobil Shopify Admin'de bugün tarihinin seçilebilir olduğu, provisional etiketinin doğru göründüğü ve 48 saat bekleme olmadığı kullanıcı tarafından açıkça kabul edilmeden paket `Done`, `PASS` veya merge sayılamaz.

## Resmî kaynak doğrulaması

Kontrol tarihi: 3 Ekim 2026.

- Klaviyo Reporting API Campaign/Flow raporlarını Klaviyo UI ile eşleşen metrikler olarak sağlar.
- Klaviyo message attribution çoğunlukla birkaç saat içinde işlenir; geç etkileşimler attribution penceresi boyunca sonucu güncelleyebilir.
- Shopify `s-date-field` inclusive üst sınırı `allow` property ile destekler.
## 3 Ekim 2026 — acceptance ve merge sonucu

- PR #321, commit `d1974cebe0683eeb86b58bf674cf534940650918` ile merge edildi.
- Gerçek Shopify Admin desktop ve 390×844 mobil viewport kabulü PASS oldu; kullanıcı görsel sonucu açıkça onayladı.
- `2026-10-03` tarihi beklemeden okundu ve `PROVISIONAL` gösterildi; Dataset V2 yazımı `0` kaldı.
- Ardından `2026-10-02` Sent Campaign/Flow verisi beklemeden bulundu ve K3 salt-okunur/kontrollü kabuline girdi.
- `dev.adstable.app` yeni main deployment'ında READY ve endpoint HTTP `200` doğrulandı.
- Sabit 48 saat kapısı kaldırılmıştır; K4 durumu `PASS_SHOPIFY_ACCEPTANCE_AND_MERGE`dir.
