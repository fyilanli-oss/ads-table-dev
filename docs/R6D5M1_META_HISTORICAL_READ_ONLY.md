# R6-D5-M1 — Meta tarihsel salt-okunur reporting completeness

## Analist brief'i

- **Paket / Execution Plan maddesi:** R6-D5-M1; R6 provider reporting completeness.
- **Kullanıcı amacı:** Mevcut Instagram → Amazon test kampanyasının gerçek performans gününü bulmak ve Meta verisini Campaign → Ad Set → Ad seviyesinde eksiksiz doğrulamak.
- **Başlangıç durumu:** Mevcut R6-D3-D yalnız hesabın önceki kapanmış gününü sorguluyor. 3 Ekim 2026 canlı tekrarında bağlantı, Account API ve Time/FX PASS; dünkü Insights sonucu 0 satır ve Dataset V2 yazımı 0 oldu.
- **Başarılı sonuç:** Server-side seçilmiş 1–3 hesabın her biri için son 31 kapanmış business date içinde provider'ın döndürdüğü en yakın Ad Insight günü bulunur. Campaign, Ad Set ve Ad sayıları ile impressions, `actions.link_click`, spend ve conversion support aggregate gösterilir.
- **Değişmeyecek davranış:** Canonical Shopify workspace authority, Meta connection/token lifecycle, E4 mapper, Time/FX, Dataset schema ve writer değişmez.
- **Kapsam dışı:** Dataset V2 yazımı, schedule, backfill, production activation, OAuth/reconnect, sentetik satır ve pixel olmayan conversion alanlarını 0'a çevirme.

## Neden yeni kapı gerekli?

Mevcut kabulün verified-empty sonucu bağlantı arızası değildir; yalnız dünkü tarihte provider satırı bulunmadığını kanıtlar. Meta'nın güncel Insights yüzeyi `level=ad`, explicit `time_range`, günlük `time_increment=1`, Campaign/Ad Set/Ad kimlikleri, impressions, actions, action_values ve spend alanlarını destekler. Bu nedenle doğru teşhis, test kampanyasının gerçek gününü bounded tarihsel taramayla bulmaktır.

## Durum matrisi

| Durum | Kullanıcının gördüğü metin | Eylem | Beklenen sonuç |
|---|---|---|---|
| Loading | Finding the most recent Meta performance date without writing Dataset V2… | Yok | Düğme loading; ikinci istek yok |
| Empty | PASS — no provider rows were found in the bounded historical window. Dataset V2 writes: 0. | Yeniden otomatik çalışma yok | Verified-empty |
| Success | PASS — tarih aralığı, Campaign/Ad Set/Ad, impressions, link clicks, spend ve conversion support | Yok | Aggregate salt-okunur kanıt |
| Error / Reauthorization | Güvenli `META_*` kodu | Gerekirse ayrı reconnect kararı | Dataset yazımı yok |
| Cancel | Bu yüzey modal/form açmaz | Yok | Uygulanmaz |

## Exact Shopify component mapping

| Görünür öğe | Exact component | Property | Resmî kaynak | Kontrol |
|---|---|---|---|---|
| Acceptance bölümü | `s-section` | `heading="Meta historical performance inventory"` | https://shopify.dev/docs/api/app-home/latest/web-components/layout-and-structure/section | 2026-10-03 |
| Dikey akış | `s-stack` | `gap="base"` | https://shopify.dev/docs/api/app-home/latest/web-components/layout-and-structure/stack | 2026-10-03 |
| Açıklama/sonuç | `s-paragraph` | sonuçta `aria-live="polite"` | https://shopify.dev/docs/api/app-home/latest/web-components/typography-and-content/text | 2026-10-03 |
| Salt-okunur eylem | `s-button` | `variant="secondary"`, runtime `loading` | https://shopify.dev/docs/api/app-home/latest/web-components/actions/button | 2026-10-03 |

Raw HTML kontrolü, `s-clickable`, inline CSS, literal renk veya özel component yoktur. Desktop ve mobil aynı section/stack yapısını Shopify'ın doğal responsive davranışıyla kullanır.

## Provider metrik kararı

- Grain: `level=ad`; hiyerarşi Campaign → Ad Set → Ad.
- Gün: `time_increment=1`; üst sınır her hesabın timezone'unda önceki kapanmış gündür.
- Pencere: 31 gün; browser tarih veya hesap sağlayamaz.
- Canonical `ad_click`: yalnız `actions.action_type=link_click`. Meta `clicks` evidence-only kalır.
- Pixel/CAPI ile gözlenemeyen add-to-cart, checkout ve purchase alanları `unknown/null` kalır; sahte 0 üretilmez.
- Birden fazla source currency varsa spend source değerleri toplanmaz; canonical FX sonrası merchant reporting currency aggregate edilir.

## Kabul planı

- Repository focused/full/security testleri PASS.
- Constitution guard ve UI contract PASS.
- Production deployment sonrası salt-okunur gerçek Meta sonucu.
- Desktop gerçek Shopify Admin PASS.
- En az 320 px gerçek mobil Shopify Admin PASS.
- Empty, success, error ve loading durumları kanıtlanır.
- Kullanıcı görsel sonucu açıkça kabul eder.
- Yalnız non-empty read-only PASS sonrasında, ayrı açık işlem-anı onayıyla tek kontrollü Dataset V2 write paketi hazırlanabilir.

Bu belge veya implementation kendi başına Dataset V2 yazımı ya da production schedule izni vermez.

## Canlı kabul kanıtı

- Production deployment: `dpl_779vYvFQXQrApvuaNxsxfxkMMgHq`; `dev.adstable.app`; commit `1a9d492c94eb4f2b19ca1a474eae831eb8badf5c`.
- Meta provider tarihi: `2026-10-01`.
- Hiyerarşi: 1 Campaign → 1 Ad Set → 1 Ad.
- Sonuç: 181 impressions, 8 canonical link clicks, 117.22 TRY spend.
- Conversion support: add-to-cart 0 satır, checkout 0 satır, purchase 0 satır. Pixel/CAPI gözlemi bulunmadığından bunlar dönüşüm değeri 0 değil; `unknown/null` semantiğidir.
- Dataset V2 yazımı: 0.
- Desktop gerçek Shopify Admin: PASS.
- 390×844 mobil gerçek Shopify Admin: PASS.
- Production error/fatal runtime log taraması: temiz.
- Ürün sahibi görsel kabulü: 3 Ekim 2026 tarihinde ACCEPTED.

## Repository sonucu

PR #323 Security regression ve Full Regression PASS; Vercel preview PASS. Production deployment ve salt-okunur canlı Meta kabulü PASS; desktop ve 390×844 mobil Shopify Admin sonucu ürün sahibi tarafından kabul edildi. Durum `LIVE_READ_ONLY_PASS_PRODUCT_OWNER_ACCEPTED_MERGE_PENDING`dir. PR henüz merge edilmemiştir. Dataset V2 write, schedule ve backfill kapalıdır.
