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
- Mapper'ın tanıdığı add-to-cart, checkout ve purchase alias'ları yoksa canonical alanlar `unknown/null` kalır; bunun provider nedeni ham response evidence görülmeden açıklanamaz.
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
- Conversion support: add-to-cart 0 satır, checkout 0 satır, purchase 0 satır. Bu yalnız mapper'ın tanıdığı alias bulamadığını kanıtlar; Meta'nın ne döndürdüğü ve nedeni bu ilk kabulde kanıtlanmamıştır.
- Dataset V2 yazımı: 0.
- Desktop gerçek Shopify Admin: PASS.
- 390×844 mobil gerçek Shopify Admin: PASS.
- Production error/fatal runtime log taraması: temiz.
- Ürün sahibi görsel kabulü: 3 Ekim 2026 tarihinde ACCEPTED.

## Repository sonucu

PR #323 Security regression ve Full Regression PASS; Vercel preview PASS. Production deployment ve salt-okunur canlı Meta kabulü PASS; desktop ve 390×844 mobil Shopify Admin sonucu ürün sahibi tarafından kabul edildi. PR #323 `b5bf97be5e4b09644147c3e30ba13b1544e79c19` commit'iyle merge edilmiştir. Son production deployment `dpl_ALzPA8cZWV4WNtM9mycdtsB9y9mo` durumunda `READY`; error/fatal runtime log bulunmadı. Durum `LIVE_READ_ONLY_PASS_MERGED`dir. Dataset V2 write, schedule ve backfill kapalıdır.

## 3 Ekim 2026 corrective — provider response opacity

Önceki canlı kabul, provider'ın gerçek `actions` ve `action_values` içeriklerini göstermeden yalnız mapper support sayılarını sundu. Bu nedenle “pixel/CAPI olmadığı için dönmedi” açıklaması kanıtsızdır ve kabul geri açılmıştır.

Corrective salt-okunur çıktı, exact provider date Meta Insights cevabından provider kimliklerini çıkartarak şu alanları gösterecektir:

- `actions[].action_type`, `actions[].value` ve aynı type/value çiftinin entry count'u;
- `action_values[].action_type`, `action_values[].value` ve aynı type/value çiftinin entry count'u;
- malformed entry sayısı;
- Dataset V2 write her durumda 0.

Bu evidence görülmeden missing conversion için provider nedeni açıklanamaz, paket tekrar PASS sayılamaz ve kontrollü Dataset V2 write açılamaz. Aynı ilke Google Ads reporting completeness için de geçerlidir: test kampanyası üretilememesi, provider'ın gerçek response/field evidence'ını görmeden empty veya unsupported sonucu PASS sayma gerekçesi değildir.

## Corrective canlı provider cevabı — 3 Ekim 2026

Exact provider date `2026-10-01` için Meta Insights tekrar çalıştırıldı. Provider kimlikleri çıkarılmış canlı response evidence:

- `actions`: `landing_page_view=8`, `link_click=8`, `omni_landing_page_view=8`, `onsite_conversion.post_net_like=1`, `page_engagement=9`, `post_engagement=9`, `post_interaction_gross=1`, `post_interaction_net=1`, `post_reaction=1`.
- `action_values`: boş.
- Mapper'ın add-to-cart alias'ları: provider cevabında yok.
- Mapper'ın checkout alias'ları: provider cevabında yok.
- Mapper'ın purchase alias'ları: provider cevabında yok.
- Dataset V2 write: 0.
- Production deployment: `dpl_76JB1cvVok41ApXyrT8LidsvmQjb`, commit `7b99bb7b4917497fcc90400168e35d566be298a2`, READY.
- Production error/fatal runtime log: bulunmadı.

**Kanıtlanan:** Meta bu exact-date isteğinde add-to-cart, checkout, purchase veya bunların action_values entry'lerini döndürmedi.

**Kanıtlanmayan:** Meta'nın provider-side bunu neden döndürmediği. Pixel/CAPI, kampanya hedefi, attribution veya başka bir neden bu response tek başına ispatlamaz.
