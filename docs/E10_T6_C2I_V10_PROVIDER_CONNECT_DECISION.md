# E10-T6-C2I-V10 — Provider Connect kararı

**Durum:** `Decision frozen / execution blocked`  
**Karar tarihi:** 2026-09-13  
**İlk provider:** `Klaviyo`  
**Fallback:** `Google Ads`

## Karar

Kullanıcının ürün tercihi uyarınca ilk kontrollü Provider Connect adayı **Klaviyo** olarak sabitlendi. Google Ads yalnız Klaviyo için execution-time resmi doküman doğrulaması veya hesap hazırlığı fail-closed durursa yeniden karar verilecek fallback'tir; otomatik geçiş yapılmaz.

Bu paket yalnız repository içindeki ürün ve güvenlik kararını dondurur. V9 için kimliksizleştirilmiş gerçek cihaz App Home ve Data Sources/Platforms kanıtı henüz repository'ye ulaşmadığından bu paket provider consent, token exchange, account discovery, Production API smoke, deployment veya başka bir Production mutation başlatmaz. V9 `PASS` olmadan V10'un execution alt paketlerinden hiçbiri açılamaz.

## Neden Klaviyo

Klaviyo, ortak OAuth zincirine ek olarak bağlantının tamamlanması için kullanıcı tarafından girilen sabit aylık e-posta plan maliyetini gerektirir. Bu yüzden ilk provider olarak seçilmesi, en geniş uçtan uca ürün durum makinesini erken doğrular: açıklama, OAuth, hesap doğrulama/seçim, ek maliyet alanı, kaydetme ve disconnect. Karmaşıklık gizlenmez veya Google ile geçici olarak atlanmaz.

Klaviyo nesnesi bir **Ad Account** değildir. UI ve API sözleşmesinde `Klaviyo account` denir. OAuth callback tek başına `Connected` sonucu değildir; server-side doğrulanmış tek Klaviyo account seçimi ve **Email Monthly Plan Cost** kaydı tamamlanmalıdır. Kullanıcının tarifindeki genel “Spend Amount” ifadesi mevcut ürün freeze'iyle uyumlu kesin etikete çevrilmiştir; `Spend Amount` veya `Estimated Monthly Spend` gösterilmez.

## Connect durum makinesi

1. Platforms üzerindeki `Connect` yalnız resmi Shopify `s-button` eylemidir ve doğrudan provider'a gitmez.
2. Buton resmi `s-modal` açıklamasını açar. Modal veri erişimi etkisini, Shopify'dan ayrılınacağını ve akışın henüz bağlantı kurmadığını açıklar.
3. Modal footer'ındaki tek primary `Connect` ile insan açık onay verir. `Cancel`, Escape veya modal kapatma provider teması yapmaz.
4. Doğrulanmış Shopify session token ile backend transaction başlatılır; yalnız allowlisted URL top-level OAuth navigasyonuna verilir. Nested iframe veya otomatik consent yoktur.
5. Callback state'i atomik ve tek kullanımlık tüketilir; token server-side encrypted store'a yazılır. Sonuç `Account selection required` olur, `Connected` olmaz.
6. Backend'in erişilebilirlik/ownership doğrulamasından geçen Klaviyo account listesi Shopify-native single-choice kontrolünde gösterilir. Browser serbest ID/name authority değildir.
7. Seçim kaydedilip account-selection yüzeyi kapatıldıktan sonra `Email Monthly Plan Cost` formu açılır.
8. Tutar ve currency server-side doğrulanıp açık `Save` ile kalıcılaştırılır. Double-submit loading/disabled state ile engellenir.
9. Yalnız bütün kapılar başarılıysa durum `Connected` olur. Ham provider gövdesi, token, code, credential, shop/workspace/user/account kimliği veya gizli uzunluk loga/UI'a taşınmaz.

## Disconnect durum makinesi

1. `Disconnect` resmi Shopify button ile resmi warning modalını açar.
2. `Cancel`, Escape ve modal kapatma hiçbir değişiklik yapmaz.
3. Primary destructive onay yeni refresh ve provider erişimini durdurur; UI ancak server sonucu sonrası `Not connected` olur.
4. Tarihsel analytics otomatik silinmez. Privacy deletion ayrı sözleşmedir ve Disconnect'e örtük biçimde bağlanamaz.

## Resmi kaynak revalidation kaydı

- Shopify App Home Modal v1.0, modalı confirmation/settings/data-entry amacıyla tanımlar; primary ve secondary footer action slotlarını resmi olarak sağlar: <https://shopify.dev/docs/api/app-home/latest/web-components/overlays/modal>
- Shopify App Home Button v1.0, `loading`, `disabled`, `variant`, `tone`, `href` ve top-level `target` davranışlarını tanımlar: <https://shopify.dev/docs/api/app-home/latest/web-components/actions/button>
- Klaviyo OAuth kurulumu ve Account API sözleşmeleri execution başlamadan aynı gün tekrar doğrulanacaktır: <https://developers.klaviyo.com/en/docs/set_up_oauth> ve <https://developers.klaviyo.com/en/reference/get_account>

Klaviyo doküman uçları bu çalışma ortamından 403 verdiği için scope, endpoint şekli veya account cardinality hakkında yeni bir iddia bu pakete eklenmedi. Mevcut scope/PKCE kodu canlı yürütme izni sayılmaz; V10-B öncesi resmi kaynak erişimi ve exact redirect/scope incelemesi zorunlu stop gate'tir.

## Paket sırası

- **Tamamlanan paket:** E10-T6-C2I-V10 Provider Connect Decision — Klaviyo ilk aday olarak donduruldu; execution yapılmadı.
- **Sıradaki paket:** E10-T6-C2I-V9 Real-device Shopify-native Acceptance — App Home ve Data Sources/Platforms kimliksiz ekran kanıtı.
- **Sonraki alt paketler:** V10-A Connect modal; V10-B ayrı consent kararı; V10-C verified Klaviyo account selection; V10-D Email Monthly Plan Cost; V10-E Disconnect; V10-F ayrıca onaylı read-only Production API smoke.

V9 ve her V10 execution alt paketi ayrı kanıt/izin kapısıdır. Bir önceki adımın `PASS` olması sonraki provider teması için örtük onay değildir.
