# R6-D4-B — Google Ads token ve manager-context uygulaması

## Analist özeti

Bu paket Google Ads bağlantısının yalnız ilk OAuth anında değil, aylar boyunca güvenli çalışabilmesi için gerekli server-side token yaşam döngüsünü kurar. Ayrıca manager hesabı üzerinden erişilen reklam hesabının sonraki API çağrılarında kaybolmaması için her seçilmiş hesabın doğrulanmış `login_customer_id` bağlamını canonical bağlantıda korur.

## Yapılanlar

1. Google OAuth token cevabında access token yanında refresh token, pozitif expiry ve `adwords` scope zorunlu hale getirildi.
2. Eksik refresh token, expiry veya scope canonical connection yazılmadan fail-closed durur.
3. Pending account selection ve connected durumlarında access token süresi dolmuş veya dolmak üzereyse server Google token endpoint'inden yeniler.
4. Provider refresh token rotation döndürürse yeni refresh token encrypted canonical store'a optimistic connection version ile yazılır; dönmezse mevcut refresh token korunur.
5. Beklenmeyen provider auth hatasında en fazla bir refresh ve bir retry yapılır. İkinci auth hatası veya `invalid_grant`, `GOOGLE_REAUTHORIZE` üretir.
6. Google Ads discovery sonucu artık `loginCustomerId` taşır. Save sırasında server provider listesini yeniden okur; seçilmiş non-manager hesabı `id`, `name`, `currency`, `login_customer_id` alanlarıyla canonical `selected_accounts` JSONB envelope'una yazar.
7. Manager hesaplar seçilebilir hesap listesine girmez. Bir hesap doğrulanmış manager context taşımıyorsa `Connected` olamaz.
8. Refresh sırasında stale connection version yeni bağlantının üzerine yazamaz.

## Neden migration yok?

`workspace_provider_connections.selected_accounts` mevcut server-only JSONB alanıdır. Yeni `login_customer_id` aynı doğrulanmış hesap envelope'una eklenir; tablo cardinality constraint'i, RLS, browser grant veya kolon yapısı değişmez. Bu nedenle additive database migration gerekmemektedir.

## Değişmeyen alanlar

- E5 Standard/PMax adapter, metric/conversion mapping, Time/FX ve Dataset V2 writer.
- Workspace reporting currency kararı.
- Google Sheets ve GA4/Organic park durumu.
- Legacy Google bağlantıları ve tarihsel kayıtlar.
- Schedule/backfill ve production activation.
- Disconnect; R6-D4-F kapısında ele alınacaktır.

## Kabul kriterleri

- Refresh token, expiry veya `adwords` scope eksik OAuth sonucu canonical store'a ulaşmaz.
- Token ve refresh token browser'a veya loga çıkmaz.
- Expiry öncesi server refresh çalışır; rotation güvenli biçimde kaydedilir.
- Bir auth hatası yalnız bir refresh + bir retry üretir; tekrarında reconnect gerekir.
- Seçilen 1–3 hesabın her biri doğrulanmış `login_customer_id` taşır.
- Account save, refresh sonrasında değişen connection version'ı kullanır.
- Database migration, provider teması, production OAuth ve Dataset V2 yazımı yoktur.

## Sonraki kapı

R6-D4-C merchant acceptance: deploy sonrasında gerçek Shopify session ile `Connect → Google OAuth → 1–3 verified account → Save → Connected → reload` zinciri ayrı açık onay ve insan gözlemiyle doğrulanacaktır.

## Durum

`Repository PASS — R6-D4-C merchant acceptance gate`

