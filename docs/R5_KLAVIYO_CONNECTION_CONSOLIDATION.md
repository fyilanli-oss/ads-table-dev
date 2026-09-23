# R5 — Mevcut Klaviyo bağlantısının konsolidasyonu

## Analist özeti

Canlıda üç ayrı durum birlikte bulunuyor:

- Shopify embedded akışında tamamlanmış bir Klaviyo bağlantısı var.
- Eski kullanıcı bazlı yapıda iki Klaviyo satırı var.
- Yeni AdsTable workspace bağlantı tablosu henüz boş.

Eski satırlardan yalnız birinin Klaviyo hesap numarası embedded kayıtla eşleşiyor. Bu eşleşme doğru adayı bulmaya yardımcı olur; fakat tek başına o eski kullanıcının ilgili workspace'e ait olduğunu kanıtlamaz. İkinci eski satırda seçilmiş hesap numarası yoktur ve otomatik olarak hiçbir workspace'e taşınamaz.

## Durum

`R5-A complete, human binding recorded — R5-B reauthorization required — R5-C controlled clean reset preparation`.

## R5 kapıları

### R5-A — Açık kullanıcı/workspace eşlemesi

`legacy_user_workspace_bindings` yalnız insan tarafından doğrulanmış eski kullanıcı → workspace kararını saklar. `20260923132407_create_legacy_user_workspace_bindings` migration'ı canlıya uygulanmıştır. Tablo önce boş bırakılmış, ardından kullanıcının 23 Eylül 2026 tarihli açık sahiplik beyanıyla yalnız embedded account ID eşleşen legacy aday için bir aktif binding yazılmıştır. Email, mağaza domaini, Klaviyo hesap numarası veya sistemde tek workspace bulunması otomatik eşleme sebebi değildir.

### R5-B — Klaviyo tarafında salt okunur doğrulama

Embedded token yalnız server tarafında çözülür ve Klaviyo Account API'ye salt okunur istek yapılır. R5 için ayrı `verifyReadOnly` yolu hazırlanmıştır: yalnız tek `GET /api/accounts/` isteği yapar; `401` halinde token refresh yapmadan durur; token veya provider cevabı loglanmaz. Dönen hesap listesinde seçilmiş hesap ve kayıtlı source currency doğrulanmadan R5-C açılamaz.

### R5-C — Koşullu kanonik taşıma veya kontrollü temiz reset

R5-B doğrulaması geçseydi embedded bağlantının uyumlu şifreli token zarfları plaintext'e çevrilmeden `workspace_provider_connections` tablosuna alınacaktı. Canlı doğrulama `409` ile fail-closed durduğu için bu yol kullanılmayacaktır. Kullanıcının temiz başlangıç kararıyla R5-C v2 kontrollü reset yoluna dönmüştür: ayrı işlem-anı onayı sonrasında eski refresh token bir kez revoke edilir; yalnız provider başarısından sonra embedded satır `revoked` olur. Kanonik satır yaratılmaz; token zarfı, hesap/maliyet/currency, V1 veri ve snapshot geçmişi silinmez.

## Canlı salt okunur envanter — 23 Eylül 2026

- Eski Klaviyo satırı: `2`; bağlı görünen: `2`.
- Shopify embedded Klaviyo satırı: `1`; `connected`: `1`.
- Kanonik Klaviyo satırı: `0`.
- Embedded hesapla aynı non-null account ID taşıyan eski satır: `1`.
- Hesap seçimi bulunmayan eski Klaviyo satırı: `1`.
- Her iki modelde de access ve refresh token zarfları beklenen `version/keyId/iv/tag/ciphertext` biçiminde.
- Eski kullanıcıyı workspace'e bağlayan tablo veya OAuth transaction kanıtı: yok.
- Yerel ortamda production token anahtarı bulunmadığı ve güvenli no-refresh route henüz deploy edilmediği için Klaviyo Account API doğrulaması bu aşamada çalıştırılmadı.

## R5-A canlı sonucu — 23 Eylül 2026

- Migration: `20260923132407_create_legacy_user_workspace_bindings`.
- Kullanıcının açık beyanından sonra eşleme satırı: `1`; aktif human-attested eşleme: `1`; belirsiz eşleme: `0`.
- Hesap seçimi olmayan ikinci legacy Klaviyo satırı unbound bırakıldı.
- RLS ve force RLS: açık.
- `anon` ve `authenticated`: CRUD yetkisi yok.
- `service_role`: explicit CRUD yetkili.
- Kanonik bağlantı sayısı `0`, legacy Klaviyo `2`, embedded Klaviyo `1` olarak değişmeden kaldı.
- Advisor taramasında R5-A kaynaklı yeni WARN yoktur. Policiesiz RLS bilgi notu server-only deny-by-default modelinde beklenir.

## R5-B repository hazırlığı

- Session-bound `GET /api/shopify/providers/klaviyo/accounts/verify` route'u eklendi.
- Route yalnız read-only Account API çağrısı yapar; refresh ve connection write yasaktır.
- Başarılı cevap account kimliğini veya tokenı dışarı vermez; yalnız doğrulama sonucu ve currency döner.
- Geçici R5 operatör parametresi normal kullanıcı akışını değiştirmeden bu route'u çağırabilir.
- İlgili Klaviyo testleri `16/16`, R5 contract testleri `3/3` geçti.
- Bu hazırlık PR #231 ile deploy edildi; canlı sonucun ayrıntısı aşağıdaki bölümde korunur.

## R5-B canlı doğrulama sonucu ve R5-C kararı — 23 Eylül 2026

- PR #231 production deployment'ı `READY` ve `dev.adstable.app` alias'ı merge commit'e bağlıdır.
- Açık onayla tek no-refresh Account API doğrulaması çalıştırıldı; endpoint `409` döndürdü.
- Retry, refresh, revoke, token silme veya canonical connection insert yapılmadı.
- Supabase kaydı salt okunur kontrolle `connected`, aktif hesaplı, `USD` currency'li ve access/refresh token zarflı bulundu.
- Doğrulanamayan eski grant kanoniğe taşınmayacak; sonuç `reauthorization_required` olarak kaydedildi.
- `contracts/r5-klaviyo-consolidation-v2.json` kontrollü temiz reset dalını tanımlar.
- Hazırlanan reset modülü henüz route'a veya kullanıcı düğmesine bağlı değildir; canlı revoke ve yerel status mutation ayrı son onay bekler.

## Değişmeyen sınırlar

- Provider revoke yok.
- Token silme, decrypt/re-encrypt veya plaintext log yok.
- Eski ve kanonik refresh hattı aynı anda açılmaz.
- İkinci belirsiz eski Klaviyo satırı otomatik bağlanmaz.
- R6 runtime aktivasyonu ve R7 ekran akışı bu paketin dışında kalır.
