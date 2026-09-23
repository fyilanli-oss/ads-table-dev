# R5 — Mevcut Klaviyo bağlantısının konsolidasyonu

## Analist özeti

Canlıda üç ayrı durum birlikte bulunuyor:

- Shopify embedded akışında tamamlanmış bir Klaviyo bağlantısı var.
- Eski kullanıcı bazlı yapıda iki Klaviyo satırı var.
- Yeni AdsTable workspace bağlantı tablosu henüz boş.

Eski satırlardan yalnız birinin Klaviyo hesap numarası embedded kayıtla eşleşiyor. Bu eşleşme doğru adayı bulmaya yardımcı olur; fakat tek başına o eski kullanıcının ilgili workspace'e ait olduğunu kanıtlamaz. İkinci eski satırda seçilmiş hesap numarası yoktur ve otomatik olarak hiçbir workspace'e taşınamaz.

## Durum

`R5-A complete, human binding recorded — R5-B live provider verification deployment pending`.

## R5 kapıları

### R5-A — Açık kullanıcı/workspace eşlemesi

`legacy_user_workspace_bindings` yalnız insan tarafından doğrulanmış eski kullanıcı → workspace kararını saklar. `20260923132407_create_legacy_user_workspace_bindings` migration'ı canlıya uygulanmıştır. Tablo önce boş bırakılmış, ardından kullanıcının 23 Eylül 2026 tarihli açık sahiplik beyanıyla yalnız embedded account ID eşleşen legacy aday için bir aktif binding yazılmıştır. Email, mağaza domaini, Klaviyo hesap numarası veya sistemde tek workspace bulunması otomatik eşleme sebebi değildir.

### R5-B — Klaviyo tarafında salt okunur doğrulama

Embedded token yalnız server tarafında çözülür ve Klaviyo Account API'ye salt okunur istek yapılır. R5 için ayrı `verifyReadOnly` yolu hazırlanmıştır: yalnız tek `GET /api/accounts/` isteği yapar; `401` halinde token refresh yapmadan durur; token veya provider cevabı loglanmaz. Dönen hesap listesinde seçilmiş hesap ve kayıtlı source currency doğrulanmadan R5-C açılamaz.

### R5-C — Kanonik taşıma ve eski hattı yerelde kapatma

R5-A ve R5-B birlikte geçerse embedded bağlantının uyumlu şifreli token zarfları plaintext'e çevrilmeden `workspace_provider_connections` tablosuna alınır. Kanonik satır doğrulanmış `connected` olur. Yalnız eşlenmiş eski bağlantı yerel olarak `migrated/disabled` durumuna alınır. Klaviyo revoke çağrılmaz; eski token zarfı, V1 veri ve snapshot geçmişi silinmez.

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
- Kod henüz deploy edilmedi; provider teması henüz gerçekleşmedi.

## Değişmeyen sınırlar

- Provider revoke yok.
- Token silme, decrypt/re-encrypt veya plaintext log yok.
- Eski ve kanonik refresh hattı aynı anda açılmaz.
- İkinci belirsiz eski Klaviyo satırı otomatik bağlanmaz.
- R6 runtime aktivasyonu ve R7 ekran akışı bu paketin dışında kalır.
