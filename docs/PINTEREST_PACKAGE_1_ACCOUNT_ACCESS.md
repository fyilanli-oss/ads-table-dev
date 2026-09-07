# Pinterest Paket 1 — OAuth, hesap seçimi ve provider sınırı

Pinterest'in eski Passive/Legacy kilidi kaldırılır; ancak bu paket performans verisini Dataset V2'ye yazmaz ve production primary aktivasyonu yapmaz.

## Bağlantı sözleşmesi

- OAuth başlangıcı authenticated JSON handshake ve ortak, tek kullanımlık transaction state üzerinden çalışır.
- Callback kullanıcı kimliğini yalnız tüketilen transaction kaydından alır.
- Token exchange ve refresh server tarafındadır; token browser/account-picker cevabına girmez.
- OAuth tamamlandıktan sonra connection tek başına `Connected` sayılmaz; explicit advertiser seçimi zorunludur.
- Scope varsayılanı `ads:read,user_accounts:read` olup environment ile açıkça değiştirilebilir.
- OAuth ayarı eksikse genel bir hata yerine yalnız eksik environment adları döner; hiçbir secret değeri veya provider cevabı gösterilmez.

## Advertiser keşfi

- `/v5/ad_accounts` cevabı yalnız ID, ad, currency ve timezone alanlarına indirgenir.
- Dört alanın tamamı bulunmayan veya duplicate advertiser seçime sunulmaz.
- Ham provider cevabı ve HTML hata sayfası kullanıcıya/evidence'a taşınmaz.
- Account selection ortak ownership, limit, schedule ve backfill hazırlık sınırını kullanır.

## Paket sınırı

Bu paket OAuth/account access hazırlığıdır. Analytics metric semantiği gerçek provider cevabıyla ayrıca karakterize edilmeden mevcut legacy normalizer production contract sayılmaz. Snapshot/Dataset V2, Time/FX, parity ve primary activation Paket 2–3 kapsamındadır.
