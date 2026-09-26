# R6-D4-A0 — Google Sheets park kararı

## Analist kararı

Google Sheets ilk Shopify embedded ürün diliminde bir veri kaynağı değildir. Mevcut standalone bağlantı, Dataset V1 tablosunu bağlı spreadsheet'e aktaran eski bir export utility'sidir. Workspace authority ve Dataset V2 ile yeniden tasarlanana kadar `Parked` kalır.

## Uygulama sonucu

- Yeni Google Sheets OAuth ve callback token exchange başlamaz.
- Access token yenilemesi, spreadsheet create/bind/settings, manuel sync ve auto-sync provider temasından önce fail-closed durur.
- Status yüzeyi `connected` yerine `parked` döndürür.
- Genel ve Google Sheets'e özel Disconnect rotaları parked durumda mutation yapmaz.
- Mevcut spreadsheet, legacy connection, encrypted token ve tarihsel sync metadata'sı değiştirilmez.
- Google provider revoke çağrısı yapılmaz; Google Ads veya başka bir Google scope'u etkilenmez.
- Dataset V1 veya Dataset V2 satırı okunmaz/yazılmaz.

## Canlı başlangıç envanteri

Salt-okunur aggregate kontrol `1` legacy connected Google Sheets kaydı, encrypted access/refresh envelope, bağlı spreadsheet, açık auto-sync ve geçmiş başarılı sync bulunduğunu gösterdi. Access token future-expiry kanıtı yoktur. Kimlik, token, spreadsheet adı/ID veya metrik değeri kanıta alınmadı.

## Sınır ve sonraki kapı

Bu paket bir database migration veya credential cleanup değildir. Production deployment ve post-deploy read-only doğrulama ayrı kapıdır. A0 doğrulanmadan R6-D4-A Google Ads bağlantı/token yaşam döngüsüne geçilmez. Gelecekte Google Sheets yeniden açılırsa yalnız canonical workspace Dataset V2 export tasarımıyla ele alınır.

## Production sonucu — PASS

PR #270 merge commit `1a9fa1d69d8a9552108e693cd4366d8852108a2a` Security/Full Regression PASS sonrasında production'a dağıtıldı. Vercel deployment `READY`, `dev.adstable.app` alias'ı bağlı ve bir saatlik runtime error taraması temizdir. Salt-okunur aggregate postcheck tarihsel connection, spreadsheet bağı ve encrypted access/refresh zarflarının korunduğunu; plaintext token ile Dataset V2 toplam/Google Sheets satırının sıfır kaldığını doğruladı. Provider çağrısı, revoke, database mutation veya spreadsheet write yapılmadı.
