# R6-D3-F — Meta bağımsız Disconnect yaşam döngüsü

## İş çıktısı ve iş değeri

Meta, Klaviyo ve Google Ads aynı AdsTable workspace içinde birlikte bulunabilir; fakat her bağlantı kendi yaşam döngüsüne sahiptir. Meta Disconnect yalnız Meta yetkisini ve canonical Meta bağlantı bilgisini kapatacak, diğer provider bağlantılarına veya tarihsel analizlere dokunmayacaktır.

Bu paket tamamlandığında merchant Meta'yı Shopify embedded Data Sources ekranından uyarı modalıyla güvenli biçimde ayırabilecek ve daha sonra temiz OAuth ile yeniden bağlayabilecektir.

## İnceleme sonucu

- Eski `server.js + public/dashboard.html` referansı Meta için Connected → Disconnect modalı → provider revoke → yerel cleanup akışını içerir.
- Yeni embedded yüzey Meta Connected durumunda yalnız başarı rozeti gösterir; Disconnect düğmesi/modalı yalnız Klaviyo için vardır.
- Session-bound ad-account rotalarında Meta Disconnect endpoint'i yoktur.
- Canonical store yalnız `disconnectKlaviyo` operasyonunu içerir; generic veya Meta-specific Disconnect operasyonu yoktur.
- `workspace_provider_connections` şeması `disconnected` durumu, `disconnected_at` ve optimistic `connection_version` alanlarıyla bu yaşam döngüsünü destekler. Yeni migration gerekmez.

## Plan düzeltmesi

Önceki plan Disconnect/revoke işini R7-B'ye ertelemişti. Ürün kuralı artık şöyledir:

> Her provider'ın Connect → Connected → Disconnect → temiz Reconnect yaşam döngüsü, o provider'ın R6-D canlı kabulü kapanmadan tamamlanır.

R7-B bütün provider'lar tamamlandıktan sonraki son ortak deneyim ve tutarlılık kapısı olarak kalır; eksik provider Disconnect uygulamalarının ilk teslim noktası değildir.

## Kullanıcı akışı

1. Meta kartında `Connected · 1 account` ve **Disconnect** görünür.
2. Disconnect, Shopify-native kritik uyarı modalını açar.
3. **Cancel** hiçbir provider çağrısı veya yerel değişiklik yapmaz.
4. Açık Disconnect onayı yalnız doğrulanmış Shopify session'daki workspace Meta bağlantısını hedefler.
5. AdsTable, bağlı user access token ile eski çalışan referanstaki Meta revoke davranışını uygular; app access token kullanmaz.
6. Provider revoke başarısızsa canonical bağlantı değişmeden `Connected` kalır.
7. Provider başarısından sonra canonical Meta tokenları ve seçilmiş hesapları temizlenir, durum `disconnected` olur.
8. Kart `Not connected / Connect` durumuna döner.
9. Klaviyo, gelecekteki Google Ads, reporting currency ve tarihsel analizler korunur.
10. Temiz reconnect OAuth → 1–3 verified hesap → Connected zincirini yeniden tamamlar.

## Canonical veri sonucu

`workspace_provider_connections` içindeki aynı Meta satırı korunur. `status=disconnected`, `disconnected_at` ve artırılmış `connection_version` yazılır. Access/refresh token zarfları ve expiry alanları, granted scope'lar, aktif/seçilmiş hesaplar, source currency ve account verification bilgisi temizlenir.

Workspace/provider kimliği, oluşturulma tarihi, önceki bağlantı zamanı ve authorization adapter kaydı audit geçmişi olarak korunur. Dataset V1/V2 satırları, legacy Meta geçmişi, workspace reporting currency ve diğer provider bağlantıları değiştirilmez.

## Güvenlik ve hata davranışı

- Workspace yalnız doğrulanmış Shopify session'dan server-side çözülür.
- Browser workspace, token, hesap adı/currency veya connection version otoritesi değildir.
- İşlem exact action-time confirmation ister.
- Optimistic `connection_version` eski bir isteğin yeni reconnect'i temizlemesini engeller.
- Provider revoke yerel cleanup'tan önce gelir; revoke hatası fail-closed sonuç verir.
- Token, hesap ID'si ve ham provider cevabı browser response'una veya kanıta çıkmaz.
- Disconnect schedule/backfill başlatmaz ve Dataset yazısı üretmez.

## Canlı kabul kriterleri

1. Başlangıç Meta durumu `Connected · 1 account` olarak doğrulanır.
2. Modal açılır; Cancel sonrası UI ve Supabase bağlantısı değişmez.
3. Modal yeniden açılır ve Disconnect onaylanır.
4. UI `Not connected / Connect` olur.
5. Salt-okunur Supabase postcheck Meta token/hesap alanlarının temizlendiğini ve `disconnected_at` alanının dolduğunu doğrular.
6. Dataset sayıları, reporting currency, legacy Meta geçmişi ve Klaviyo bağlantısı değişmez.
7. Temiz Meta reconnect tamamlanır ve `Connected · 1 account` yeniden doğrulanır.

## Bu karar paketinde yapılmayanlar

- Uygulama kodu yazılmaz.
- Provider revoke çağrısı yapılmaz.
- Supabase mutation veya migration yapılmaz.
- Canlı Meta bağlantısı kesilmez.
- Google Ads paketi başlatılmaz.

## Durum

`PASS contract only — implementation gate`
