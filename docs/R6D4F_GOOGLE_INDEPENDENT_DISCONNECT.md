# R6-D4-F — Google Ads bağımsız Disconnect ve temiz Reconnect

## Analist sonucu

Bu paket Google Ads bağlantı yaşam döngüsündeki son eksik davranışı tamamlar. Bağlı Google Ads kartı `Disconnect` eylemi gösterir. Eylem Shopify-native bir uyarı modalı açar; `Cancel` bağlantıyı değiştirmez. Açık onay Google Ads'i bu AdsTable workspace'inden ayırır ve kartı `Not connected` durumuna getirir.

Disconnect, Google hesabının genel yetkisini provider tarafında iptal etmez. Eski Google Ads, Google Sheets ve GA4 akışları aynı Google OAuth client'ını kullandığı için global revoke başka bir Google bağlantısını da bozabilir. Bu nedenle işlem yalnız canonical `google_ads` kaydındaki yerel token envelope'larını, süreleri, scope'ları, seçilmiş hesapları ve doğrulanmış hesap metadata'sını temizler.

## Korunan iş verileri

- Meta ve Klaviyo bağlantıları değişmez.
- Workspace reporting currency değişmez.
- Dataset V2 ve legacy tarihsel analytics satırları silinmez.
- Park edilmiş Google Sheets ve GA4 durumu değişmez.
- Schedule, backfill veya yeni veri yazımı açılmaz.

## Supabase davranışı

Yeni migration gerekmez. Güncelleme yalnız doğrulanmış `workspace_id`, `provider = google_ads`, `status = connected` ve mevcut `connection_version` eşleşmesinde çalışır. Başarılı işlem sürümü bir artırır ve `disconnected_at` yazar. Eşzamanlı başka bir bağlantı değişikliği varsa işlem `CONNECTION_CHANGED` ile fail-closed durur.

## Canlı kabul

Repository PASS tek başına paketi kapatmaz. Canlı kabul sırası şöyledir:

1. `Connected · 1–3 accounts` durumunda Disconnect modalı açılır.
2. `Cancel` sonrası bağlı durum korunur.
3. Açık Disconnect onayı sonrası `Not connected` görülür.
4. Salt-okunur Supabase postcheck Google Ads yerel credential/hesap temizliğini ve diğer provider/veri alanlarının korunduğunu doğrular.
5. Temiz OAuth, provider-doğrulanmış 1–3 hesap seçimi ve Save sonrasında yeniden `Connected` görülür.
6. Son salt-okunur postcheck canonical reconnect'i doğrular.

Bu gözlemler ve postcheck'ler tamamlanmadan R6-D4-F production PASS sayılmaz.
