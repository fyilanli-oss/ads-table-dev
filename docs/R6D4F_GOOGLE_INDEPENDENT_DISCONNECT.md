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

## Gerçekleşen production kabulü — PASS

Merchant akışı canlıda tamamlandı: `Connected · 3 accounts` durumunda açılan uyarı modalındaki `Cancel` bağlantıyı değiştirmedi; açık Disconnect sonrasında `Not connected` görüldü; temiz OAuth ve provider-doğrulanmış üç hesap seçimi sonrasında yeniden `Connected · 3 accounts` görüldü.

Son salt-okunur Supabase doğrulaması Google Ads canonical bağlantısını `connected`, connection version'ı `13`, access/refresh envelope ve expiry alanlarını mevcut, seçilmiş hesap sayısını `3` olarak doğruladı. Meta, Klaviyo ve reporting currency korundu. Migration, Dataset V2 yazımı, schedule veya backfill aktivasyonu yapılmadı.

Daha sonra ayrı açık onayla yürütülen R6-D4-G, legacy Google Sheets/GA4 erişimini emekli edip gerçek provider grant revoke ve temiz Google Ads reconnect gerçekleştirdi. Bu sonraki işlem, R6-D4-F'te kanıtlanan bağımsız Cancel/Disconnect/Reconnect davranışını geçersiz kılmaz.

Kanıt: `docs/security/evidence/R6D4F_GOOGLE_DISCONNECT_RECONNECT_LIVE_2026-09-26.json`.
