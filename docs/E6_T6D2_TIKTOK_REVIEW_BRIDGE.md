# E6-T6D2 — TikTok production review bridge

## Doğrulanan hata

Production OAuth advertiser discovery başarılı bir token üretiyor ancak bağlı production TikTok kimliği sandbox advertiser'ı listeleyemediği için account picker boş kalıyor. Non-production sandbox değişkenleri production startup tarafından bilinçli olarak reddedildiğinden Preview ayarları `dev.adstable.app` production deployment'ına uygulanmıyor.

## Geçici ve açıkça sınırlandırılmış çözüm

`TIKTOK_REVIEW_FALLBACK_ENABLED=true` yalnız `TIKTOK_REVIEW_ADVERTISER_ID` ve server-only `TIKTOK_REVIEW_ACCESS_TOKEN` birlikte mevcutsa çalışır. OAuth listesi boş olduğunda picker bu tek review advertiser'ı gösterir. Seçim, `reportBase=sandbox` ve `tokenSource=server_review_access_token` metadata'sını kaydeder; sonraki TikTok refresh tokenı browser'a göndermeden sandbox report host'una gider.

Genel production kullanıcıları için sandbox fallback açılmaz. Eski `TIKTOK_SANDBOX_*` değerleri Production environment'ta yanlışlıkla kalsa bile runtime bunları karantinaya alır ve sandbox özelliklerini kapalı tutar. Review flag/token/advertiser üçlüsü eksikse yalnız review bridge fail-closed biçimde devre dışı kalır; optional TikTok konfigürasyonu login/public-config dahil uygulamanın geri kalanını durduramaz.

Bu köprü TikTok primary aktivasyonu değildir. Legacy snapshot otoritesi ve `TIKTOK_V2_SHADOW_ENABLED` kapısı değişmeden kalır.
