# E7 Klaviyo adapter — T1–T5 birleşik sözleşme

Bu çalışma E7'yi yeni mikro paketlere bölmeden T1–T5 sınırlarını tek canonical mapper'da dondurur.

- Platform kimliği daima `klaviyo`; channel yalnız `email|sms` olur.
- Campaign kökü `campaign_message`, Flow kökü `flow_message` leaf üretir. Parent seviye uydurulmaz.
- Entity key branch, root ve message kimliğini birlikte taşır; aynı message ID Campaign ve Flow arasında çakışmaz.
- `unique_opens` yalnız provenance bilgisidir. `ad_click` yalnız `unique_clicks` değerinden gelir; Open hiçbir koşulda Click yerine kullanılmaz.
- Journey count/value alanları provider ölçümü varsa `supported`, açıkça desteklenmiyorsa `unsupported/null`, henüz belirlenemiyorsa `unknown/null` kalır.
- T5 sınırında yalnız SMS `provider_spend` canonical spend olabilir. Eksik SMS spend ve bütün Email spend değerleri `unsupported/null` kalır; tahminî veya manuel maliyet T6 kararı verilmeden okunmaz.
- GA4/Organic bu mapper'ın girdisi değildir ve Klaviyo Campaign/Flow hierarchy'sine dağıtılmaz.

T6 estimated/manual Email spend fallback'i kullanıcıyla ayrıca kesinleştirilecektir.

## T7 kararı — GA4 Organic güvenli park

GA4 Organic attribution kullanıcı UTM kalitesine bağımlı olduğu ve yanlış/eksik UTM paid/organic sınıflandırmasını güvenilmez yaptığı için ingestion süresiz park edilmiştir. Yeni OAuth transaction, property discovery/binding, manual snapshot ve automation çalışmaz; mevcut bağlantı veya snapshot verisi silinmez. Park sabit kod politikasıdır ve environment ile yanlışlıkla açılamaz. `Paid`, `Organic` ve `Blend` analysis-scope/formula capability'leri kaldırılmaz; ileride güvenilir backend source kararıyla tekrar kullanılabilir.
