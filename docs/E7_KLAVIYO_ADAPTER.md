# E7 Klaviyo adapter — birleşik sözleşme

Bu çalışma E7'yi yeni mikro paketlere bölmeden T1–T7 kararlarını tek canonical adapter sözleşmesinde toplar.

- Platform kimliği daima `klaviyo`; channel yalnız `email|sms` olur.
- Campaign kökü `campaign_message`, Flow kökü `flow_message` leaf üretir. Parent seviye uydurulmaz.
- Entity key branch, root ve message kimliğini birlikte taşır; aynı message ID Campaign ve Flow arasında çakışmaz.
- `unique_opens` yalnız provenance bilgisidir. `ad_click` yalnız `unique_clicks` değerinden gelir; Open hiçbir koşulda Click yerine kullanılmaz.
- Journey count/value alanları provider ölçümü varsa `supported`, açıkça desteklenmiyorsa `unsupported/null`, henüz belirlenemiyorsa `unknown/null` kalır.
- T5 sınırında yalnız SMS `provider_spend` canonical spend olabilir. Eksik SMS spend ve bütün Email spend değerleri `unsupported/null` kalır; tahminî veya manuel maliyet T6 kararı verilmeden okunmaz.
- GA4/Organic bu mapper'ın girdisi değildir ve Klaviyo Campaign/Flow hierarchy'sine dağıtılmaz.

## T6 kararı — usage-weighted spend

Kullanıcı onayıyla takvim gününe eşit bölme kaldırılır. SMS provider gerçek spend her zaman önceliklidir. Email aylık plan maliyeti `monthly_plan_cost × daily_sent / monthly_sent` ile günlük gönderim payına dağıtılır ve `allocated` olarak işaretlenir; açık ay `provisional`, kapanmış ay `finalized` olur. Overage yalnız kullanıcı açıkça included send ve sözleşmesel unit cost sağlarsa kümülatif kullanım farkından `estimated` hesaplanır. SMS provider spend yoksa yalnız açık kullanıcı unit cost ayarıyla estimated kullanım maliyeti hesaplanabilir. İnternetten veya global sabit birim fiyat kullanılmaz; actual ile estimate aynı gün üst üste toplanmaz.

## T7 kararı — GA4 Organic güvenli park

GA4 Organic attribution kullanıcı UTM kalitesine bağımlı olduğu ve yanlış/eksik UTM paid/organic sınıflandırmasını güvenilmez yaptığı için ingestion süresiz park edilmiştir. Yeni OAuth transaction, property discovery/binding, manual snapshot ve automation çalışmaz; mevcut bağlantı veya snapshot verisi silinmez. Park sabit kod politikasıdır ve environment ile yanlışlıkla açılamaz. `Paid`, `Organic` ve `Blend` analysis-scope/formula capability'leri kaldırılmaz; ileride güvenilir backend source kararıyla tekrar kullanılabilir.

## T8 — Time/FX, Dataset V2 ve parity

Account identity, IANA timezone ve provider business date tek normalization sınırında doğrulanır. Bütün supported monetary facts ortak FX service ile tam bir kez çevrilir; unsupported/unknown değerler `null` kalır. Campaign/Flow ve Email/SMS aynı canonical writer'dan geçer, business-date + entity key duplicate'ı reddedilir ve zero-row boş write olarak korunur. Shadow akışı legacy sonucu otoriter tutar, V2 hatasını güvenli aşama koduyla izole eder ve production activation yapmaz. Parity ancak non-empty entity set, branch/channel, metric support ve raw fact'ler birebir eşleşirse `PASS` olur.
