# E5-T6 — Google V2-primary koordinasyon ve completeness

## İş kararı

Uygulama geliştirme aşamasındadır ve mevcut V1 dashboard/dataset artık ürün hedefi değildir. Google Refresh Meta ile aynı kararı izler:

- Google performansı doğrudan Dataset V2'ye gider.
- Dataset V1 veya Dashboard V1 için Google satırı yazılmaz.
- V2 hatasında V1'e sessiz fallback yapılmaz; işlem fail-closed durur.
- V1 parity yerine provider branch → canonical accepted → Dataset V2 persisted completeness kullanılır.

## Standard/PMax kabulü

Tek Google V2-primary koordinasyonu Standard ve PMax branch'lerini ayrı çalıştırır. Her branch için `attempted == persisted` zorunludur. Standard veya PMax branch'lerinden biri hata/veri sayısı sapması üretirse tüm Google işlemi başarısızdır. Gerçek zero-row branch geçerlidir; sahte satır üretilmez.

Başarılı sonuç yalnız branch type, attempted/persisted, empty sonucu ve completeness boolean taşır. Customer/entity/token/raw metrik veya provider payload taşımaz.

## Production sınırı

Bu paket production route'a bağlanmaz ve canlı Google API/V2 işlemi çalıştırmaz. Runtime wiring, default-off gate, deployment ve canlı kabul ayrı PR ve açık production onayı gerektirir.
