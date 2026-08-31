# E5-T2 — Google customer currency ve timezone

## İş çıktısı

Google rapor günü sunucu, tarayıcı veya yönetici saat diliminden üretilmez. Seçili Google Ads customer için aynı provider sorgusundan alınan `customer.currency_code` ve `customer.time_zone` zorunludur.

Sorgu yalnız şu metadata alanlarını ister:

```sql
SELECT customer.id, customer.currency_code, customer.time_zone FROM customer LIMIT 1
```

Dönen customer kimliği istenen customer ile bire bir uyuşmalıdır. Currency üç harfli ISO kodu, timezone geçerli IANA bölgesi olmalıdır. Eksik veya farklı metadata UTC/default fallback ile devam etmez.

## Evidence

Operasyon kanıtı customer ID, isim veya ham provider cevabı taşımaz. Yalnız identity doğrulamasının sonucu, currency/timezone varlığı, hesap business date'i ve time engine sürümünü taşır.

## Production sınırı

Bu paket production Google API çağrısı, OAuth değişikliği, Dataset V2 yazımı veya runtime delegation yapmaz.
