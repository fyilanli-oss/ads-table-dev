# E6-T6C2 — TikTok shadow dual-write no-change composition

## Amaç

Mevcut legacy write sonucunu otoriter tutarken V2 runner ve E6-T6C1 parity'yi shadow olarak compose etmek; V2 veya parity başarısızlığının legacy sonucu değiştirmemesini ve production activation üretmemesini garanti etmek.

## Davranış

- Legacy write önce çalışır; başarısızsa V2 hiç başlamaz.
- Legacy başarılı olduktan sonra V2 shadow çalışır.
- V2 başarısızlığı raw mesaj yerine allowlisted failure stage ile raporlanır; legacy sonuç başarılı kalır.
- V2 tamamlanırsa legacy/V2 satırları yüklenir ve entity-level parity değerlendirilir.
- Parity FAIL görünür kalır fakat legacy sonucu değiştirmez ve `production_activation=false` invariant'ını korur.
- Caller request klonlanır ve mutation kontrol edilir.

Bu coordinator hiçbir Express route'a kayıtlı değildir. Production activation yalnız E6-T6D açık insan onayıyla değerlendirilebilir.
