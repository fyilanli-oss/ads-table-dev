# E6-T6C1 — TikTok delivery parity evaluator

## Amaç

Legacy dashboard satırları ile delivery-only canonical V2 satırlarını Ad kimliği bazında karşılaştırmak; legacy davranışı değiştirmeden spend/impressions/clicks eşitliğini, event-null politikasını ve synthetic izolasyonunu kanıtlamak.

## Evidence

Evidence yalnız PASS/FAIL, satır sayıları ve boolean kontroller taşır. Ad/advertiser/job kimliği, metric değeri veya provider payload içermez. Entity set ve her Ad'ın delivery facts'i eşleşmelidir; yalnız toplamların eşleşmesi yeterli değildir.

Legacy sentetik placeholder'lar parity production nüfusundan çıkarılır ve sayıyla raporlanır. Canonical event facts `unsupported/null`, provenance `synthetic=false` olmak zorundadır. Drift olduğunda assertion fail-closed durur ve redacted evidence non-enumerable hata metadata'sında kalır.

Bu paket write yapmaz ve legacy input'u değiştirmez. Server dual-write composition E6-T6C2, production activation E6-T6D kapsamındadır.
