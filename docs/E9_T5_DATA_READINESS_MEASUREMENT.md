# E9-T5 — Data readiness ölçümü

Readiness kanıtı altı fail-closed kapı üretir: beklenen checkpoint'lerin tamamlanması, canonical identity duplicate bulunmaması, metric-support sözleşmesi, timezone bağlamı, FX bağlamı ve freshness. Tüm kapılar doğru olmadan sonuç `PASS` olamaz.

Freshness, en yeni tamamlanmış beklenen checkpoint business date'i ile çağıranın açıkça verdiği `as_of_business_date` arasında ölçülür ve en fazla bir gün olabilir. Tamamlanmış gerçek zero-row provider sonucu completeness ve freshness açısından geçerlidir; sentetik canonical satır gerektirmez. Ölçüm yalnız sayı, tarih ve boolean kapılar döndürür; provider payload'u veya metrik değerleri kanıta taşımaz.

Bu görev ölçüm sözleşmesini hazırlar; production sorgusu, provider çağrısı, migration veya backfill çalıştırmaz. Provider bazlı sunum E9-T6 kapsamındadır.
