# E9-T6 — Provider readiness/parity dashboard

Dashboard Meta, Google, TikTok ve Klaviyo için E9-T5 readiness kanıtını provider bazında özetler. Hesap kimlikleri, provider payload'ları ve metrik değerleri gösterilmez; yalnız hesap kanıtı, checkpoint, canonical row, duplicate ve parity sayaçları sunulur. Pinterest ve Organic kabul edilmez.

Bir provider'ın readiness kanıtları geçip parity kanıtı henüz yoksa `PARITY_PENDING`, readiness veya parity başarısızsa `BLOCKED`, tüm readiness ve mevcut parity kanıtları geçtiyse `READY` gösterilir. Genel durum ancak kanıtı bulunan tüm provider'lar `READY` olduğunda `READY` olabilir. Bu gösterim production aktivasyonu yapmaz.

Bu görev redacted sunum modelini hazırlar; public UI değişikliği, production sorgusu, provider çağrısı veya backfill çalıştırmaz. Operatör kontrolü ve runbook E9-T7 kapsamındadır.
