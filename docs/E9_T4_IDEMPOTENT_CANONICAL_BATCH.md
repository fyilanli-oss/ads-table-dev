# E9-T4 — Idempotent canonical batch

Her backfill batch'i yalnız atomik olarak claim edilip `running` durumuna geçirilmiş checkpoint kapsamındaki aynı user, provider, account ve business date satırlarını kabul eder. Batch içindeki canonical kimlik tekrarları persistence öncesinde reddedilir.

Yazım yalnız mevcut `CanonicalWriteBoundary` üzerinden yapılır. Alt repository'nin `user_id,platform,platform_account_id,business_date,traffic_type,entity_key` conflict anahtarlı upsert'i aynı batch tekrar oynatıldığında yeni fact üretmek yerine aynı canonical satırı günceller. Dönüş cardinality ve canonical kimlik kümesi doğrulanmadan checkpoint tamamlanmaya uygun sayılmaz. Gerçek provider zero-row sonucu boş batch olarak kabul edilir; sentetik fact üretilmez.

Bu görev orchestration sözleşmesini hazırlar; production migration, provider çağrısı, checkpoint güncellemesi veya backfill çalıştırmaz.
