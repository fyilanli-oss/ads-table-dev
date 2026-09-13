# E10-T4 — Token, uninstall ve privacy lifecycle

## Karar

Yalnız doğrulanmış Shopify webhook/admin olayı lifecycle yetkisi üretir. Uninstall veya access loss yeni Shopify/provider erişimini önce kapatır, ardından shop token'larını kaldırır ve retention kararını kaydeder. Shop redact aynı fail-closed sıraya personal-data deletion ve evidence ekler. Customer data request/redact yalnız opaque subject referansıyla sınırlıdır.

Her event `shop_id + event_id` ile tek kez claim edilir; replay reddedilir. Eylemler sıralı ve injectable port'lardır. Sonuç yalnız event sınıfı, eylem sayısı ve access/token boolean'larını taşır; shop, customer, token veya payload değeri yayımlamaz.

Hardcoded retention süresi veya varsayılan destructive policy yoktur. `record_retention_decision`, `delete_*` ve evidence portlarının gerçek policy/persistence implementasyonu production migration/provisioning öncesinde ayrı onay gerektirir.

Bu task gerçek webhook registration/call, token deletion, customer export/delete, migration, Partner Dashboard veya production işlemi çalıştırmaz.
