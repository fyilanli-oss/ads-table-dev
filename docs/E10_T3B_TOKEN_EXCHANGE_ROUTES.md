# E10-T3-B — Token exchange, encrypted persistence ve HTTP registration

Verified embedded session, active tenant çözümlemesinden sonra injectable Shopify token-exchange client'ına aktarılır. Dönen token strict allowlist ile doğrulanır ve yalnız encrypted olduğunu kanıtlayan server-side token store'a yazılır. Response yalnız redacted lifecycle durumu taşır.

HTTP sınırı install callback'i HMAC/state/complete-install zincirine, embedded session endpoint'ini yalnız Authorization Bearer header'ına bağlar. Query/body token kabul edilmez; route'lar business veya persistence logic içermez.

Bu repository paketi gerçek Shopify endpoint'i, credential, migration veya production token store oluşturmaz. Resmi API sürümü ve production adapter'ları ayrı production-onaylı provisioning öncesinde yeniden doğrulanır.
