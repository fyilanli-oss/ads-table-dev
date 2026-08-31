# E5-T7 — Google canlı V2-primary runtime

Google manuel Refresh, onaylanan Meta modeliyle aynı şekilde doğrudan Dataset V2'ye bağlanır.

- Refresh yalnız Google customer timezone'ından türetilen tek business date'i sorgular.
- Standard Ad ve PMax Asset Group ayrı sorgulanır ve ayrı V2 completeness üretir.
- Dataset V1/Dashboard V1 yazılmaz; V2 hatasında V1 fallback yapılmaz.
- V1 kaynaklı Google Sheets auto-sync bu yolda çalıştırılmaz.
- Gerçek zero-row sonuç başarıyla kanıtlanır ve sahte satır yaratılmaz.
- Response/job evidence customer, entity, token, provider row veya ham metrik taşımaz.

Runtime gate `GOOGLE_V2_PRIMARY_REFRESH_ENABLED=true` ile Meta V2-primary kararıyla aynı biçimde açık gelir. Rollback yalnız gate'i kapatır; hata anında V1'e fallback yapmaz.

Bu repository paketi canlı refresh çalıştırmaz. Merge ve Vercel kontrollerinden sonra refresh kullanıcı tarafından başlatılır.
