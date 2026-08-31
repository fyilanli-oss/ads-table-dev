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

## Manager account discovery düzeltmesi

`customers:listAccessibleCustomers` yalnız OAuth kullanıcısının doğrudan erişebildiği customer resource adlarını verir; bir manager hesabının altındaki reklam hesaplarını listelemez. Account picker bu nedenle her erişilebilir root için resmi `customer_client` hierarchy sorgusunu çalıştırır, manager satırlarını seçim listesinden ayırır ve yalnız reklam hesaplarını seçilebilir döndürür.

Her seçilebilir reklam hesabı kendi `customerId` değeriyle birlikte onu açan manager hesabın `loginCustomerId` değerini taşır. Seçim lifecycle'ı bu çifti ownership ve connection metadata'sında korur. Manuel refresh böylece hedef reklam hesabını Google Ads API path'inde, manager hesabını ise `login-customer-id` header'ında kullanır; manager hesabı yanlışlıkla performans customer'ı olmaz.

Accessible root doğrudan OAuth erişimine sahip olduğu için root hierarchy keşif sorgusu `login-customer-id` header'ı olmadan yapılır. Root kimliği yalnız keşfedilen child ad account'a daha sonra yapılacak isteklerin login context'i olur. Account discovery hata verirse reconnect URL parametreleri hata modalı gösterilmeden tüketilir; `Close` sayfayı yenilediğinde aynı başarısız seçim akışı tekrar açılmaz.

`listAccessibleCustomers` birden fazla doğrudan root döndürebilir. Test-only developer token ile sorgulanamayan bir production root, erişilebilen test manager sonucunu gölgeleyemez: root sorguları birbirinden izole edilir, en az bir başarılı hierarchy varsa child ad account listesi döner; tüm root'lar başarısızsa yalnız redacted terminal hata üretilir. OAuth token alınmış fakat account selection tamamlanmamış bağlantı sidebar'da `Connected` sayılmaz; kullanıcı gereksiz Disconnect yapmadan Connect akışını yeniden deneyebilir.

Dashboard Google butonu kendi `/api/platform/google/status` endpoint'ini kullandığı için bu endpoint de token varlığını değil account-selection-aware ortak connection durumunu döndürür. Dataset V2 `source_job_id` UUID kolonuna yalnız normalize edilmiş scalar job UUID yazılır; orchestration context bir nesne taşısa bile allowlisted `id|sourceJobId|source_job_id` alanından UUID çıkarılır, nesnenin `[object Object]` olarak PostgreSQL'e ulaşmasına izin verilmez.
