# E1-T6D GitHub production dry-run runbook

## Güvenli environment hazırlığı

Bu hazırlık repository kodundan yapılamaz; yetkili bir kullanıcı GitHub üzerinde tamamlamalıdır:

1. Repository **Settings → Environments** bölümünde `production-token-backfill` environment'ını oluşturun.
2. Mümkünse bir required reviewer ile manuel deployment onayı ekleyin.
3. Deployment branch kuralını yalnız `main` branch'iyle sınırlandırın. Workflow ayrıca yanlış branch'i hata ile reddeder; iki kontrol birlikte korunmalıdır.
4. Şu environment variable adlarını ekleyin: `SUPABASE_URL`, `SUPABASE_PROJECT_REF`, `PROVIDER_TOKEN_ACTIVE_KEY_ID`.
5. Şu environment secret adlarını ekleyin: `SUPABASE_SERVICE_ROLE_KEY`, `PROVIDER_TOKEN_ENCRYPTION_KEYS`, `PROVIDER_TOKEN_BACKFILL_REFERENCE_SECRET`.

Gerçek değerleri bu belgeye, repository'ye, PR'a, issue'ya veya sohbete koymayın. Değerleri yalnız yetkili kullanıcı GitHub Environment ekranına girmelidir; Codex'e veya bu taska göndermeyin.

## İlk manuel çalıştırma

1. GitHub **Actions → Provider token production dry-run → Run workflow** ekranını açın.
2. Branch olarak `main` seçin.
3. İlk çalıştırmada `batch_size` için `25` seçin ve `cursor` alanını boş bırakın.
4. Workflow'u başlatın ve protected environment reviewer onayını tamamlayın.
5. Production job logunda yalnız operator'ın redacted JSON sonucunu inceleyin.

Taskın teslim edildiği PR kapsamında workflow çalıştırılmaz. Environment'ın oluşturulması ve değerlerin girilmesi de task tarafından yapılmaz.

## Evidence değerlendirmesi

İzin verilen evidence alanları `ok`, `mode`, `contractVersion`, `scanned`, `eligible`, `written`, `alreadyEncrypted`, `rotationCandidates`, `empty`, `failed`, `nextCursor`, `failures` ve `batchSize` ile sınırlıdır.

- Devam koşulları `ok=true`, `mode=dry-run`, `written=0` ve `failed=0` sonuçlarıdır.
- `written` her zaman `0` olmalıdır. Başka bir değer fail-closed operator invariant ihlalidir.
- `failed > 0` ise sonraki batch'i başlatmayın; sonucu yöneticinin güvenlik incelemesine gönderin.
- `nextCursor` doluysa yalnız güvenli yönetici kanalında tutun. Önceki run incelendikten sonra değeri değiştirmeden bir sonraki manuel run'ın `cursor` alanına taşıyın.
- Secretları, cursor'ı veya allowlist dışındaki logları paylaşmayın. Artifact ya da job summary üretmeyin.
- Her run sonucunu bir sonraki batch öncesinde yönetici review'una gönderin.

Başarılı dry-run, write-mode için onay değildir. Bu workflow write-mode sunmaz ve encryption flag'ini açmaz. Ayrı GO/NO-GO kararı verilene kadar encrypted backfill, plaintext temizliği ve E1-T6D `Done` durumu söz konusu değildir.

## Sorun halinde

Workflow başarısızsa sonraki cursor batch'ini çalıştırmayın ve write-mode'a geçmeyin. Secret veya ham hata çıktısı paylaşmadan workflow'u devre dışı bırakın ya da workflow artefaktını geri alın. Mevcut operator, Supabase şeması ve kapalı encryption flag'i değişmeden kalır.
