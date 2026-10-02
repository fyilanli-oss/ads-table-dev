# AdsTable repository çalışma kuralları

Bu repository'de **Execution Plan anayasadır**. Bir iş paketi plan, ilgili karar belgesi ve executable contract ile çelişemez.

## Her iş için zorunlu başlangıç kapısı

Kod, test veya yapılandırma değişikliğinden önce aşağıdaki kapılar tamamlanır:

1. İş bir dış provider, platform, API, framework, kütüphane veya Codex/OpenAI davranışı içeriyorsa önce ilgili tarafın **güncel resmî dokümantasyonu** okunur. Blog, hafıza, eski task çıktısı veya tahmin resmî dokümanın yerine geçmez.
2. Shopify işi için Shopify; Klaviyo işi için Klaviyo; GitHub işi için GitHub; Supabase işi için Supabase; Vercel işi için Vercel; Codex/OpenAI ortamı için OpenAI Docs esas alınır. Koddan önce kullanılan resmî kaynak ve kontrol tarihi kullanıcıya bildirilir.
3. İşin başında `git status`, mevcut commit/branch ve `git rev-parse --git-common-dir` kontrol edilir. İstenen teslim branch/commit gerektiriyorsa hedef dala geçilebildiği ve ortak Git metadata dizinine yazılabildiği **uygulamaya başlamadan önce** doğrulanır.
4. Git yazma kapısı, provider dokümanı veya zorunlu environment yetkisi geçmiyorsa uzun uygulama/test çalışmasına başlanmaz. Sorun ilk dakikalarda doğru katmanda sınıflandırılır ve çözülür; kullanıcıya saatler sonra komut çalıştırma işi bırakılmaz.
5. Yerel Git metadata/sandbox/ACL sorunu “GitHub çalışmıyor” diye adlandırılmaz. GitHub ağı/API'si, yerel Git repository'si ve Codex sandbox'ı ayrı ayrı teşhis edilir.
6. Kanıtlanmamış bilgiyle “biliyorum” varsayımı yapılmaz. İlgili provider'ın güncel resmî sözleşmesi okunmadan o provider hakkında implementasyon kararı verilmez.

## Shopify embedded UI için zorunlu kapı

`src/shopify/**`, Shopify embedded route'ları, App Home ekranları veya bunların testleri üzerinde çalışmadan önce aşağıdaki belgelerin tamamı okunur:

1. `docs/SHOPIFY_EMBEDDED_UI_CONSTITUTION.md`
2. `contracts/shopify/shopify-embedded-ui-constitution-v1.json`
3. `docs/templates/SHOPIFY_EMBEDDED_UI_TASK_TEMPLATE.md`
4. İlgili E10/R paketi ve UI contract'ı

Bağlayıcı kurallar:

- Görsel ve etkileşimli arayüz yalnız Shopify App Bridge ve Shopify'ın güncel Polaris web componentleriyle kurulur.
- Raw HTML form/action kontrolleri, özel button/card/badge/modal/form taklidi, inline CSS, `<style>`, literal renk, özel gölge/kenarlık/radius ve Shopify görünümünü CSS ile kopyalama yasaktır.
- `s-clickable`, `s-button` veya `s-link` ile sağlanabilen bir eylem için button taklidi olarak kullanılmaz.
- Resmî component veya property doğrulanamıyorsa kod yazılmaz; karar/contract güncellemesi için durulur.
- Koddan önce analist brief'i ve exact Shopify component eşlemesi hazırlanır.
- Desktop ve gerçek mobil Shopify Admin kabulü, doğru durum/metin/aksiyon kanıtı olmadan iş `Done` veya `PASS` sayılamaz ve merge edilemez.
- Bir ekranın çalışması görsel kabul anlamına gelmez. Kullanıcı açıkça kabul etmeden “tamamlandı” denmez.
- Kanıtlanmamış aşama tamamlanmış kabul edilmez.

Mevcut geçici ihlaller yalnız constitution contract'ındaki açık borç listesinde tutulabilir. Bu liste yeni ihlal eklemek için emsal değildir; sayı yalnız azalabilir.

## Zorunlu Codex Windows çalışma ve GitHub teslim modeli

Bu repository için Windows Codex çalışma modeli `contracts/repository-delivery-workflow-v1.json` ile bağlayıcıdır. Amaç yerel geliştirmeyi yasaklamak değil, hiçbir anlamlı işi yalnız yerel diskte bırakmamaktır.

1. GitHub `main` ürün kaynağı ve başlangıç otoritesidir. Yeni iş güncel uzak `main` commit'i doğrulanmadan başlatılmaz.
2. Ana Local checkout `C:\\Users\\dev\\Documents\\Codex\\ads-table-dev` aktif geliştirme alanı değildir; repository anchor'ı ve salt-okunur envanter kaynağıdır. Kullanıcı açıkça istemeden burada kod, test, sözleşme, migration veya Execution Plan değişikliği üretilmez.
3. Kodlama, test ve build işlemleri görev bazlı Codex-managed worktree'de yapılır. Yerel çalışma dosyaları geçici execution materyalidir; kalıcı teslim sayılmaz.
4. GitHub branch, blob/tree, commit, PR, CI okuma ve merge işlemlerinde birincil uzak yol GitHub Connector'dır. Windows sandbox içindeki yerel `.git` yazımı teslim için zorunlu bağımlılık yapılamaz.
5. GitHub web editörü veya tarayıcı üzerinden repository mutation yalnız kullanıcı bunu açıkça isterse kullanılabilir. Connector fallback'i olarak kendiliğinden web arayüzüne geçilmez.
6. Başlangıçta worktree durumu, HEAD/branch, `git-common-dir`, staged/tracked/untracked içerik ve GitHub Connector erişimi ayrı kapılar olarak doğrulanır.
7. Connector erişimi veya güncel uzak `main` kanıtı yoksa implementasyona başlanmaz. Yerel Git/GCM/ACL, elevated/unelevated işlem, process sonlandırma, servis/VM/server yeniden başlatma veya bilinmeyen sonuçlu sistem mutation'ı denenmez; engel ilk kapıda raporlanır.
8. Uzak teslim her zaman güncel `main`den açılan `codex/` branch üzerinde, yalnız görev kapsamındaki explicit dosyalarla yapılır. Güncel blob SHA/content yeniden okunur; yazım sonrası uzak içerik tekrar doğrulanır.
9. PR ve zorunlu CI PASS olmadan iş merge-ready sayılmaz. Merge ayrıca açık kullanıcı onayı gerektirir.
10. Merge sonrası görev worktree'si, local-only sıfır doğrulamasından sonra Codex'in geri alınabilir arşiv mekanizmasıyla kapatılır. Cache, dependency ve build çıktıları kaynak teslim değildir.
11. Ana Local klasör veya ortak `.git`, ona bağlı worktree'ler kapanmadan silinmez, taşınmaz veya yeniden kurulmaz.
12. Connector kullanılamıyorsa web editörü, yerel Git onarımı veya server müdahalesine geçilmez; iş güvenli biçimde durur ve kullanıcıya tek engel bildirilir.

## Zorunlu iş bitiş kapısı: local-only iş sıfır

Tamamlanmış, onay bekleyen veya yeniden üretilemeyecek hiçbir proje işi yalnız yerel diskte bırakılamaz. Bir iş paketi aşağıdaki kapılar geçmeden tamamlandı, güvende, teslim edildi veya merge-ready sayılamaz:

1. İş sonunda staged, tracked-modified, untracked proje dosyaları, stash'ler, worktree'ler ve upstream'siz/ahead yerel branch uçları yeniden envanterlenir.
2. Anlamlı kod, test, sözleşme, Execution Plan kararı, migration ve yeniden üretilemeyecek kabul/kanıt dosyaları GitHub üzerinde doğrulanabilir bir branch, PR veya commit'e aktarılmış olmalıdır. Tamamlanmış iş için doğrulanmamış local-only proje içeriği sıfır olmalıdır.
3. Uzak dosyalar GitHub'dan tekrar okunur; beklenen commit SHA ve exact content/hash doğrulanır. Zorunlu CI/testler PASS olmadan uzak kopya güvenli teslim sayılmaz.
4. Merge ayrı bir kapıdır ve açık kullanıcı onayı gerektirir; fakat merge bekleyen çalışma dahi GitHub branch/PR üzerinde dayanıklı olmalıdır.
5. Yerel stash, reflog, worktree veya makine yedeği tek başına dayanıklı teslim ya da uzak yedek sayılmaz. Eşdeğerliği kanıtlanmamış local-only içerik varsa iş açık risk olarak raporlanır ve kapanmaz.
6. Secret, ignored cache, dependency/build çıktısı ve yeniden üretilebilir geçici dosyalar GitHub'a yüklenmez. Bunların proje işi olmadığı açıkça sınıflandırılır; secret'lar uygun secret manager veya provider sisteminde tutulur.
7. Yerel dosya, branch veya stash; uzak eşdeğeri ve kurtarılabilirliği doğrulanmadan silinmez, drop edilmez veya overwrite edilmez.
8. Teslim özetinde GitHub URL/PR/commit, CI sonucu ve varsa bilinçli yerel istisnalar açıkça yazılır. “GitHub güncel” ifadesi yalnız bu kanıtlarla kullanılabilir.
9. Repository ZIP veya günlük makine yedeği ikincil kurtarma katmanıdır; Git commit geçmişi, branch/tag, PR/issue ve diğer GitHub metadata'sının yerine geçmez ve bu bitiş kapısını kaldırmaz.
10. Merge sonrası görev worktree'si arşivlenir; arşiv sonucu ve bilinçli bırakılan yerel istisnalar teslim özetinde yazılır.
