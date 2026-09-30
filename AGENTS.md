# AdsTable repository çalışma kuralları

Bu repository'de **Execution Plan anayasadır**. Bir iş paketi plan, ilgili karar belgesi ve executable contract ile çelişemez.

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

