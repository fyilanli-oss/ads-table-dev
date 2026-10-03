# Repository-Wide Execution Integrity Audit

**Tarih:** 2026-10-03  
**Durum:** Kapsam hazır; audit execution başlamadı  
**Bağlayıcı contract:** `contracts/repository-wide-execution-integrity-audit-v1.json`

## İş çıktısı

AdsTable’ın müşteri, Shopify ve provider karşısında yanlış veri, eksik silme, tenant güvenlik açığı veya geri döndürülemez cutover üretebilecek gizli risklerini implementation başlamadan görünür hale getiren bağımsız audit.

## İş değeri

Merchant’ın AdsTable ile provider Ads Manager verisini karşılaştırdığı ilk güven anını; Delete My Data, uninstall ve Shopify review süreçlerini; gelir/maliyet/formül doğruluğunu release öncesinde korur.

## Neden paket bazlı normal ilerleme yeterli değil?

E9-T8 incelemesi tek bir paketin içinde görünmeyen fakat başka katmanlara yayılan riskleri ortaya çıkardı:

- reconciliation contract’ı ile runtime rerun davranışı çelişiyor;
- finality provider ayarlarına değil varsayıma dayanabiliyor;
- Dataset V2 maturity provenance taşımıyor;
- provider’dan kaybolan leaf eski haliyle kalabiliyor;
- checkpoint authority ve recurring schedule modeli uyuşmuyor;
- deletion/uninstall hattı E9-T8 dışında olmasına rağmen aynı müşteri güven zincirini etkiliyor.

Bu nedenle audit yalnız task başlıklarını okumaz; müşteri yolculuğunu uçtan uca izler.

## Bağımsızlık kuralı

Audit sırasında bulgu düzeltilmez. Aksi halde:

1. kök sebep kaybolabilir;
2. başka paketlerle ortak bağımlılık görülmeden yerel çözüm üretilebilir;
3. audit kanıtı ile remediation kanıtı birbirine karışabilir;
4. bir düzeltme başka bir riski örtebilir.

Audit sonunda remediation paketleri birlikte ve bağımlılık sırasıyla önerilir. Uygulama ayrıca onaylanır.

## İnceleme zinciri

`Install → Workspace authority → Provider connect → Reporting Account → İlk bootstrap → Saatlik snapshot → Reconciliation → Provider karşılaştırması → Attribution/finality → Formula/currency/cost → UI/API → Disconnect → Delete My Data → Uninstall → Clean reinstall → Cutover → Rollback → Retirement`

Her düğümde beş kanıt katmanı karşılaştırılır:

| Katman | Sorulan soru |
|---|---|
| Execution Plan | Kullanıcıya ve sisteme ne vaat ediliyor? |
| Executable contract | Bağlayıcı davranış tam olarak ne? |
| Runtime kodu | Sistem gerçekte ne yapıyor? |
| Veritabanı | Authority, persistence ve constraint bunu destekliyor mu? |
| Canlı evidence | İddia gerçek ortamda doğrulanmış mı? |

Bir `Done/PASS` iddiası yalnız uygulanabilir beş katman birbiriyle uyuşuyorsa doğrulanır. Eksik kanıt PASS değildir.

## Audit dalgaları

### A0 — Envanter ve statü doğruluk haritası

E1–E14, R0–R11 ve bütün alt paketler çıkarılır. Her `Done/PASS/Pending/Blocked/Parked` iddiası kanıt konumuyla eşlenir.

### A1 — Tenant, credential ve güvenlik authority

E1–E3 ve R0–R5; workspace/user authority, OAuth/token sınırı, RLS, cross-tenant erişim ve migration gerçekliği incelenir.

### A2 — Provider veri doğruluğu ve maturity

E4–E9, R6 ve R8; provider sorgusu, ham yanıt, mapping, Dataset V2 yazımı, zero/unknown, timezone, attribution, correction, reconciliation ve finality incelenir. E9-T8 sicili bu dalganın başlangıç kanıtıdır.

### A3 — Shopify lifecycle, privacy ve deletion

E10 ve R7; install, mandatory privacy webhooks, Delete My Data, uninstall, credential/data deletion ve clean reinstall zinciri incelenir. Bu hat E9-T8’e karıştırılmaz.

### A4 — Formula, API ve UI doğruluğu

E11–E12; canonical metric support, aggregation grain, currency, Klaviyo maliyet dağıtımı, revenue/ROAS/CPC/CPS ve kullanıcıya gösterilen zero/unknown/freshness davranışı incelenir.

### A5 — Cutover, rollback ve retirement

E13–E14 ile R9–R11; canary, parity, backup/restore, rollback, consumer-zero ve destructive retirement kapıları incelenir.

### A6 — Cross-package ölüm fermanı taraması

Bulgular duplicate’lerden arındırılır; ortak kök nedenler birleştirilir; P0/P1 release blocker listesi ve bağımlılık sıralı remediation paket seti hazırlanır.

## Risk sınıfları

- **P0:** Yanlış müşteri verisi, veri kaybı/ifşası, tenant authority bozulması, deletion/privacy ihlali veya gerçek Shopify kaldırılma/review blocker riski.
- **P1:** Stale, eksik, yanıltıcı veya operasyonel olarak güvensiz davranış.
- **P2:** İleride release blocker’a dönüşebilecek governance, observability, ölçek veya bakım zayıflığı.
- **P3:** Güncel maddi müşteri/uyumluluk etkisi olmayan temizlik ve kalite işi.

Risk, çözümün zorluğuna göre değil müşteri/platform etkisine göre sınıflandırılır.

## Zorunlu çıktılar

1. E1–E14 ve R paketleri statü doğruluk matrisi
2. İnsan tarafından okunabilir audit raporu
3. Makine tarafından okunabilir bulgu sicili
4. Müşteri yolculuğu risk matrisi
5. P0/P1 release blocker listesi
6. Bilinmeyenler ve çözülemeyen çelişkiler listesi
7. Bağımlılık sıralı remediation paket önerisi

## Güvenlik sınırı

Audit read-only yürür. Production mutation, migration, deploy, provider write, token/revoke, veri silme, scheduler activation veya legacy retirement audit yetkisine dahil değildir. Böyle bir ihtiyaç oluşursa audit durur ve ayrı açık karar istenir.

## Tamamlanma tanımı

Audit yalnız bütün dalgalar ve zorunlu çıktılar incelendiğinde tamamlanabilir. Audit tamamlanması, bulguların çözüldüğü veya remediation implementation’ın onaylandığı anlamına gelmez.
