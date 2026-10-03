# A6 — Repository-Wide Critical Risk Synthesis

**Tarih:** 2026-10-03  
**Mod:** Read-only audit synthesis  
**Durum:** Audit execution complete; user review pending  
**Remediation yetkisi:** Yok

## Analist sonucu

A0–A5 genel audit, tekil teknik kusurlar listesinden daha önemli dört **P0 ölüm fermanı** çıkardı:

1. Shopify compliance/uninstall olayları gerçek, doğrulanmış ve dayanıklı bir lifecycle zincirine girmiyor.
2. Delete My Data ürün vaadi, exact manifest ve çalışan bir workspace deletion executor ile karşılanmıyor.
3. Terminal deletion sonrası clean reinstall eski workspace yetkisini canlandırabilir.
4. Legacy dashboard ve dolayısıyla mevcut rollback hedefi, veri yokluğunu veya hatayı ölçülmüş **0** gösterebilir.

Bu dört riskten herhangi biri çözülmeden ilgili release kapısının açılması; yanlış müşteri verisi, yerine getirilmeyen silme vaadi, uninstall sonrası süren erişim veya eski yetkinin dirilmesi sonucunu doğurabilir.

Ham dalga bulguları birbirini tekrar ediyordu: A1–A5 içinde **6 P0** ve **20 P1** kayıt vardı. Kök neden bazında tekilleştirme sonunda **4 P0 + 7 P1 = 11 release blocker** kaldı. Bu azalma riskin azaldığı anlamına gelmez; aynı kök nedenin veri, UI ve rollback katmanlarındaki sonuçları tek kayıtta toplanmıştır.

## Ne güvenli biçimde doğrulandı?

- Caller-supplied workspace/user/shop kimlikleri canonical Shopify authority kabul edilmiyor.
- Canonical provider token envelope'ları server-only ve encrypted tasarlanmış; canlı legacy plaintext token sayısı 0.
- Cross-workspace canonical Dataset V2 write koruması var.
- Legacy provider write ve standalone OAuth insert guard'ları canlı veritabanında aktif.
- Meta ve Klaviyo için kontrollü provider evidence var; Google raw response verified-empty olarak ayrı tutuluyor.
- Formula Phase 1 testleri **53/53**, Query Phase 2 testleri **33/33** geçiyor.
- Destructive legacy retirement yapılmamış; mevcut blocked durum doğrudur.

Bu pozitif kontroller, aşağıdaki açık zincirleri otomatik olarak PASS yapmaz.

## Tekilleştirilmiş P0 release blocker'lar

| ID | Kök risk | Müşteri/platform sonucu | Kilitlediği kapı |
|---|---|---|---|
| AF-A6-001 | Shopify lifecycle ingress + uninstall fail-closed zinciri yok | Compliance/uninstall olayı kaybolabilir; provider/job erişimi sürebilir | Shopify lifecycle/review GO |
| AF-A6-002 | Delete My Data vaadi executor'a bağlı değil | Silindi denirken veri/credential kalabilir | Delete My Data ve hard-delete PASS |
| AF-A6-003 | Clean reinstall generation authority yok | Eski workspace/token/dataset yeniden yetkili olabilir | Clean reinstall GO |
| AF-A6-004 | Legacy false-zero + güvensiz rollback target | Yok/hata/bayat veri gerçek 0 görünür | E11/E12 cutover ve E13/R9 rollback GO |

## Tekilleştirilmiş P1 release blocker'lar

| ID | Kök risk | Neden ayrı tutuldu? |
|---|---|---|
| AF-A6-005 | Supabase privilege/SECURITY DEFINER yüzeyi | Tenant ve credential zincirinin temel güvenlik sınırı |
| AF-A6-006 | Token crypto startup fail-closed kanıtı yok | Env adlarının varlığı doğru runtime state'i kanıtlamıyor |
| AF-A6-007 | Hourly scheduler/job/lease/checkpoint/provenance yok | Recurring completeness, backfill resume ve fact lineage aynı operasyonel kök |
| AF-A6-008 | Provider maturity/reconciliation/finality taşınmıyor | Ads Manager parity ve truthful freshness için ayrı provider policy gerekiyor |
| AF-A6-009 | Formula/API semantic envelope eksik | Revenue naming, support/null reason, compare ve Shopify DTO aynı tüketim sözleşmesine bağlı |
| AF-A6-010 | Cutover control plane ve bağımsız read/write/fallback yok | Canary/rollback operasyonu veri üretim kararından ayrılmalı |
| AF-A6-011 | Consumer-zero sağlanmadı | Legacy sökümü halen aktif yolları kırar |

## Ölüm fermanı zincirleri

### 1. Veri doğruluğu zinciri

`AF-A6-007 → AF-A6-008 → AF-A6-009 → AF-A6-010 → AF-A6-011`

Scheduler/run provenance olmadan provider düzeltmeleri güvenilir biçimde işletilemez. Provider finality taşınmadan Formula/API/UI “current/final” diyemez. Bu sözleşme tamamlanmadan canary/rollback doğruluk koruyamaz. Cutover stabilize olmadan consumer-zero ve retirement mümkün değildir.

### 2. Shopify privacy zinciri

`AF-A6-005/006 → AF-A6-001 → AF-A6-002 → AF-A6-003`

Önce least-privilege ve credential fail-closed sınırı; sonra doğrulanmış lifecycle ingress; ardından exact deletion manifest/executor; en son yeni install generation. Sıra ters çevrilirse yeni akış eski yetkiyi veya silinmemiş kalıntıları taşıyabilir.

### 3. Truthful presentation zinciri

`AF-A6-004 → AF-A6-008 → AF-A6-009`

False-zero hemen ayrı bir güvenlik/doğruluk kapısıdır. Ancak yalnız UI metnini değiştirmek yetmez: freshness/finality ve derived support/null reason veri sözleşmesinden UI'a kadar taşınmalıdır.

## Önerilen remediation sırası

Bu sıra bir uygulama onayı değildir.

0. **A6-RM-00 — Release/status containment gates**
1. **A6-RM-01 — Database least-privilege + crypto fail-closed**
2. **A6-RM-02 — Truthful legacy state adapter**
3. **A6-RM-03 — Shopify verified lifecycle ingress + immediate uninstall stop**
4. **A6-RM-04 — Workspace deletion manifest + resumable executor**
5. **A6-RM-05 — Clean reinstall generation authority**
6. **A6-RM-06 — Workspace scheduler/job/lease/checkpoint/provenance**
7. **A6-RM-07 — Provider maturity + bounded reconciliation policy**
8. **A6-RM-08 — Versioned Formula/Compare/support envelope**
9. **A6-RM-09 — E11/E12 secure Shopify API + truthful UI**
10. **A6-RM-10 — Cutover control plane + canary + telemetry + full restore rehearsal**
11. **A6-RM-11 — Consumer-zero + legacy retirement**
12. **A6-RM-12 — R11 adapter-readiness contract**

Paralel yürütülebilecek iki ilk teknik hat vardır: güvenlik/lifecycle hattı ve truthful-legacy containment. Provider operational authority ancak güvenlik/config kapısından sonra başlamalıdır.

## Müşteri yaşam döngüsü kararı

- Install: **at risk**
- Provider connect: **at risk**
- Initial bootstrap: **at risk**
- Hourly snapshot/backfill: **blocked**
- Ads Manager parity/correction/finality: **blocked**
- Formula/API/UI presentation: **blocked**
- Delete My Data: **blocked**
- Uninstall: **blocked**
- Clean reinstall: **blocked**
- Cutover/rollback: **blocked**
- Legacy retirement: **blocked**

“Blocked” mevcut kodun tamamının çalışmadığı anlamına gelmez. Müşteri veya platform açısından güvenli PASS kanıtının henüz bulunmadığı anlamına gelir.

## Açık unknown'lar

En kritik unresolved alanlar:

- Legacy dashboard ve Delete My Data yüzeylerinin gerçek production reachability/trafik durumu.
- Production encryption boolean/key state'inin secret okumadan doğrulanması ve startup fail-closed davranışı.
- Repository/database dışında canonical scheduler olup olmadığı.
- DB dışı storage/log/deployment/third-party deletion manifesti.
- Her provider için exact reconciliation window, attribution/finality ve quota/cost bütçesi.
- Google non-empty live hierarchy/metric acceptance.
- Full restore RTO/RPO hedefleri.
- R11 common-core adapter bağımsızlığı.

Unknown kayıtları PASS, “veri yok” veya 0 olarak yorumlanamaz.

## Dosyalar

- `A6_DEDUPLICATED_P0_P1_REGISTER_2026-10-03.json`: kaynak bulgudan tekilleştirilmiş blocker'a izlenebilirlik.
- `A6_DEPENDENCY_ORDERED_REMEDIATION_PACKAGES_2026-10-03.json`: bağımlılık sıralı paket önerisi.
- `A6_CUSTOMER_LIFECYCLE_RISK_MATRIX_2026-10-03.json`: müşteri yolculuğu bazında risk.
- `A6_UNRESOLVED_UNKNOWN_REGISTER_2026-10-03.json`: PASS'e çevrilmemesi gereken açık unknown'lar.

## Kapanış durumu

A0–A6 **audit execution** tamamlandı. Audit henüz kullanıcı tarafından review edilmediği için anayasal contract'ın `completion_rule` şartına göre “reviewed/complete” değildir. Hiçbir remediation paketi otomatik olarak açılmamış veya uygulanmamıştır. Production write, migration, deployment, provider mutation, data deletion ve legacy retirement yapılmamıştır.
