# A5 — Cutover, Rollback ve Legacy Retirement Auditi

**Tarih:** 3 Ekim 2026  
**Kapsam:** E13, E14, R9, R10, R11  
**Yöntem:** Salt-okunur; Execution Plan → runtime flags → static consumer scan → canlı DB inventory → rollback/restore evidence  
**Yetki sınırı:** Feature flag, deployment, migration, read/write cutover, silme veya retirement yapılmadı.

## Analist özeti

Execution Plan'ın mevcut durum etiketleri gerçeğe uygundur:

- E13 Production cutover: Not started
- E14 Legacy retirement: Not started
- R9 Production read cutover: R6–R8 tarafından blocked
- R10 Legacy retirement: R9 stabilization tarafından blocked
- R11 WooCommerce readiness: Deferred

Bu paketlerin erken açılmaması doğru karardır. Canlı sistemde 5 V2 satırına karşı 4.947 V1 dataset satırı, 2.185 snapshot, 2.392 legacy job ve 10 legacy schedule vardır. Legacy consumer ve rollback bağı henüz sıfır değildir.

En ağır risk rollback hedefindedir: E13 rollback planı UI/API read flag'lerini legacy'ye döndürür. A4 ise legacy /dashboard'ın empty/unknown/error durumlarını sayısal 0 gösterebildiğini kanıtlamıştır. Bu nedenle legacy yol teknik olarak çalışsa bile doğruluk açısından güvenli rollback değildir.

## Canlı inventory

| Nesne | Satır |
|---|---:|
| performance_dataset_rows (V1) | 4.947 |
| performance_dataset_rows_v2 | 5 |
| dashboard_snapshots | 2.185 |
| snapshot_jobs | 2.392 |
| snapshot_schedules | 10 |
| platform_connections | 8 |
| platform_connection_tokens | 6 |
| workspace_provider_connections | 3 |

Bu sayılar silme izni veya retirement önerisi değildir. Tersine, consumer-zero ve retention çalışmasının henüz başlamaması gerektiğini doğrular.

## Bulgular

### AF-A5-001 — P0 — Mevcut legacy rollback hedefi veri doğruluğu açısından güvenli değil

Müşteri etkisi: Yeni API/UI sorununda rollback yapıldığında sistem açılır, fakat veri yokluğu veya fetch hatası ölçülmüş 0 olarak gösterilebilir.

Kanıt: E13 legacy UI/API read rollback öngörür. A4 AF-A4-001 public /dashboard'ın snapshot:null ve hata durumlarında 0 gösterebildiğini kanıtladı.

Kural: Rollback'in “çalışması” yeterli değildir; rollback yüzeyi de aynı zero/null/freshness doğruluk sözleşmesini geçmelidir.

### AF-A5-002 — P1 — R9 cutover control plane henüz yok

Müşteri etkisi: Provider/workspace canary, yüzde ramp, error budget ve anlık read rollback olmadan full cutover all-or-nothing hale gelir.

Kanıt:

- legacy_dashboard, funnel_api_canary ve funnel_api_enabled flag'leri planlanmış fakat runtime'da bulunmamıştır;
- /api/funnel/data yoktur;
- provider/account canary assignment, parity SLO ve cutover telemetry yoktur.

Bu eksiklik mevcut status ile uyumludur; R9'un Blocked kalması doğrudur.

### AF-A5-003 — P1 — Legacy consumer-zero koşulundan çok uzaktayız

Müşteri etkisi: Erken route/table/column kaldırma canlı dashboard, refresh, job, audit veya rollback zincirini kırabilir.

Kanıt:

- canlı V1/snapshot/job hacmi yüksektir;
- server.js hâlâ performance_dataset_rows, dashboard_snapshots, snapshot_jobs, snapshot_schedules ve legacy provider connection davranışlarının sahibidir;
- public dashboard inline binding ve legacy snapshot endpoint'ini kullanır.

R10 ve E14 bu nedenle başlamamalıdır.

### AF-A5-004 — P1 — Provider write rollback “V2'yi durdur” ile “V1'e tekrar yaz” davranışını birleştiriyor

Müşteri etkisi: Bir V2 incident'ında flag kapatıldığında aynı manual refresh V1 snapshot yoluna geçer. Bu, yalnız hatalı write'ı durdurmak yerine yeni V1 history üretip V1/V2 ayrışmasını büyütebilir.

Kanıt:

- META_V2_PRIMARY_REFRESH_ENABLED false branch writeMetaSnapshotImmutable çağırır;
- GOOGLE_V2_PRIMARY_REFRESH_ENABLED false branch writeGoogleSnapshotImmutable ve legacy sync davranışına döner;
- her iki flag absent durumda true default olur.

Gerekli ayrım: stop-writing, read rollback ve explicit legacy-write fallback üç farklı operasyon kararı olmalıdır.

### AF-A5-005 — P2 — Full backup/restore rehearsal kanıtı yok

Müşteri etkisi: Cutover sonrası DB, config, deployment ve read path birlikte bozulursa teorik rollback gerçek restore süresini ve veri kaybını kanıtlamaz.

Kanıt: E2 Dataset V2 restore-readiness artefaktları vardır, ancak E13 kapsamındaki full app backup/restore, environment snapshot, deployment alias, DB recovery ve RTO/RPO tatbikatı yoktur.

Bu durum E13 Not started ile uyumludur.

### AF-A5-006 — P2 — R11 WooCommerce-readiness contract henüz oluşturulmamış

Müşteri etkisi: Shopify'a özgü shop identity veya lifecycle varsayımları ortak workspace/provider/data çekirdeğine sızarsa ikinci commerce adapter pahalı refactor gerektirir.

Kanıt: Plan R11'i Deferred tutar; repository'de bağımsız R11/WooCommerce readiness contract veya test paketi bulunmadı.

Bu bir mevcut ürün hatası değildir. R11 implementation değildir; mimari bağımsızlık acceptance'ıdır ve E13/E14 öncesi uygun noktada açılmalıdır.

## Paket durum gerçeği

| Paket | A5 sınıflaması | Gerekçe |
|---|---|---|
| E13 | verified_as_not_started | control plane, SLO, restore rehearsal ve GO yok |
| E14 | verified_as_not_started | consumer-zero yok; legacy runtime aktif |
| R9 | verified_as_blocked | R6–R8 ve E11/E12 kapıları açık |
| R10 | verified_as_blocked | R9 stabilization yok; retirement unsafe |
| R11 | verified_as_deferred | implementation yok; contract/test de henüz yok |

## Güvenli dependency sırası önerisi

Bu audit implementation izni vermez. A6 için önerilen sıra:

1. A2 scheduler/provenance/finality ve A4 truthful-state remediasyonları.
2. E11/E12 gerçek API/UI acceptance.
3. Legacy rollback yüzeyini zero/null/freshness açısından güvenli hale getirme.
4. Read, write ve fallback flag'lerini ayrı control plane olarak dondurma.
5. Provider/workspace canary, parity, lag, rejection ve error-budget telemetry.
6. Full backup/restore + flag rollback rehearsal; RTO/RPO evidence.
7. İnsan GO ile R9 yüzdeli ramp ve stabilization.
8. Static/runtime consumer registry; consumer-zero observation.
9. Retention/legal/audit kararı ve restore noktası.
10. Ayrı destructive R10/E14 retirement migration/release.
11. Ortak çekirdeğin Shopify identity'ye kilitlenmediğini doğrulayan R11 contract.

## Sonuç

A5'te erken silinmiş bir legacy bileşen bulunmadı; bu olumlu sonuçtur. Risk, henüz başlamamış cutover'ın rollback hedefinin yanlış veri gösterebilmesi ve write/read rollback kararlarının birbirinden ayrılmamış olmasıdır.

A5 bir P0, üç P1 ve iki P2 bulgu kaydetmiştir. Hiçbir production state değiştirilmemiştir.
