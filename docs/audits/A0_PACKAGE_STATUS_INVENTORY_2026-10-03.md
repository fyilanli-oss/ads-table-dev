# A0 — Paket ve Statü Envanteri

**Tarih:** 2026-10-03  
**Audit:** `repository-wide-execution-integrity-audit-v1`  
**Baseline:** GitHub `main`; Execution Plan blob `e30170595bca0b95bcfac1527b4070c9da493692`  
**Durum:** Envanter çıkarıldı; doğruluk hükmü verilmedi

## İş çıktısı

E1–E14, R0–R11 ve Execution Plan içinde bulunan alt paketlerin hiçbirini audit dışında bırakmayan ilk statü haritası oluşturuldu.

## Kritik okuma kuralı

Bu kayıt, bir paketin gerçekten `Done/PASS` olduğunu doğrulamaz. Yalnız planın bugün ne iddia ettiğini ve aynı paket için kaç ayrı statü ifadesi bulunduğunu gösterir. Bütün paketlerin audit sınıfı başlangıçta `not_yet_audited`dır.

## Sayımlar

- Execution Plan satırı: **3887**
- Repository dosyası: **785**
- Birincil paket/statü ifadesi: **378**
- Benzersiz paket/alt paket kimliği: **287**
- Beklenen kök paket: **26**
- Eksik beklenen kök paket: **0**
- Birden fazla statü token’ı taşıyan paket: **60**

## Kök paket kontrolü

| Paket | Planda | İddia token’ları | Kayıt sayısı |
|---|---|---|---:|
| E1 | Var | fail, done | 1 |
| E2 | Var | deferred, done | 1 |
| E3 | Var | done | 2 |
| E4 | Var | done | 3 |
| E5 | Var | done | 1 |
| E6 | Var | parked | 1 |
| E7 | Var | blocked, verification | 3 |
| E8 | Var | parked | 1 |
| E9 | Var | done | 1 |
| E10 | Var | deferred, in_progress, done, pass | 1 |
| E11 | Var | blocked | 1 |
| E12 | Var | pending | 1 |
| E13 | Var | pending | 1 |
| E14 | Var | pending | 1 |
| R0 | Var | ready | 2 |
| R1 | Var | done | 2 |
| R2 | Var | done, pass | 2 |
| R3 | Var | in_progress, done, pass, verification | 3 |
| R4 | Var | fail, done | 2 |
| R5 | Var | fail, done | 2 |
| R6 | Var | in_progress, pending, pass | 1 |
| R7 | Var | fail, pending, pass | 2 |
| R8 | Var | blocked | 2 |
| R9 | Var | blocked | 2 |
| R10 | Var | blocked | 2 |
| R11 | Var | deferred | 2 |

## İlk governance sinyali

Aynı paket kaydında veya farklı plan konumlarında birden fazla statü token’ı bulunan **60** kimlik vardır. Bu sayı otomatik olarak hata anlamına gelmez; “parent Done / child pending”, tarihsel kayıt veya gerçek çelişki olabilir. A1–A6 sırasında tek tek sınıflandırılacaktır.

İncelenecek kimlikler:

- `E1`
- `E2`
- `E2-C1`
- `E2-C3`
- `E2-C5`
- `E2-C6`
- `E2-C7`
- `E2-T4`
- `E2-T6-D1-R1`
- `E2-T6-D3-R1`
- `E2-T6-D3-R2`
- `E2-T7-A`
- `E2-T7-B-D1`
- `E2-T7-B-D2`
- `E2-T8-A`
- `E6-T2`
- `E6-T6B1`
- `E6-T6B2`
- `E6-T6C1`
- `E6-T6C2`
- `E6-T6D1`
- `E7`
- `E7-T6`
- `E7-T7`
- `E7-T8`
- `E10`
- `E10-T1`
- `E10-T2`
- `E10-T3-A`
- `E10-T3-B`
- `E10-T4`
- `E10-T5-C`
- `E10-T6`
- `E10-T6-A`
- `E10-T6-A1`
- `E10-T6-B`
- `E10-T6-C`
- `R2`
- `R3`
- `R4`
- `R5`
- `R5-A`
- `R6`
- `R6-C`
- `R6-D1`
- `R6-D2`
- `R6-D2-C6`
- `R6-D2-C7`
- `R6-D4-G`
- `R6-D5-G1`
- `R6-D5-K1`
- `R6-D5-K3`
- `R6-D5-K4`
- `R6-D5-M1`
- `R7`
- `R7-B1`
- `R7-B2`
- `R7-B3`
- `R7-B4`
- `R7-B5-C3`

## Makine tarafından okunabilir kanıt

Tam occurrence listesi, satır numarası, ham statü ifadesi, normalize token’lar ve yardımcı artifact eşleşmeleri:

`docs/audits/evidence/A0_PACKAGE_STATUS_INVENTORY_2026-10-03.json`

## Bu dalgada yapılmayanlar

- Hiçbir `Done/PASS` iddiası doğrulanmadı veya geri alınmadı.
- Kod, migration, provider veya production sistemi değiştirilmedi.
- Remediation paketi açılmadı.
- Filename eşleşmesi evidence kabul edilmedi.
- E9-T8 bulguları kapatılmadı.

## Sonraki kapı

A0’ın bir sonraki parçası, bu envanterdeki her kimliği plan–contract–runtime–schema–live evidence matrisine yerleştirmektir. İlk derin doğrulama A1 tenant/credential/security authority dalgasında başlayacaktır.
