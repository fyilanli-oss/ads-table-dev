# E9-T7 — Backfill pause/resume/cancel runbook

## Güvenlik kapısı

Bu migration'ın uygulanması ve her gerçek backfill çalıştırması ayrı açık production onayı gerektirir. Önce migration checksum'u, hedef proje ve mevcut checkpoint sayaçları salt okunur olarak doğrulanır. Provider token veya payload loglanmaz.

## Pause

Service-role operatörü hedef checkpoint için `control_backfill_checkpoint(id, 'pause')` çağırır. Çalışan checkpoint `queued` durumuna döner, opaque cursor korunur ve lease bırakılır. **Pause doğrulaması:** `control_state=paused`, lease alanları `NULL`; yeni worker claim'i sıfır satır döndürmelidir.

## Resume

Yalnız paused checkpoint için `control_backfill_checkpoint(id, 'resume')` çağrılır. Cursor değiştirilmez; sonraki worker yeni lease alarak kaldığı sayfadan devam eder. **Resume doğrulaması:** `control_state=active`, cursor aynı ve aynı anda en fazla tek claim başarılıdır.

## Cancel

`control_backfill_checkpoint(id, 'cancel')` checkpoint'i `skipped/cancelled` terminal durumuna geçirir ve lease'i bırakır. **Cancel doğrulaması:** sonraki claim ve resume sıfır satır döndürmelidir. Cancel geri alınamaz; tekrar başlatma gerekiyorsa yeni, açıkça onaylanmış bir onboarding planı oluşturulur.

Her kontrolden sonra yalnız checkpoint durumu, attempt sayısı ve redacted E9 readiness sayaçları incelenir. Dataset satırları elle silinmez; idempotent canonical upsert korunur. Bu paket migration'ı uygulamaz ve production backfill çalıştırmaz.
