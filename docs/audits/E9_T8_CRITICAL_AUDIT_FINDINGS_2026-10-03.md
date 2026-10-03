# E9-T8 Kritik Audit Bulguları Sicili

**Kayıt tarihi:** 2026-10-03  
**Durum:** Açık — remediation başlamadı  
**Kapsam:** E9-T8 ve doğrudan bağımlılıkları  
**Genel audit durumu:** E1–E14 ve bütün R paketlerini kapsayan bağımsız repository auditi henüz yapılmadı.

## Amaç

Bu sicil, E9-T8 uygulamasına başlamadan önce doğrulanan veri güvenilirliği risklerinin kaybolmasını önler. Sicil bir çözüm paketi, implementation onayı veya production aktivasyonu değildir.

## Bağlayıcı audit hold

Aşağıdaki koşullar gerçekleşene kadar E9-T8 implementation, production scheduler aktivasyonu ve `Done/PASS` kararı yasaktır:

1. Her bulgu için kanıt, karar ve remediation sahibi belirlenir.
2. Genel repository auditi E1–E14, bütün R paketleri, contract, kod, veritabanı şeması ve canlı kanıtlar üzerinde tamamlanır.
3. Audit bulguları bağımlılık sıralı remediation paketlerine dönüştürülür.
4. P0 veri doğruluğu ve uyumluluk kapıları kapanır.
5. Provider verisi ile Dataset V2 için kontrollü reconciliation kabulü gerçek canlı kanıtla PASS olur.

## Doğrulanmış E9-T8 bulguları

### AF-E9T8-001 — Provider attribution ve correction ayarları okunmuyor

**Önem:** P0 adayı  
**Durum:** Açık

Meta istemcisi attribution-window ve action-report-time ayarlarını; Google istemcisi conversion action bazlı attribution pencerelerini okumuyor. Klaviyo tarafında ise varsayılan pencere kod içinde 5 gün olarak kabul ediliyor. Hesaba özgü gerçek pencereler bilinmeden hangi tarihin ne zaman kesinleşeceği güvenilir biçimde belirlenemez.

**Gerekli karar:** Provider bazlı resmî setting discovery, provenance ve fallback/fail-closed davranışı.

### AF-E9T8-002 — Mevcut Dataset V2 satırları kontrollü kabul akışında yeniden uzlaştırılamıyor

**Önem:** P0 adayı  
**Durum:** Açık

Meta, Google Ads ve Klaviyo controlled acceptance akışları mevcut Dataset V2 satırı gördüğünde tekrar çalışmayı reddediyor. Buna karşın Klaviyo journey contract’ı campaign satırlarının güncellenmesini ve saatlik idempotent reconciliation yapılmasını istiyor. Contract ile runtime davranışı çelişiyor.

**Gerekli karar:** Güvenli rerun/upsert kabul kapısı, değişiklik kanıtı ve duplicate engeli.

### AF-E9T8-003 — Dataset V2 satırlarında maturity/finality provenance bulunmuyor

**Önem:** P0 adayı  
**Durum:** Açık

Canonical satırlar `provisional/settling/finalized`, kullanılan attribution penceresi, son reconciliation zamanı ve finality kaynağını taşımıyor. E9-T8 üç maturity durumu tanımlarken mevcut checkpoint şeması aynı durum kümesini desteklemiyor.

**Gerekli karar:** Satır veya eşlenmiş canonical metadata seviyesinde finality sözleşmesi ve migration.

### AF-E9T8-004 — Provider’dan kaybolan satırların geçersizleştirme kuralı yok

**Önem:** P0 adayı  
**Durum:** Açık

Repository yalnız provider’ın döndürdüğü satırları upsert ediyor. Önceki okumada gelen fakat sonraki okumada kaybolan leaf için replace-set, tombstone veya stale-row invalidation davranışı tanımlı değil. Eski değerler Dataset V2’de yaşamaya devam edebilir.

**Gerekli karar:** Provider/date/scope bazlı completeness boundary ve güvenli eksilme semantiği.

### AF-E9T8-005 — Saatlik workspace reconciliation modeli mevcut checkpoint yapısıyla uyuşmuyor

**Önem:** P0 adayı  
**Durum:** Açık

Kalıcı checkpoint şeması user temelli; canonical sistem workspace temelli. Completed checkpoint terminal kabul edildiği için aynı kapsamın saatlik tekrarına doğal olarak izin vermiyor. Onboarding/readiness kodunda parked TikTok da aktif provider gibi yer alıyor.

**Gerekli karar:** Workspace authority, recurring run identity, parked-provider dışlama ve production migration.

### AF-E9T8-006 — UTC ile provider/account timezone sınırı karışıyor

**Önem:** P1  
**Durum:** Açık

Klaviyo finality sınıflandırması ve bazı Meta/Google acceptance tarih kapıları account timezone kesin uygulanmadan UTC gününe göre karar verebiliyor. UTC dışındaki hesaplarda günün yanlış tarihe yazılması veya erken/geç finalized edilmesi riski var.

**Gerekli karar:** Provider business date üretiminin tek authority’si ve sınır saatleri kabul testi.

### AF-E9T8-007 — Sıfır ile unknown semantiği provider bazında kesinleşmedi

**Önem:** P1; müşteri karşılaştırmasında P0’a yükselebilir  
**Durum:** Açık

Meta’da dönmeyen action type ve Google’da sonuç satırı üretmeyen conversion sorgusu bugün `unknown/null` yorumuna gidebiliyor. Provider’ın resmî yokluk semantiği doğrulanmadan `0` veya `unknown` seçimi kullanıcıya yanlış sonuç gösterebilir.

**Gerekli karar:** Provider bazlı absence matrix, ham yanıt kanıtı ve Dataset V2 mapping sözleşmesi.

### AF-E9T8-008 — E9 durumu implementation gerçekliğiyle çelişiyor

**Önem:** P1  
**Durum:** Açık

Execution Plan E9 üst durumunu implementation tamamlanmış gibi gösterebilir; fakat E9-T8 implementation pending’dir ve mevcut checkpoint modeli recurring workspace reconciliation’ı desteklemiyor. Bu drift başka taskların kapıyı atlamasına yol açabilir.

**Gerekli karar:** Plan, contract, kod, migration ve evidence statülerinin tek tek yeniden sınıflandırılması.

### AF-E9T8-009 — Saatlik çalışma için nicel kapasite modeli yok

**Önem:** P1  
**Durum:** Açık

Contract sharding, single-flight, retry ve provider budget kavramlarını içeriyor; ancak workspace/account sayısına göre çağrı hacmi, concurrency sınırı, provider quota bütçesi ve Supabase yük kabulü sayısal olarak tanımlı değil.

**Gerekli karar:** Kapasite formülü, kontrollü yük testi, alarm ve production activation limiti.

## E9-T8 ile ilişkili fakat ayrı remediation hattında tutulacak bulgular

### AF-LINKED-001 — Formula ve maliyet contract drift’i

Execution Plan `revenue` ve `revenue_margin` isimlerini canonical kabul ederken mevcut formula engine `profit` ve `margin` üretiyor. Klaviyo email cost belgeleri de message-level allocation konusunda birbiriyle çelişiyor. E11 öncesinde ayrıca çözülmelidir.

### AF-LINKED-002 — Plan ve kanıt statülerinde governance drift’i

Execution Plan üst özetleri, ayrıntılı paket statüleri ve güncel canlı kanıtlar bazı noktalarda birbirini tutmuyor. Genel audit sırasında bütün `Done/PASS/Pending` iddiaları evidence ile yeniden doğrulanmalıdır.

### AF-LINKED-003 — Workspace Data Deletion / uninstall / clean reinstall eksikliği

Bu konu E9-T8’in içine alınmayacaktır. Shopify zorunlu webhooks, durable idempotency, provider credential silme, Dataset V2 silme, Delete My Data ve clean reinstall ayrı P0 uyumluluk hattıdır.

## Müşteri ve ürün riski

Bir merchant’ın ilk güven testi AdsTable verisini provider Ads Manager ile karşılaştırmaktır. Açıklanamayan fark:

1. bağlantı kesme, Delete My Data ve uninstall;
2. Shopify şikâyeti;
3. uygulamanın güvenilirliğinin veya dağıtım uygunluğunun sorgulanması

sonuçlarını doğurabilir. Bu nedenle veri doğruluğu ve deletion uyumluluğu özellik değil, release blocker’dır.

## Genel auditte izlenecek zincir

`Connected → Reporting Account → İlk bootstrap → Saatlik reconciliation → Provider karşılaştırması → Attribution/correction değişiklikleri → Final veri → Formüller/UI → Disconnect → Delete My Data → Uninstall → Clean reinstall`

Her adım için şu beş kayıt karşılaştırılacaktır:

1. Execution Plan vaadi
2. executable contract
3. runtime kodu
4. veritabanı şeması
5. canlı evidence

## Bu kayıtla yapılmayanlar

- Kod veya migration yazılmadı.
- Provider çağrısı ve production veri işlemi yapılmadı.
- Production scheduler aktive edilmedi.
- Remediation paketleri henüz açılmadı.
- Genel repository auditinin tamamlandığı iddia edilmedi.
- Hiçbir bulgu `Resolved`, paket `Done` veya kabul `PASS` yapılmadı.
