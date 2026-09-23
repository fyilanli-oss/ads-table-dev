# R3 — Dataset V2 workspace tenant dönüşümü

## Durum

`In progress — R3-A ve R3-B tamamlandı; R3-C, R6 runtime kabul kapısına bağlı`.

R3-A additive migration açık production onayıyla canlı Supabase'e uygulandı ve postcheck `PASS` verdi. Canonical contract V2, workspace-scoped in-memory/Supabase repository, workspace query service, workspace backfill boundary ve güvenlik SQL'leri hazırdır. Provider runtime cutover yapılmadı.

R3-B, server-resolved workspace authority kullanan ortak Dataset V2 runtime sınırını tamamladı. Browser/query/body kaynaklı `workspace_id`, `user_id` veya `shop_id` tenant authority olarak kabul edilmez. Aynı runtime cross-workspace yazımı ve okumayı fail-closed reddeder; hiçbir production route bu pakette aktive edilmez.

Supabase CLI'nin Windows kullanıcı profilindeki global metadata klasörü işletim sistemi ACL'i tarafından engellendi. `migration new` yardım/scaffold çağrısı iki farklı izinli yöntemle denenip aynı noktada durduğu için migration dosyası repository içinde `apply_patch` ile oluşturuldu. Canlı uygulama Supabase migration servisi üzerinden yapıldı; repository dosyası canlı ledger sürümü `20260923083734` ile eşitlendi.

## Analist özeti

Dataset V2 bugün veriyi `user_id` altında ayırıyor. R1 kararı gereği kullanıcı tenant değildir; tenant `workspace_id` olmalıdır. Geçiş, mevcut hatları kırmamak için iki kimliği geçici olarak yan yana taşır:

- `workspace_id`: yeni canonical tenant ve bütün yeni unique/query sınırının ilk alanı.
- `user_id`: R10'a kadar compatibility/actor alanı; yeni tenant kararı veremez.

Yeni workspace satırları aynı provider hesabını kullansa bile farklı workspace'lerde birbirini güncelleyemez. Aynı workspace ve aynı canonical key ise deterministik UPSERT üretir.

## Canlı R3-A sonucu — 23 Eylül 2026

- Dataset V2 satır sayısı `0`; distinct user sayısı `0`.
- Canonical workspace sayısı `1`; workspace currency ayarı sayısı `0`.
- Dataset V2'ye nullable `workspace_id` eklendi; doğrulanmış `workspaces(id)` ilişkisi kuruldu.
- Dört workspace-scoped partial index oluşturuldu; eski unique index ve `user_id` yapısı korundu.
- `backfill_checkpoints` canlıda yok.
- Migration ledger sürümü `20260923083734`; postcheck `PASS`.
- Dataset V2 satır sayısı uygulamadan sonra da `0`; workspace-bound satır sayısı `0`.
- RLS, mevcut authenticated SELECT policy ve rol grant'leri değiştirilmedi.

Bu nedenle belirsiz legacy satır eşleştirmesi veya veri backfill'i yapılmayacaktır.

## R3-A migration davranışı

`20260923083734_add_dataset_v2_workspace_tenant.sql`:

1. Dataset V2'ye nullable `workspace_id` ekler.
2. `workspaces(id)` foreign key'ini `NOT VALID` ekleyip ayrı validate eder.
3. Workspace canonical unique key ve üç workspace query indexini partial olarak oluşturur.
4. Eski `user_id` kolonunu/indexlerini/policy'sini silmez.
5. Veri kopyalamaz, provider'a temas etmez ve `NOT NULL` uygulamaz.

## Backfill checkpoint sapma kararı

Execution Plan başlangıçta mevcut `backfill_checkpoints` tablosuna nullable workspace kolonu ekleneceğini varsayıyordu. Canlıda tablo yoktur ve eski E9 migration'ı ledger'da uygulanmamıştır. Bu yüzden:

- eski user-scoped E9 migration production'a uygulanmayacak;
- workspace checkpoint kimliği için yeni runtime modülleri R3'te hazırlandı;
- fiziksel workspace-scoped checkpoint tablosu R8 aktivasyonunda, lease/control fonksiyonlarıyla tek kontrollü paket olarak oluşturulacak.

## R3-B runtime doğrulaması ve bağımlılık düzeltmesi

Canlı metadata doğrulaması Dataset V2'nin boş olduğunu ve workspace unique indexinin bulunduğunu tekrar doğruladı. Buna karşılık legacy `user_id` kolonu hâlâ `NOT NULL` ve `public.users(id)` foreign key'ine bağlıdır. Shopify embedded workspace'in zorunlu bir Supabase user UUID'si olmadığı için Shopify user ID, email veya yapay bir UUID bu alana yazılamaz.

Bu nedenle canonical contract V2'de:

- `workspace_id` zorunlu tenant anahtarıdır;
- `user_id` yalnız mevcutsa compatibility/actor bilgisidir ve tenant kararı vermez;
- server authority workspace'i satıra bağlar;
- farklı workspace iddiası yazımdan önce reddedilir;
- query filtreleri browser'dan tenant alanı kabul etmez.

Canlı `user_id NOT NULL` değişikliği R3-B'de yapılmadı. Workspace writer aktive edilirken, aynı release kapısında `user_id` nullable geçişi hazırlanıp ayrı production onayıyla uygulanmalıdır. `workspace_id NOT NULL`, legacy policy/index retirement ve nihai R3-C enforcement ise R6 workspace writer kabulünden sonra yapılabilir.

Bu bulgu eski bağımlılık ifadesindeki döngüyü kaldırır: R4, R3-A+B sonrasında açılır; R6, R4-R5 sonrasında çalışır; R3-C ise R6 kabulünden sonra geri dönüp tenant enforcement'ı tamamlar.

## Kabul kapıları

### R3-A canlı additive schema

- ayrı açık production onayı;
- preflight `PASS` ve Dataset V2 satır sayısı `0`;
- migration apply;
- workspace kolon/FK/index postcheck;
- row count değişmediğinin kanıtı;
- security/performance advisor.

Bu kapı tamamlandı. Advisor taramasında R3 foreign key'i için eksik index uyarısı oluşmadı. Dataset boş olduğu için yeni query indexlerinin henüz kullanılmamış görünmesi beklenen bilgi kaydıdır. Mevcut fonksiyon/RLS uyarıları R3-A tarafından üretilmedi ve ayrı güvenlik kapsamındadır.

### R3-C final tenant enforcement

Aşağıdakiler aynı release hattında doğrulanmadan `workspace_id NOT NULL`, eski authenticated policy retirement veya eski unique index kaldırma yapılmaz:

- canonical writer V2 workspace contract kullanıyor;
- server workspace authority doğrulanıyor;
- cross-workspace IDOR negatif testleri geçiyor;
- R6 provider runtime workspace context'e geçmiş;
- null workspace satırı yok;
- rollback/restore noktası hazır.

R3-A'nın `PASS` olması R6 runtime cutover veya provider teması için onay değildir.
