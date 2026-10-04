# A6-RM-00 — Release ve Durum Emniyet Kapıları

**Karar tarihi:** 4 Ekim 2026  
**Durum:** Decision frozen / active containment  
**Runtime etkisi:** Yok  
**Production mutation:** Yok

## İş çıktısı

Repository genel auditinde bulunan riskler çözülmeden paketlerin yanlışlıkla `Done`, `PASS`, `review-ready`, cutover-safe veya rollback-safe ilan edilmesini engelleyen bağlayıcı kapılar kurulmuştur.

Bu paket ürün özelliği tamamlamaz. Amacı, eksik kanıtı statü değişikliğiyle görünmez hâle getirmemektir.

## Neden gerekli?

A3 auditi, E10-T4'ün foundation testleri bulunmasına rağmen production webhook/lifecycle zinciri olmadan `Done` göründüğünü kanıtladı. A4 ve A5 ise çalışan bir legacy ekranın aynı zamanda doğru ve güvenli rollback hedefi olmadığını gösterdi. Bu nedenle kodun varlığı, testin geçmesi, PR merge'i veya deployment tek başına kabul değildir.

## Aktif HOLD kapıları

1. **Shopify review-ready:** RM-01–RM-10 ile R6/R7/R8 kritik kabulü tamamlanmadan açılamaz.
2. **Delete My Data complete:** RM-03 ve RM-04 uçtan uca kanıtlanmadan açılamaz.
3. **Clean reinstall safe:** RM-03–RM-05 tamamlanmadan açılamaz.
4. **Truthful Funnel cutover:** RM-02 ve RM-06–RM-09 tamamlanmadan açılamaz.
5. **Safe rollback/R9:** RM-02 ve RM-10 failure-injection kanıtı olmadan açılamaz.
6. **Legacy retirement:** RM-10 stabilizasyonu, RM-11 consumer-zero ve ayrı destructive approval olmadan açılamaz.

## Statü değiştirmeyen olaylar

Aşağıdakilerin hiçbiri tek başına `PASS` veya `Done` üretmez:

- Kodun repository'ye eklenmesi
- Unit testlerin geçmesi
- PR'ın merge edilmesi
- Deployment'ın başarılı olması
- Zaman geçmesi
- Bir development-store senaryosunun diğer lifecycle senaryoları yerine kullanılması

## Review kritik yolu

Mevcut anayasal sıraya göre review-ready kapısı RM-01–RM-10'u ve R6/R7/R8'in gerekli canlı kabulünü taşır. RM-11 legacy retirement ve RM-12 ikinci-adapter readiness review öncesi zorunlu değildir.

R8'in review öncesi zorunluluğu ancak ayrı, versionlı bir kararla kaldırılabilir; bu belge kendiliğinden o kararı vermez.

## Kabul

RM-00 şu koşullarla tamamlanır:

- Her HOLD kapısı contract'ta machine-readable olarak bulunur.
- Her kapı exact upstream paket ve evidence şartı taşır.
- Gelecekteki paket statüleri bu kapılara referans verir.
- Hiçbir runtime, migration, provider, deployment veya deletion işlemi yapılmaz.

## Bağlayıcı contract

`contracts/a6-rm-00-release-status-containment-v1.json`
