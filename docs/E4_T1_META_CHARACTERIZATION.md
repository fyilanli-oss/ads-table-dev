# E4-T1 — Meta mevcut durum ve hedef alan karakterizasyonu

## İş çıktısı

Meta API'den alınacak hesap, hiyerarşi ve ham performans alanları; Dataset V2'ye girecek gerçekler ile yalnız karşılaştırma kanıtı olarak kalacak alanlar ayrılmıştır. Bu çalışma production verisi çekmez ve runtime davranışını değiştirmez.

## Mevcut davranış

- Hesap keşfi `id`, `name`, `account_status`, `currency`, `timezone_name` ister.
- Insights campaign, adset ve ad seviyelerinde sorgulanır.
- Ortak alanlar `campaign_id`, `campaign_name`, `account_currency`, `impressions`, `reach`, `clicks`, `ctr`, `cpc`, `spend`, `actions`, `action_values`, `cost_per_action_type`, `conversion_rate_ranking`dir.
- AdSet ve Ad sorguları kendi kimlik/ad alanlarını ekler.
- Standard conversion action önce, eşdeğer `omni_*` action fallback olarak kullanılır; alias değerleri toplanmaz.
- Insights satırı yoksa mevcut V1 akışı entity listesinden sıfır/unknown fallback satırı üretir. Bu satır Dataset V2 production fact'i olamaz.
- Mevcut normalizer `abandoned`, `sales`, `revenue` ve `roas` hesaplar. Bunlar Dataset V2 raw fact'i olamaz.

## E4 hedef alan sözleşmesi

| Grup | Meta kaynağı | AdsTable hedefi | Karar |
|---|---|---|---|
| Account | `id`, `name`, `account_status` | Hesap seçimi/ownership | Korunur |
| Time/currency | `timezone_name`, `currency`, `account_currency`, `date_start`, `date_stop` | Business date ve FX | Zorunlu |
| Lineage | Campaign, AdSet, Ad id/name | Campaign → AdSet → Ad | Kayıpsız korunur |
| Delivery | `impressions` | `raw_metrics.impression` | Canonical fact |
| Click | `clicks`, `actions.link_click` | `raw_metrics.ad_click` | İş tanımı kararı gerekli; öneri `link_click` |
| Cost | `spend` | `raw_metrics.spend_value` | Canonical monetary fact |
| ATC | `actions` + `action_values` | count/value | Standard önce, omni fallback |
| Checkout | `actions` + `action_values` | count/value | Standard önce, omni fallback |
| Purchase | `actions` + `action_values` | count/value | Standard önce, omni fallback |
| Diagnostic | `reach`, `ctr`, `cpc`, rankings/cost fields | Parity evidence | Dataset V2 raw fact değil |

## Açık iş kararı

`ad_click` tanımı kesinleştirilmelidir:

- `clicks`: Meta reklamı üzerindeki bütün click etkileşimleri.
- `link_click`: Reklamdan bir bağlantıya yönelen click; Funnel web trafiği anlamına daha yakındır.

**Öneri:** AdsTable Paid Funnel `ad_click = link_click` kullansın. `clicks` yalnız delivery/parity evidence olarak kalsın. Bu karar verilmeden adapter mapping'i production-ready sayılmaz.

## E4 boyunca korunacak sınır

E4-T1 yalnız fixture ve characterization baseline'ıdır. Meta runtime, V1 snapshot, Dataset V2 veya production üzerinde değişiklik yapmaz. Adapter uygulaması sonraki E4 çalışmasında `src/providers/meta` altında yapılır.
