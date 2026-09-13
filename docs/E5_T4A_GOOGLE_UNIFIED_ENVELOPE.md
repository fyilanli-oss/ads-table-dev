# E5-T4A — Google Standard/PMax ortak envelope

## İş çıktısı

Google provider sınırının tek adapter girişi vardır. Caller açıkça `standard` veya `performance_max` campaign type seçer; adapter doğru provider fetch/mapping yoluna delege eder. Bilinmeyen veya eksik tür fail-closed olur.

Her iki yol aynı canonical yedi blok, aynı on raw metric ve aynı metric support anahtarlarını döndürür. Ayrı Google Standard/PMax veri şeması yoktur.

| Capability | Standard | Performance Max |
|---|---|---|
| Root | Campaign | Campaign |
| Parent | AdGroup | Yok (`null`) |
| Leaf | Ad | Asset Group |
| Campaign type | `standard` | `performance_max` |

Provider farkları yalnız bu capability/entity değerlerinde kalır. PMax için fake AdGroup/Ad, Standard için Asset Group kabul edilmez.

## Production sınırı

Bu paket production Google API çağrısı, Dataset V2 write, runtime delegation veya feature flag açılması yapmaz.
