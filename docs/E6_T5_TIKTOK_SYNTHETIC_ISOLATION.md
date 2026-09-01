# E6-T5 — TikTok synthetic fallback isolation

## Amaç

Legacy dashboard snapshot akışında boş raporları görselleştirmek için üretilmiş fallback satırlarının canonical production Dataset'e girmesini engellemek. Bu paket legacy dashboard görünümünü silmez; canonical adapter sınırından önce production ve synthetic kaynakları ayırır.

## Sınır

Explicit `synthetic` provenance, `fallback_reason`, fallback source-confidence ve fallback kimlik/status marker'ları taşıyan satırlar production listesinden çıkarılır. Boundary yalnız güvenli sayısal evidence döndürür: source, production ve isolated synthetic satır sayıları ile canonical'a yazılan synthetic sayısının daima sıfır olduğu bilgisi.

Synthetic-only input boş canonical sonuç üretir; ölçülmüş sıfır satırı üretmez. Kalan gerçek satırlar E6-T4 delivery mapper'a gider ve orada yeniden sentetik/non-leaf kontrolünden geçer.

Bu paket Dataset V2 write veya runtime flag activation yapmaz; E6-T6 wiring öncesi fail-closed isolation boundary'sini hazırlar.
