# E9-T2 — Resumable checkpoint

Her iş birimi `user + platform + account + business_date + date_key` kimliğiyle tek checkpoint taşır. Database unique constraint aynı birimin ikinci kez oluşturulmasını engeller. Yesterday ve Today ayrı checkpoint'tir; Yesterday tamamlanınca Today beklenmeden terminal hale gelebilir.

Durumlar `queued`, `running`, `completed`, `failed`, `skipped` olarak sabittir. `completed` ve `skipped` terminaldir ve yeniden çalıştırılmaz. `queued`, yarıda kalmış `running` ve `failed` birimler devam adayıdır. Provider cursor yalnız opaque metin olarak, en fazla 4096 karakter saklanır; kullanıcı girdisi veya provider payload'ı olarak yorumlanmaz. Hata ayrıntısı yerine yalnız güvenli `error_code` tutulur.

Tablo forced-RLS ve service-role-only'dir. Bu görev checkpoint sözleşmesi ve persistence şemasını hazırlar; production migration/backfill çalıştırmaz. Atomic worker leasing, provider quota/retry ile birlikte E9-T3'te bağlanacaktır.
