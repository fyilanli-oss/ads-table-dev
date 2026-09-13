# E5-T3 — Google Standard adapter

## İş çıktısı

Google Standard reklam performansı yalnız Ad leaf seviyesinde Dataset V2 canonical adayına dönüşür. Campaign ve AdGroup satırları ayrıca fact olarak yazılmaz; böylece aynı performans üç hierarchy seviyesinde toplanmaz.

Hierarchy `Campaign → AdGroup → Ad` olarak korunur. Google AdGroup, Meta AdSet adına çevrilmez. Performance Max bu adapter tarafından açıkça reddedilir ve E5-T4 kapsamındadır.

E5-T1 conversion count/value sözleşmesi ile E5-T2 customer currency/timezone/business-date sözleşmesi aynı canonical satırda birleşir. Missing metrik `null + unknown`, desteklenmeyen session `null + unsupported`, gerçek sıfır `0 + supported` kalır.

## Production sınırı

Bu paket production Google API çağrısı, Dataset V2 write, runtime delegation veya feature flag açılması yapmaz.
