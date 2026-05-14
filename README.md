# AdsTable Stabilization Build v1

Upload/replace:
- server.js
- package.json
- vercel.json
- public/dashboard.html
- public/dashboard-demo.html
- sql/stabilization_foundation.sql

Optional: keep existing landing/login/signup if already working.

Before testing account discovery, run:
sql/stabilization_foundation.sql

New endpoints:
- /api/unified/status
- /api/debug/connections
- POST /api/connections/:platform/disconnect
- /api/meta/adaccounts
- /api/google/customers
- /api/pinterest/adaccounts
- /api/accounts
