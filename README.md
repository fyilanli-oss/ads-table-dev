# AdsTable Google Ready Build

Upload these files to GitHub and let Vercel deploy.

Required Vercel env:
- SESSION_SECRET
- META_APP_ID
- META_APP_SECRET
- META_REDIRECT_URI
- GOOGLE_CLIENT_ID
- GOOGLE_CLIENT_SECRET
- GOOGLE_REDIRECT_URI
- GOOGLE_DEVELOPER_TOKEN
- GOOGLE_LOGIN_CUSTOMER_ID (optional, manager account ID no dashes)
- GOOGLE_ADS_API_VERSION (default v19)

Test:
- /auth/google
- /api/google/status
- /api/google/customers
- /api/google/insights?customerId=YOUR_ID
