# test for Cloudflare service token auth via admin.gr8r
curl.exe -i "https://admin.gr8r.com/db1/videos" `
  -H "CF-Access-Client-Id: <ID>" `
  -H "CF-Access-Client-Secret: <SECRET>" `
  -H "Authorization: Bearer <DB1_INTERNAL_KEY>"
