# Cryptographic test data

`c2sp_chunked_encryption_aes_256_gcm_test.json` is the unmodified
Cobblestone-256 corpus from C2SP Project Wycheproof commit
`b61843a9a5115bb758134b6a1f5d5e502d445342`:

`testvectors_v1/c2sp_chunked_encryption_aes_256_gcm_test.json`

Its SHA-256 digest is
`0f8e35568d59649fb55fb0e5602dc678dc8910d6822beef4945aee0b5994ef05`.
The test asserts that digest before parsing the fixture. As specified by the
Wycheproof application notes, each `ct` field is hex-decoded and then
zlib-decompressed before use.
