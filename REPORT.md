# OSS: mask sign-in and OAuth app secrets under HIDE_SECRET_CONFIG, reveal on request

Branch `feat/oss-mask-auth-oauth-secrets` on upstream/main 3395a2581. Stack: OSS app image built
from the branch, Neo4j, Redis, Qdrant, Mongo, Mailpit, mock LLM. All secrets are test strings.

| File | What it shows |
|---|---|
| raw/B0-before-upstream-main.txt | upstream/main with HIDE_SECRET_CONFIG=true returns sign-in, connector OAuth app and action credentials unmasked |
| raw/M1-hidden.txt | branch, HIDE_SECRET_CONFIG=true: 29/29 (masked by default, ?reveal=true returns them, masked save keeps stored values) |
| raw/M2-flag-off.txt | branch, HIDE_SECRET_CONFIG=false: 24/24 (admins get stored values by default, reveal still works) |
| raw/M3-sign-in-uses-real-values.txt | flag on: admin GET masked, sign-in page still gets the real Google client id |
| raw/T1-unit-tests.txt | unit, lint and type-check results |
| screenshots/01-07 | UI before/after "Show stored values" |
