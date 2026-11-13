---
name: UK request scope
description: Latest user direction supersedes hub-term requests with minimal country-wide GraphQL inputs.
---

The user superseded the earlier hub-term approach: job inputs must contain only country "United Kingdom", locale "en-GB", and optional nextToken; schedule inputs additionally contain jobId.

**Why:** The user reported GraphQL rejecting undefined SearchJobRequest fields and explicitly requested removing query and other extra fields.

**How to apply:** Do not reintroduce hub terms or unsupported filters. Preserve request pacing, cooldowns, independent loops, and the notification queue. Do not claim live schema compatibility or complete UK coverage from offline mocks alone.
