---
name: UK hub-search direction
description: User-requested change from blank nationwide queries to supplied fulfillment-hub search terms.
---

The user requested discovery using their supplied UK fulfillment-hub search terms with gentle request pacing, replacing blank queries.

**Why:** The user supplied replacement production source after reporting GraphQL input failures. This changes the earlier maximum-speed, unfiltered-feed direction.

**How to apply:** Preserve the shared notification queue and independent loops while integrating this approach. Do not claim that hub-term searches cover every UK listing, or that the GraphQL rejection is fixed, without successful request/response evidence. The supplied source does not demonstrate a paginated response contract.
