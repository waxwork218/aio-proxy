---
'@aio-proxy/core': patch
'@aio-proxy/server': patch
'@aio-proxy/types': patch
'aio-proxy': patch
---

Fail over to the next provider when an OpenAI Responses stream returns HTTP 200 and then reports capacity overload (`server_is_overloaded` / `service_unavailable_error`), instead of forwarding that stream and staying on the affinity-selected provider. `server.retry.failoverOnStreamOverload` defaults to on. The same provider is not retried, and `retry_after` is not waited out while another candidate remains. A stream that already produced output is unchanged. If every candidate is overloaded, the last overload stream is still returned to the client.
