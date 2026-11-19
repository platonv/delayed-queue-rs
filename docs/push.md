# Push delivery

With push delivery, dally sends each message to your app as an HTTP request,
so you don't need to run a worker. A background job handler becomes one more
endpoint in the web app you already have.

## Setting it up

Give a queue a push endpoint, in the config file:

```toml
[queues.emails.push]
url = "https://app.example.com/jobs/emails"
secret = "a long random string"   # optional, but recommended
timeout = "30s"                   # how long to wait for a response
concurrency = 8                   # requests in flight at once, per server
```

or at runtime through the API:

```sh
curl -X PUT localhost:7070/v1/queues/emails \
  -d '{"push": {"url": "https://app.example.com/jobs/emails", "secret": "..."}}'
```

Changes made through the API are picked up within a few seconds. Pausing the
queue stops deliveries until it is resumed.

## What your endpoint receives

```http
POST /jobs/emails HTTP/1.1
Content-Type: application/json
Dally-Message-Id: 0193b0f2-4c1e-7d2a-9f3e-6a1b2c3d4e5f
Dally-Queue: emails
Dally-Attempt: 1
Dally-Signature: t=1763550000,v1=6f1c...

{"to": "ana@example.com", "template": "welcome"}
```

The body is the message's payload, exactly as it was scheduled.
`Dally-Attempt` counts from 1, so a handler can tell a retry from a first try.

## What your endpoint should return

- **Any 2xx** means the work is done, and the message is acked.
- **Anything else**, a timeout or a connection error means it failed. dally
  retries with the queue's backoff and marks the message dead after
  `max_attempts`. The status and the first 1 KiB of the response body become
  the message's `last_error`, so a short explanation in the body shows up in
  the API and the UI.
- **429 or 503 with `Retry-After: <seconds>`** asks dally to retry after that
  delay instead of the usual backoff.

Respond within the queue's `timeout`, and keep it below the queue's
`visibility_timeout`. Otherwise the message can be delivered again while the
first request is still running. For long jobs, accept the request, do the work
elsewhere, and schedule a follow-up message when it is done.

## Delivered at least once

Push has the same guarantee as polling: every message arrives at least once,
and sometimes more than once (see [semantics.md](semantics.md)). If your
handler finishes the work but the response never reaches dally, the message is
delivered again. Use `Dally-Message-Id` to recognise duplicates.

## Verifying the signature

When the queue has a `secret`, every request carries a `Dally-Signature`
header:

```text
Dally-Signature: t=<unix seconds>,v1=<hex HMAC-SHA256 of "<t>.<body>">
```

To verify it:

1. Read the raw request body before parsing it. Any change, even
   re-serializing the JSON, breaks the signature.
2. Compute the HMAC-SHA256 of the timestamp, a dot and the body, keyed with
   the secret.
3. Compare it with each `v1` value in constant time.
4. Reject timestamps more than five minutes from your clock. That stops a
   captured request from being replayed later.

### Rust

`dally-server` exports the same function dally uses to check its own tests:

```rust
use std::time::Duration;

use dally_server::signature;

let now = chrono::Utc::now().timestamp();
signature::verify(secret, header, &body, now, Duration::from_secs(300))?;
```

### Python

```python
import hashlib
import hmac
import time


def verify(secret: bytes, header: str, body: bytes, tolerance: int = 300) -> bool:
    timestamp, signatures = None, []
    for part in header.split(","):
        key, _, value = part.strip().partition("=")
        if key == "t":
            timestamp = value
        elif key == "v1":
            signatures.append(value)

    if timestamp is None or not timestamp.isdigit():
        return False
    if abs(time.time() - int(timestamp)) > tolerance:
        return False

    expected = hmac.new(secret, timestamp.encode() + b"." + body, hashlib.sha256).hexdigest()
    return any(hmac.compare_digest(expected, signature) for signature in signatures)
```

### TypeScript (Node)

```ts
import { createHmac, timingSafeEqual } from "node:crypto";

export function verify(
  secret: string,
  header: string,
  body: Buffer,
  toleranceSeconds = 300,
): boolean {
  let timestamp: string | undefined;
  const signatures: string[] = [];
  for (const part of header.split(",")) {
    const [key, value] = part.trim().split("=", 2);
    if (key === "t") timestamp = value;
    else if (key === "v1" && value) signatures.push(value);
  }

  if (!timestamp || !/^\d+$/.test(timestamp)) return false;
  if (Math.abs(Date.now() / 1000 - Number(timestamp)) > toleranceSeconds) return false;

  const expected = createHmac("sha256", secret).update(`${timestamp}.`).update(body).digest();
  return signatures.some((signature) => {
    const given = Buffer.from(signature, "hex");
    return given.length === expected.length && timingSafeEqual(given, expected);
  });
}
```

With Express, use `express.raw({ type: "application/json" })` on the route so
`req.body` is the raw `Buffer`.

### Go

```go
import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"strings"
	"time"
)

func Verify(secret []byte, header string, body []byte, tolerance time.Duration) bool {
	var timestamp string
	var signatures []string
	for _, part := range strings.Split(header, ",") {
		key, value, _ := strings.Cut(strings.TrimSpace(part), "=")
		switch key {
		case "t":
			timestamp = value
		case "v1":
			signatures = append(signatures, value)
		}
	}

	seconds, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil {
		return false
	}
	if age := time.Since(time.Unix(seconds, 0)); age > tolerance || age < -tolerance {
		return false
	}

	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(timestamp + "."))
	mac.Write(body)
	expected := mac.Sum(nil)
	for _, signature := range signatures {
		if given, err := hex.DecodeString(signature); err == nil && hmac.Equal(given, expected) {
			return true
		}
	}
	return false
}
```

## Rotating the secret

The header format allows several `v1` values, and every verifier above accepts
a match on any of them. To rotate without downtime, deploy receivers that
accept both the old and the new secret, then change the queue's `secret`, and
finally remove the old one from the receivers.
