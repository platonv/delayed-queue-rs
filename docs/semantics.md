# Delivery semantics

This page describes what dally promises about delivery, and what it does not.
It applies to every storage backend; the backend pages describe their
durability.

## At least once

Every message is delivered at least once, unless it is cancelled first. It may
be delivered more than once. That happens when:

1. **The worker is slower than the visibility timeout.** The lease expires and
   another worker gets the message while the first one is still working on it.
   The first worker's ack then fails with `409 lease_mismatch`.
2. **The ack is lost.** The worker finishes, but the ack request fails, or the
   worker crashes before sending it. The lease expires and the message is
   delivered again.
3. **A push endpoint does the work but responds too slowly, or with a non-2xx
   status.** dally counts that as a failure and retries.

dally cannot tell these cases apart from a worker that failed, so it retries.
The alternative, at most once, loses work whenever a worker crashes.

## Making handlers safe to run twice

Pick the cheapest that fits:

- **Naturally idempotent work.** "Set the user's plan to pro" is safe to repeat;
  "add 10 credits" is not. Prefer the first kind where you can.
- **A done marker.** Store the message id when the work is done, in the same
  transaction as the work, and skip messages whose id is already stored.
- **Idempotency keys downstream.** Pass the message id as the idempotency key
  to the payment provider, email service or API you call.

## Leases

A poll hands out each message with a lease token. The token is valid until the
message's visibility timeout runs out (30 seconds by default, configurable per
queue and per message). Only the holder of the current token can ack or nack
the message. Each new lease gets a new token, so a worker whose lease expired
can never ack a message that has since gone to someone else.

An ack after the lease expired still succeeds if nobody else has leased the
message in the meantime, because the work is done and redoing it helps nobody.

## Retries and dead messages

- A **nack** ends the attempt. The message is retried after the queue's backoff
  (5 seconds, doubling each time, capped at 1 hour, with ±20% jitter), or after
  the `retry_in` the worker asked for.
- An **expired lease** ends the attempt too, but the message can be leased
  again right away: the worker most likely crashed, so waiting would only add
  latency.
- After `max_attempts` attempts (5 by default), the message becomes **dead**.
  Dead messages keep their last error, stay visible in the API and the UI, and
  can be retried with `POST /v1/queues/{queue}/dead/retry`.

## Ordering

When a poll finds several due messages, the highest `priority` goes first,
then the earliest `run_at`. That is a best effort, not a guarantee:
concurrent workers, retries and expired leases all reorder messages. If you
need strict ordering, a job queue is the wrong tool.

## Idempotency keys

Scheduling with an `idempotency_key` that already exists in the queue does not
create a second message. You get the existing one back with `200 OK` instead of
`201 Created`. Keys are remembered for as long as the message is stored: until
it finishes and the queue's retention period (7 days by default) has passed.

Use them whenever a retried request could schedule the same job twice, which is
almost always: `"welcome-email:user-42"`, `"invoice:2026-11:customer-7"`.

## Time

Due times are measured by the storage clock (PostgreSQL's `now()`, Redis's
`TIME`), not by each server's clock, so several servers agree on what is due.
A message is delivered at or after its `run_at`, usually within a second
because that is how often waiting polls check for messages that became due.
