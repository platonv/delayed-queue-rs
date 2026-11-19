-- Schedules a batch of messages in one queue. A message whose idempotency
-- key is already used in the queue is not stored; the existing one is
-- returned instead.
-- ARGV: prefix, queue, then eight values per message: id, payload, priority,
-- max_attempts, visibility_timeout_ms, idempotency_key ('' for none),
-- run_at in ms ('' to use the delay), delay_ms.
-- Returns {created (0 or 1), message} for each message, in order.

local FIELDS = 8
local queue = ARGV[2]
local keys = queue_index(queue, 'keys')
local now = now_ms()
local results = {}
local created_any = false

for i = 3, #ARGV, FIELDS do
  local id, payload, priority, max_attempts, visibility, key, at, delay =
    unpack(ARGV, i, i + FIELDS - 1)

  local existing = key ~= '' and redis.call('HGET', keys, key)
  if existing then
    results[#results + 1] = { 0, message_reply(existing) }
  else
    local run_at = now + tonumber(delay)
    if at ~= '' then
      run_at = tonumber(at)
    end

    redis.call('HSET', message_key(id),
      'id', id,
      'queue', queue,
      'payload', payload,
      'status', 'scheduled',
      'priority', priority,
      'attempts', '0',
      'max_attempts', max_attempts,
      'visibility_timeout_ms', visibility,
      'run_at', int(run_at),
      'created_at', int(now),
      'updated_at', int(now))
    if key ~= '' then
      redis.call('HSET', message_key(id), 'idempotency_key', key)
      redis.call('HSET', keys, key, id)
    end
    redis.call('ZADD', status_index(queue, 'scheduled'), 0, id)
    enqueue({ id = id, queue = queue }, run_at)

    created_any = true
    results[#results + 1] = { 1, message_reply(id) }
  end
end

if created_any then
  redis.call('PUBLISH', channel(), queue)
end
return results
