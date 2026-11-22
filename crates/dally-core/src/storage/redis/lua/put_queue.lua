-- Creates a queue or replaces its settings, and returns its hash.
-- ARGV: prefix, queue, then the settings as save_settings reads them.

local queue = ARGV[2]
local key = queue_key(queue)
local now = int(now_ms())

save_settings(queue, 3)
redis.call('HSETNX', key, 'created_at', now)
redis.call('HSET', key, 'updated_at', now)
redis.call('ZADD', queues_key(), 0, queue)

-- Resuming a queue makes its messages deliverable again.
if ARGV[8] == '0' then
  redis.call('PUBLISH', channel(), queue)
end

return redis.call('HGETALL', key)
