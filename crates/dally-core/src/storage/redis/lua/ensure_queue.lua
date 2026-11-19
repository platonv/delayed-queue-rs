-- Returns a queue's hash, creating the queue with the given settings if it
-- does not exist.
-- ARGV: prefix, queue, then the settings as save_settings reads them.

local queue = ARGV[2]
local key = queue_key(queue)

if redis.call('EXISTS', key) == 0 then
  save_settings(queue, 3)
  local now = int(now_ms())
  redis.call('HSET', key, 'created_at', now, 'updated_at', now)
  redis.call('ZADD', queues_key(), 0, queue)
end

return redis.call('HGETALL', key)
