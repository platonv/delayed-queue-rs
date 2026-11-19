-- Deletes finished messages older than their queue's retention.
-- ARGV: prefix.
-- Returns how many messages were deleted.

local now = now_ms()
local deleted = 0

for _, queue in ipairs(redis.call('ZRANGE', queues_key(), 0, -1)) do
  local retention = tonumber(redis.call('HGET', queue_key(queue), 'retention_ms'))
  local finished = queue_index(queue, 'finished')
  local cutoff = '(' .. int(now - retention)

  for _, id in ipairs(redis.call('ZRANGE', finished, '-inf', cutoff, 'BYSCORE')) do
    local message = load_message(id)
    if message then
      if message.idempotency_key then
        redis.call('HDEL', queue_index(queue, 'keys'), message.idempotency_key)
      end
      redis.call('ZREM', status_index(queue, message.status), id)
      redis.call('DEL', message_key(id))
    end
    redis.call('ZREM', finished, id)
    deleted = deleted + 1
  end
end
return deleted
