-- Moves dead messages back to scheduled with a fresh set of attempts, due
-- now.
-- ARGV: prefix, queue, then the ids to retry; with no ids, every dead
-- message in the queue.
-- Returns how many messages were moved.

local queue = ARGV[2]
local ids = {}
if #ARGV > 2 then
  for i = 3, #ARGV do
    ids[#ids + 1] = ARGV[i]
  end
else
  ids = redis.call('ZRANGE', status_index(queue, 'dead'), 0, -1)
end

local now = now_ms()
local moved = 0
for _, id in ipairs(ids) do
  local message = load_message(id)
  if message and message.queue == queue and message.status == 'dead' then
    redis.call('ZREM', queue_index(queue, 'finished'), id)
    redis.call('HDEL', message_key(id), 'finished_at')
    set_status(message, 'scheduled')
    redis.call('HSET', message_key(id), 'attempts', '0', 'run_at', int(now), 'updated_at', int(now))
    enqueue(message, now)
    moved = moved + 1
  end
end

if moved > 0 then
  redis.call('PUBLISH', channel(), queue)
end
return moved
