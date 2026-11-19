-- Changes when a scheduled message runs.
-- ARGV: prefix, message id, run_at in ms ('' to use the delay), delay in ms.
-- Returns the message, or false if it is not scheduled.

local message = load_message(ARGV[2])
if not message or message.status ~= 'scheduled' then
  return false
end

local now = now_ms()
local run_at = now + tonumber(ARGV[4])
if ARGV[3] ~= '' then
  run_at = tonumber(ARGV[3])
end

-- Dequeue before changing run_at: the ready set's member is built from it.
dequeue(message)
redis.call('HSET', message_key(message.id), 'run_at', int(run_at), 'updated_at', int(now))
enqueue(message, run_at)
redis.call('PUBLISH', channel(), message.queue)
return message_reply(message.id)
