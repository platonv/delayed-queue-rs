-- Records a failed attempt: the message is retried after a delay, or is dead
-- if it has no attempts left.
-- ARGV: prefix, message id, lease token, retry delay in ms, and optionally
-- the error text.
-- Returns the message, or false if it is not running under this lease.

local message = load_message(ARGV[2])
if not message or message.status ~= 'running' or message.lease_token ~= ARGV[3] then
  return false
end

local key = message_key(message.id)
local now = now_ms()
release_lease(message)
if ARGV[5] then
  redis.call('HSET', key, 'last_error', ARGV[5])
else
  redis.call('HDEL', key, 'last_error')
end

if tonumber(message.attempts) >= tonumber(message.max_attempts) then
  finish(message, 'dead', now)
else
  local run_at = now + tonumber(ARGV[4])
  set_status(message, 'scheduled')
  redis.call('HSET', key, 'run_at', int(run_at), 'updated_at', int(now))
  enqueue(message, run_at)
end
return message_reply(message.id)
