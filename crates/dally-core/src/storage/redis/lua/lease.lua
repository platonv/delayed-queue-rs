-- Leases due messages from a queue, highest priority first, then oldest
-- run_at first.
-- ARGV: prefix, queue, then one fresh lease token per message to lease.
-- Returns the leased messages.

-- How many delayed messages one call promotes to the ready set. Bounds the
-- work of a single call after a large batch becomes due at once; the rest
-- are promoted by the following polls.
local PROMOTE_LIMIT = 1000

local queue = ARGV[2]
local limit = #ARGV - 2
local now = now_ms()
local running = queue_index(queue, 'running')
local delayed = queue_index(queue, 'delayed')
local ready = queue_index(queue, 'ready')

local function make_ready(message)
  redis.call('ZADD', ready, -tonumber(message.priority), ready_member(message.id, message.run_at))
end

-- Expired leases: the worker probably crashed. Retry right away if attempts
-- are left, otherwise the message is dead.
for _, id in ipairs(redis.call('ZRANGE', running, '-inf', now, 'BYSCORE')) do
  local message = load_message(id)
  release_lease(message)
  redis.call('HSET', message_key(id), 'last_error', 'lease expired', 'updated_at', int(now))
  if tonumber(message.attempts) >= tonumber(message.max_attempts) then
    finish(message, 'dead', now)
  else
    set_status(message, 'scheduled')
    make_ready(message)
  end
end

for _, id in ipairs(redis.call('ZRANGE', delayed, '-inf', now, 'BYSCORE', 'LIMIT', 0, PROMOTE_LIMIT)) do
  redis.call('ZREM', delayed, id)
  make_ready(load_message(id))
end

local leased = {}
for i, member in ipairs(redis.call('ZRANGE', ready, 0, limit - 1)) do
  local id = ready_member_id(member)
  local message = load_message(id)
  local expires = now + tonumber(message.visibility_timeout_ms)

  redis.call('ZREM', ready, member)
  set_status(message, 'running')
  redis.call('HSET', message_key(id),
    'attempts', int(tonumber(message.attempts) + 1),
    'lease_token', ARGV[2 + i],
    'lease_expires_at', int(expires),
    'updated_at', int(now))
  redis.call('ZADD', running, expires, id)

  leased[#leased + 1] = message_reply(id)
end
return leased
