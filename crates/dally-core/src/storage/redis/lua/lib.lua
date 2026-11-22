-- Helpers shared by every script. Each script is this file followed by the
-- script's own body, so the helpers are plain locals.
--
-- ARGV[1] is always the key prefix. Keys are built here rather than passed in
-- KEYS because most of them depend on data read inside the script, such as
-- the queue a message belongs to. This is fine on a single Redis server, but
-- it is why dally does not support Redis Cluster.

local prefix = ARGV[1]

-- In the order the Rust side expects counts.
local STATUSES = { 'scheduled', 'running', 'succeeded', 'cancelled', 'dead' }

local function queues_key()
  return prefix .. 'queues'
end

local function queue_key(queue)
  return prefix .. 'queue:' .. queue
end

local function queue_index(queue, name)
  return prefix .. 'queue:' .. queue .. ':' .. name
end

local function status_index(queue, status)
  return queue_index(queue, 'status:' .. status)
end

local function message_key(id)
  return prefix .. 'message:' .. id
end

local function channel()
  return prefix .. 'scheduled'
end

local function now_ms()
  local time = redis.call('TIME')
  return tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
end

-- Formats an integer for storage. Lua's tostring switches to exponent
-- notation above 1e14, which the Rust side would not parse.
local function int(n)
  return string.format('%d', n)
end

-- Members of the ready set share a score per priority, so within a priority
-- they sort by member: zero-padded run_at, then id. The offset keeps run_at
-- non-negative so its digits sort like numbers; 2^52 ms is about 142,000
-- years either side of 1970, and anything beyond is clamped.
local RUN_AT_OFFSET = 2 ^ 52
local ID_START = 18 -- 16 digits and a colon come before the id.

local function ready_member(id, run_at)
  local shifted = math.min(math.max(tonumber(run_at) + RUN_AT_OFFSET, 0), 2 * RUN_AT_OFFSET)
  return string.format('%016d', shifted) .. ':' .. id
end

local function ready_member_id(member)
  return string.sub(member, ID_START)
end

local function load_message(id)
  local fields = redis.call('HGETALL', message_key(id))
  if #fields == 0 then
    return nil
  end
  local message = {}
  for i = 1, #fields, 2 do
    message[fields[i]] = fields[i + 1]
  end
  return message
end

-- What scripts return for a message: its hash as a flat field/value list.
local function message_reply(id)
  return redis.call('HGETALL', message_key(id))
end

local function status_counts(queue)
  local counts = {}
  for i, status in ipairs(STATUSES) do
    counts[i] = redis.call('ZCARD', status_index(queue, status))
  end
  return counts
end

local function set_status(message, status)
  redis.call('ZREM', status_index(message.queue, message.status), message.id)
  redis.call('ZADD', status_index(message.queue, status), 0, message.id)
  redis.call('HSET', message_key(message.id), 'status', status)
  message.status = status
end

-- Every scheduled message enters through the delayed set. Leasing promotes
-- the due ones to the ready set, so only one place computes ready members.
local function enqueue(message, run_at)
  redis.call('ZADD', queue_index(message.queue, 'delayed'), run_at, message.id)
end

local function dequeue(message)
  redis.call('ZREM', queue_index(message.queue, 'delayed'), message.id)
  redis.call('ZREM', queue_index(message.queue, 'ready'), ready_member(message.id, message.run_at))
end

local function release_lease(message)
  redis.call('ZREM', queue_index(message.queue, 'running'), message.id)
  redis.call('HDEL', message_key(message.id), 'lease_token', 'lease_expires_at')
end

local function finish(message, status, now)
  set_status(message, status)
  redis.call('HSET', message_key(message.id), 'finished_at', int(now), 'updated_at', int(now))
  redis.call('ZADD', queue_index(message.queue, 'finished'), now, message.id)
end

-- Writes queue settings taken from ARGV, starting at position `first`:
-- visibility_timeout_ms, max_attempts, backoff_base_ms, backoff_cap_ms,
-- retention_ms, paused (0 or 1), push (JSON, or empty for none).
local function save_settings(queue, first)
  local key = queue_key(queue)
  redis.call('HSET', key,
    'name', queue,
    'visibility_timeout_ms', ARGV[first],
    'max_attempts', ARGV[first + 1],
    'backoff_base_ms', ARGV[first + 2],
    'backoff_cap_ms', ARGV[first + 3],
    'retention_ms', ARGV[first + 4],
    'paused', ARGV[first + 5])
  if ARGV[first + 6] == '' then
    redis.call('HDEL', key, 'push')
  else
    redis.call('HSET', key, 'push', ARGV[first + 6])
  end
end

