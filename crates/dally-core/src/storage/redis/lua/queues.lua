-- Returns {queue hash, status counts} for one queue, or for every queue in
-- name order.
-- ARGV: prefix, and optionally a queue name.

local names
if ARGV[2] then
  names = { ARGV[2] }
else
  names = redis.call('ZRANGE', queues_key(), 0, -1)
end

local summaries = {}
for _, queue in ipairs(names) do
  local fields = redis.call('HGETALL', queue_key(queue))
  if #fields > 0 then
    summaries[#summaries + 1] = { fields, status_counts(queue) }
  end
end
return summaries
