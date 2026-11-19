-- Marks a running message as succeeded.
-- ARGV: prefix, message id, lease token.
-- Returns the message, or false if it is not running under this lease.

local message = load_message(ARGV[2])
if not message or message.status ~= 'running' or message.lease_token ~= ARGV[3] then
  return false
end

release_lease(message)
finish(message, 'succeeded', now_ms())
return message_reply(message.id)
