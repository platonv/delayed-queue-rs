-- Cancels a scheduled message.
-- ARGV: prefix, message id.
-- Returns the message, or false if it is not scheduled.

local message = load_message(ARGV[2])
if not message or message.status ~= 'scheduled' then
  return false
end

dequeue(message)
finish(message, 'cancelled', now_ms())
return message_reply(message.id)
