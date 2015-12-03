counter = 0

request = function()
  counter = counter + 1
  local method = "PATCH"
  local body   = "{message:'"..counter.."'}"
  return wrk.format(method, nil, nil, body)
end

-- response = function()
--   if counter == 1 then
--     wrk.thread:stop()
--   end
-- end
