counter = 0

request = function()
  counter = counter + 1
  local method = "POST"
  local body   = "{documentSelfLink:'"..counter.."', message:'"..counter.."'}"
  return wrk.format(method, nil, nil, body)
end

-- response = function()
--   if counter == 5 then
--     wrk.thread:stop()
--   end
-- end
