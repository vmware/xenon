-- generate a mixed workload (default 70/30 read-heavy)

counter = 0
read_mode = true

request = function()
  local isRead = function ()
    return math.random() < 0.7 -- TEMPLATE:RAND
  end
  counter = counter + 1
  local method =  isRead() and "GET" or "PUT"
  local body = (method == "GET") and "" or ("{message: '" .. counter .. "'}")
  --print("-- " .. method .. ":" .. body) -- TEMPLATE:DELETE
  return wrk.format(method, nil, nil, body)
end

-- response = function()
--  if counter == 5 then
--    wrk.thread:stop()
--  end
-- end
