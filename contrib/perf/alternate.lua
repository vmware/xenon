--Alternate between read and write
--
counter = 0
read_mode = true

request = function()
  counter = counter + 1
  read_mode = not read_mode
  local method = read_mode and "GET" or "PUT"
  local body = read_mode and "" or ("{message: '" .. counter .. "'}")
  print("-- " .. method .. ":" .. body) -- TEMPLATE:DELETE
  return wrk.format(method, nil, nil, body)
end

-- response = function()
--  if counter == 2 then
--    wrk.thread:stop()
--  end
-- end
