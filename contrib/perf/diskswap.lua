-- create a complex pipeline of requests
-- cycling through a huge number of services
-- so that we force swap to disk

-- N.b.: assumes services have been created externally
services = {}

init = function(args)
  -- cycle through 1M services 10 times (TODO: more than 10)
  -- this just prepares the requests in memory
  for i= 1, 10, 1 do
    for s= 1, 1000000, 1 do
      services[s + i] = wrk.format(nil, ("/" .. s))
    end
  end
  req = table.concat(services)
end

request = function()
  return req
end

counter = 0
-- for testing: receive only 10 responses
response = function()
  if counter == 10 then
    wrk.thread:stop()
  end
  counter = counter + 1
end
