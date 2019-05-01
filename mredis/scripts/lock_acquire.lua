local cache_key = KEYS[1]
local cache_value = ARGS[1]
local ttl = tonumber(ARGS[2])
local result = redis.call('setnx', cache_key, cache_value)
if result == cache_value and ttl > 0 then
    redis.call('expire', cache_key, ttl)
end
return result
