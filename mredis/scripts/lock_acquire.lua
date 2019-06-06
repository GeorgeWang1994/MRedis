local cache_key = KEYS[1]
local cache_value = ARGV[1]
local ttl = tonumber(ARGV[2])
local result = redis.call('setnx', cache_key, cache_value)
if result == 1 and ttl > 0 then
    redis.call('expire', cache_key, ttl)
end
return result
