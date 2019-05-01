local cache_key = KEYS[1]
local ttl = tonumber(KEYS[2])
local left_ttl = redis.call('ttl', cache_key)
if left_ttl > 0 then
    redis.call('expire', cache_key, left_ttl + ttl)
    return 1
else
    return 0
end
