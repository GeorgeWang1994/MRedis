local cache_key = KEYS[1]
local event_key = KEYS[2]
local result = redis.call('get', cache_key)
if result then
    redis.call('lpush', event_key)
    redis.call('ltrim', event_key, 0, 0)
    return redis.call('del', cache_key)
else
    return 0
end
