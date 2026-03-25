local now_ms    = tonumber(ARGV[1])
local num_rules = #KEYS / 2

local min_wait_ms = 0
local computed    = {}

-- Read pass: find number of tokens remaining for all rules
for i = 1, num_rules do
    local tok_key    = KEYS[(i - 1) * 2 + 1]
    local refill_key = KEYS[(i - 1) * 2 + 2]
    local limit      = tonumber(ARGV[1 + (i - 1) * 2 + 1])
    local window_ms  = tonumber(ARGV[1 + (i - 1) * 2 + 2])
    local rate       = limit / window_ms

    local tokens = tonumber(redis.call('GET', tok_key))
    if tokens == nil then tokens = limit end

    local last_refill = tonumber(redis.call('GET', refill_key))
    if last_refill == nil then last_refill = now_ms end

    local elapsed = now_ms - last_refill
    if elapsed > 0 then
        tokens = math.min(limit, tokens + elapsed * rate)
    end

    computed[i] = {
        tok_key    = tok_key,
        refill_key = refill_key,
        tokens     = tokens,
        window_ms  = window_ms,
        rate       = rate,
    }

    if tokens < 1 then
        local wait = math.ceil((1 - tokens) / rate)
        if wait > min_wait_ms then min_wait_ms = wait end
    end
end

-- Write pass: Update token states and consume token if able
if min_wait_ms > 0 then
    for i = 1, num_rules do
        local c = computed[i]
        redis.call('SET', c.tok_key, tostring(c.tokens))
        redis.call('SET', c.refill_key, tostring(now_ms))
        redis.call('PEXPIRE', c.tok_key, c.window_ms + 5000)
        redis.call('PEXPIRE', c.refill_key, c.window_ms + 5000)
    end
    return min_wait_ms
end

for i = 1, num_rules do
    local c = computed[i]
    redis.call('SET', c.tok_key, tostring(c.tokens - 1))
    redis.call('SET', c.refill_key, tostring(now_ms))
    redis.call('PEXPIRE', c.tok_key, c.window_ms + 5000)
    redis.call('PEXPIRE', c.refill_key, c.window_ms + 5000)
end

return 0
