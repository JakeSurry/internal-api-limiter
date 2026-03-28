-- Get args from redis.eval()
local now_ms      = tonumber(ARGV[1])
local num_rules   = #KEYS / 2

-- Init locals
local min_wait_ms = 0
local computed    = {}

-- Read pass: find number of tokens remaining for all rules
for i = 0, (num_rules - 1) do
    local tok_key     = KEYS[i * 2 + 1]
    local refill_key  = KEYS[i * 2 + 2]
    local limit       = tonumber(ARGV[i * 2 + 2])
    local window_ms   = tonumber(ARGV[i * 2 + 3])
    local tokens      = tonumber(redis.call('GET', tok_key))
    local first_refill= tonumber(redis.call('GET', refill_key))
    local rate        = limit / window_ms

    -- Set tokens to limit if key doesn't exist yet (as on first pass or new user)
    if tokens == nil and first_refill == nil then
        tokens       = limit
        first_refill = now_ms 
    end

    if tokens == 0 then
        if now_ms - first_refill >= window_ms then
            tokens = limit
            first_refill = now_ms
        end
    end

    computed[i+1] = {
        tok_key      = tok_key,
        refill_key   = refill_key,
        tokens       = tokens,
        window_ms    = window_ms,
        first_refill = first_refill
    }
    
    -- Determine how long it will take for all rules to refill
    if tokens == 0 then
        local wait = now_ms - first_refill
        if wait > min_wait_ms then 
            min_wait_ms = wait 
        end
    end
end

-- Write pass: Update token states and consume token if able
if min_wait_ms > 0 then
    for i = 1, num_rules do
        local c = computed[i]
        redis.call('SET', c.tok_key, tostring(c.tokens))
        redis.call('SET', c.refill_key, tostring(c.first_refill))
        redis.call('PEXPIRE', c.tok_key, c.window_ms + 5000)
        redis.call('PEXPIRE', c.refill_key, c.window_ms + 5000)
    end
    -- Return wait-time for all rules to have >= 1 token
    return min_wait_ms
end

for i = 1, num_rules do
    local c = computed[i]
    redis.call('SET', c.tok_key, tostring(c.tokens - 1))
    redis.call('SET', c.refill_key, tostring(c.first_refill))
    redis.call('PEXPIRE', c.tok_key, c.window_ms + 5000)
    redis.call('PEXPIRE', c.refill_key, c.window_ms + 5000)
end

-- Return 0 as there is no need to wait for tokens
return 0
