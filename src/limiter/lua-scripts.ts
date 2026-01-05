/**
 * ConcurrentLimiter: Acquire a slot with expiring lock
 * KEYS[1] = limiter key (ZSET of lockId -> expireTime)
 * ARGV[1] = max concurrent
 * ARGV[2] = lock ID (unique per request)
 * ARGV[3] = current time (seconds)
 * ARGV[4] = lock timeout (seconds)
 * Returns: [allowed (0/1), current count, retry_after]
 */
export const LUA_CONCURRENT_ACQUIRE = `
local key = KEYS[1]
local max_concurrent = tonumber(ARGV[1])
local lock_id = ARGV[2]
local now = tonumber(ARGV[3])
local lock_timeout = tonumber(ARGV[4])

-- Remove expired locks
redis.call("ZREMRANGEBYSCORE", key, "-inf", now)

-- Check current count
local current = redis.call("ZCARD", key)
if current < max_concurrent then
  -- Add lock with expiration time as score
  local expire_at = now + lock_timeout
  redis.call("ZADD", key, expire_at, lock_id)
  redis.call("EXPIRE", key, lock_timeout + 10)
  return {1, current + 1, 0}
end

-- Get time until next slot frees up
local oldest = redis.call("ZRANGE", key, 0, 0, "WITHSCORES")
local retry_after = 0
if oldest[2] then
  retry_after = math.max(0, tonumber(oldest[2]) - now)
end
return {0, current, retry_after}
`;

/**
 * ConcurrentLimiter: Release a lock
 * KEYS[1] = limiter key
 * ARGV[1] = lock ID
 */
export const LUA_CONCURRENT_RELEASE = `
redis.call("ZREM", KEYS[1], ARGV[1])
return 1
`;

/**
 * BucketLimiter: Increment counter at time boundary
 * KEYS[1] = limiter key
 * ARGV[1] = max count
 * ARGV[2] = current bucket timestamp (boundary)
 * ARGV[3] = TTL seconds
 * Returns: [allowed (0/1), current count, seconds until reset]
 */
export const LUA_BUCKET_ACQUIRE = `
local key = KEYS[1]
local max_count = tonumber(ARGV[1])
local bucket_ts = ARGV[2]
local ttl = tonumber(ARGV[3])

-- Use hash: field is bucket timestamp, value is count
local current = tonumber(redis.call("HGET", key, bucket_ts) or "0")

if current < max_count then
  redis.call("HINCRBY", key, bucket_ts, 1)
  redis.call("EXPIRE", key, ttl)
  -- Clean old buckets
  local fields = redis.call("HKEYS", key)
  for _, f in ipairs(fields) do
    if f ~= bucket_ts then
      redis.call("HDEL", key, f)
    end
  end
  return {1, current + 1, 0}
end

return {0, current, ttl}
`;

/**
 * WindowLimiter: Sliding window using sorted set
 * KEYS[1] = limiter key (ZSET of request timestamps)
 * ARGV[1] = max count
 * ARGV[2] = window size (seconds)
 * ARGV[3] = current time (seconds, with decimals)
 * ARGV[4] = request ID (unique)
 * Returns: [allowed (0/1), current count, retry_after]
 */
export const LUA_WINDOW_ACQUIRE = `
local key = KEYS[1]
local max_count = tonumber(ARGV[1])
local window_size = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local request_id = ARGV[4]

local window_start = now - window_size

-- Remove entries outside window
redis.call("ZREMRANGEBYSCORE", key, "-inf", window_start)

-- Count entries in window
local current = redis.call("ZCARD", key)

if current < max_count then
  redis.call("ZADD", key, now, request_id)
  redis.call("EXPIRE", key, math.ceil(window_size) + 1)
  return {1, current + 1, 0}
end

-- Calculate retry_after based on oldest entry
local oldest = redis.call("ZRANGE", key, 0, 0, "WITHSCORES")
local retry_after = 0
if oldest[2] then
  retry_after = math.max(0, (tonumber(oldest[2]) + window_size) - now)
end
return {0, current, retry_after}
`;
