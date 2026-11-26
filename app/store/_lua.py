"""Atomic Lua scripts for ``RedisGenerationStore`` (I3, I4, fencing, and TTLs)."""

# Atomically checks terminal state and optional fence token, assigns index, appends token, sets TTL.
APPEND_TOKEN_LUA = """
if redis.call('EXISTS', KEYS[2]) == 1 then
  return {0, 'CLOSED'}
end
if ARGV[3] ~= '' then
  if redis.call('EXISTS', KEYS[4]) == 0 then
    return {0, 'STALE_FENCE'}
  end
  local current_fence = redis.call('GET', KEYS[3])
  if not current_fence or tonumber(current_fence) ~= tonumber(ARGV[3]) then
    return {0, 'STALE_FENCE'}
  end
end
local index = redis.call('XLEN', KEYS[1])
local entry_id = redis.call(
  'XADD', KEYS[1], '*',
  'kind', 'token',
  'index', tostring(index),
  'data', ARGV[1]
)
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2]))
return {1, tostring(index), entry_id}
"""

# Atomically checks optional fence, records terminal state with TTL, and wakes XREAD readers.
MARK_TERMINAL_LUA = """
if ARGV[5] ~= '' then
  if redis.call('EXISTS', KEYS[4]) == 0 then
    return {0, 'STALE_FENCE'}
  end
  local current_fence = redis.call('GET', KEYS[3])
  if not current_fence or tonumber(current_fence) ~= tonumber(ARGV[5]) then
    return {0, 'STALE_FENCE'}
  end
end
if redis.call('EXISTS', KEYS[2]) == 1 then
  return {0, 'CLOSED'}
end
local len = redis.call('XLEN', KEYS[1])
local last_index = cjson.null
if len > 0 then
  last_index = len - 1
end
local reason = ARGV[2]
if ARGV[3] == '0' then
  reason = cjson.null
end
local payload = cjson.encode({status = ARGV[1], reason = reason, last_index = last_index})
redis.call('SET', KEYS[2], payload, 'EX', tonumber(ARGV[4]))
redis.call('XADD', KEYS[1], '*', 'kind', 'terminal', 'status', ARGV[1])
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[4]))
return {1, payload}
"""

# Single-producer election (I3): acquire lease only if absent, bumping monotonic fence with TTL.
ACQUIRE_LEASE_LUA = """
if redis.call('EXISTS', KEYS[1]) == 1 then
  return {0}
end
local fence = redis.call('INCR', KEYS[2])
redis.call('EXPIRE', KEYS[2], tonumber(ARGV[3]))
local lease_val = ARGV[1] .. ':' .. tostring(fence)
redis.call('SET', KEYS[1], lease_val, 'PX', tonumber(ARGV[2]))
return {1, tostring(fence)}
"""

# Renew lease only if caller still holds the exact owner:fence value.
RENEW_LEASE_LUA = """
if redis.call('GET', KEYS[1]) ~= ARGV[1] then
  return 0
end
redis.call('PEXPIRE', KEYS[1], tonumber(ARGV[2]))
redis.call('EXPIRE', KEYS[2], tonumber(ARGV[3]))
return 1
"""

# Release lease only if caller still holds the exact owner:fence value.
RELEASE_LEASE_LUA = """
if redis.call('GET', KEYS[1]) ~= ARGV[1] then
  return 0
end
redis.call('DEL', KEYS[1])
return 1
"""
