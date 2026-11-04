-- compare thread sign of lock with current thread sign
if (redis.call("GET", KEYS[1]) == ARGV[1]) then
  -- release lock
  return redis.call("DEL", KEYS[1])
end
return 0