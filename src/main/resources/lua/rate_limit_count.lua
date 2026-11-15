-- 只记账不拦截：用于"按用户名统计失败次数、只告警不锁定"的辅助维度
-- 与 rate_limit.lua 的区别是不做阈值判断，永远记录并返回当前窗口内次数
-- KEYS[1] 窗口 key
-- ARGV[1] 当前时间戳(ms)  ARGV[2] 窗口左边界(ms)  ARGV[3] key 存活时间(ms)  ARGV[4] 成员标识

local key = KEYS[1]
redis.call('ZREMRANGEBYSCORE', key, 0, ARGV[2])
redis.call('ZADD', key, ARGV[1], ARGV[4])
redis.call('PEXPIRE', key, ARGV[3])
return redis.call('ZCARD', key)
