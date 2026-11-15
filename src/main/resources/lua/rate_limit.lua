-- 滑动窗口限流：一次 EVAL 内完成"清理 -> 计数 -> 判定 -> 记账 -> 续期"
-- 用 Redis 单线程保证原子性，避免"多个请求同时 ZCARD 都看到未超限"的并发漏洞
-- KEYS[1] 窗口 key
-- ARGV[1] 当前时间戳(ms)  ARGV[2] 窗口左边界(ms)  ARGV[3] key 存活时间(ms)
-- ARGV[4] 窗口内最大次数   ARGV[5] 本次请求的唯一成员标识
-- 返回 -1 表示已超限（本次不记账）；返回 >=1 表示本次放行，值为窗口内累计次数

local key = KEYS[1]
redis.call('ZREMRANGEBYSCORE', key, 0, ARGV[2])

local count = redis.call('ZCARD', key)
if count >= tonumber(ARGV[4]) then
    redis.call('PEXPIRE', key, ARGV[3])
    return -1
end

redis.call('ZADD', key, ARGV[1], ARGV[5])
redis.call('PEXPIRE', key, ARGV[3])
return count + 1
