-- 1.参数列表
-- 1.1优惠券id
local voucherId = ARGV[1]
-- 1.2用户id
local userId = ARGV[2]
-- 1.3订单id（由 Java 侧生成，Lua 不再使用；仅保留位置方便排查）

-- 2.数据key
-- 2.1库存key
local stockKey = "seckill:stock:" .. voucherId

-- 2.2订单key
local orderKey = "seckill:order:" .. voucherId

-- 3.脚本业务
-- 注意：这里只做“判定 + 预占”，不再 XADD。消息投递由 Java 侧的 RabbitMQ 生产者完成，
-- 因为 Lua 只能写 Redis，无法保证消息进入 broker，也就无法参与发布确认与失败回滚。
-- 3.1库存判断
if tonumber(redis.call("get", stockKey)) <= 0 then
    -- 3.2库存不足，返回1
    return 1
end
-- 判断用户是否下单 SISMEMBER orderKey userId
if redis.call("sismember", orderKey, userId) == 1 then
    -- 3.3用户已下单，返回2
    return 2
end
-- 3.4减库存
redis.call("incrby", stockKey, -1)
-- 3.5标记一人一单
redis.call("sadd", orderKey, userId)
-- 3.6返回成功，由 Java 投递 RabbitMQ
return 0
