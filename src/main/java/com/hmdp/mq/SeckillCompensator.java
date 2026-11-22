package com.hmdp.mq;

import cn.hutool.json.JSONUtil;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.utils.RedisConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * 秒杀预占的补偿动作：把 Lua 扣掉的库存还回去、把一人一单标记移除，并留下失败快照。
 * <p>
 * 两处会用到：① 生产端投递失败（消息没进 broker）；② 消费端重试耗尽进了死信队列。
 * 用 SETNX 标记保证幂等——同一订单被补偿多次时只会生效一次，否则库存会越加越多。
 */
@Slf4j
@Component
public class SeckillCompensator {

    private final StringRedisTemplate stringRedisTemplate;

    public SeckillCompensator(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    /**
     * 回滚 Redis 侧的秒杀预占。
     *
     * @return true 表示本次真的执行了补偿（false 表示之前已补偿过，幂等跳过）
     */
    public boolean compensate(VoucherOrder voucherOrder) {
        String markKey = RedisConstants.ORDER_COMPENSATED_KEY + voucherOrder.getId();
        Boolean first = stringRedisTemplate.opsForValue().setIfAbsent(markKey, "1", Duration.ofHours(2));
        if (!Boolean.TRUE.equals(first)) {
            log.info("该订单已补偿过，跳过 orderId={}", voucherOrder.getId());
            return false;
        }
        try {
            String stockKey = RedisConstants.SECKILL_STOCK_KEY + voucherOrder.getVoucherId();
            // 只有库存 key 还在时才回加，避免 key 过期后 INCR 凭空造出值为 1 的库存
            if (Boolean.TRUE.equals(stringRedisTemplate.hasKey(stockKey))) {
                stringRedisTemplate.opsForValue().increment(stockKey);
            }
            stringRedisTemplate.opsForSet().remove(
                    RedisConstants.SECKILL_ORDER_KEY + voucherOrder.getVoucherId(),
                    voucherOrder.getUserId().toString());
            log.warn("已回滚秒杀预占 orderId={} voucherId={} userId={}",
                    voucherOrder.getId(), voucherOrder.getVoucherId(), voucherOrder.getUserId());
            return true;
        } catch (Exception e) {
            log.error("回滚秒杀预占失败，需人工介入 orderId={}", voucherOrder.getId(), e);
            return false;
        }
    }

    /**
     * 落库最终失败的任务快照，保留 7 天便于人工核对与补单。
     */
    public void markFailed(VoucherOrder voucherOrder, String reason) {
        try {
            String key = RedisConstants.ORDER_FAIL_KEY + voucherOrder.getId();
            stringRedisTemplate.opsForValue()
                    .set(key, JSONUtil.toJsonStr(voucherOrder) + "|" + reason, Duration.ofDays(7));
        } catch (Exception e) {
            log.error("记录失败订单快照异常 orderId={}", voucherOrder.getId(), e);
        }
    }
}
