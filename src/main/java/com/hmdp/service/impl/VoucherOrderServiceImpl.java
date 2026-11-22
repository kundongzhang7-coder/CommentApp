package com.hmdp.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.config.SecurityProperties;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.mq.SeckillCompensator;
import com.hmdp.mq.SeckillOrderMessage;
import com.hmdp.mq.SeckillOrderProducer;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.RedisRateLimiter;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.util.Collections;

import static com.hmdp.utils.RedisConstants.LIMIT_SECKILL_KEY;

/**
 * 秒杀下单服务。
 * <p>
 * 链路：滑动窗口限流 -> Redis Lua 原子预扣（库存 + 一人一单）-> RabbitMQ 投递订单任务 -> 返回订单号；
 * 落库由 {@link com.hmdp.mq.SeckillOrderConsumer} 异步完成，失败重试耗尽后进死信队列做补偿。
 * <p>
 * 与早期 Redis Stream 方案相比：队列的可靠性（持久化、投递确认、死信路由）交给 broker，
 * 应用侧不再需要自建 pending-list 扫描线程和 Redis 重试计数。
 *
 * @author 虎哥
 */
@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Autowired
    private ISeckillVoucherService seckillVoucherService;

    @Autowired
    private RedisIdWorker redisIdWorker;

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Autowired
    private RedissonClient redissonClient;

    @Autowired
    private TransactionTemplate transactionTemplate;

    @Autowired
    private RedisRateLimiter rateLimiter;

    @Autowired
    private SecurityProperties securityProperties;

    @Autowired
    private SeckillOrderProducer seckillOrderProducer;

    @Autowired
    private SeckillCompensator seckillCompensator;

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    /**
     * 秒杀入口：只做「资格判定 + 预占 + 投递」，绝不在请求线程里碰数据库。
     */
    @Override
    public Result seckillVoucher(Long voucherId) {
        UserDTO user = UserHolder.getUser();
        if (user == null) {
            return Result.fail("请先登录");
        }
        Long userId = user.getId();

        // 0.用户维度滑动窗口限流：拦住脚本连点。放在 Lua 之前，超限请求不参与库存争抢，
        //   也不会污染一人一单集合（否则正常用户想重试时会被误判“已下单”）
        boolean allowed = rateLimiter.tryAcquire(
                LIMIT_SECKILL_KEY + userId + ":" + voucherId,
                securityProperties.getSeckillUserMax(),
                Duration.ofSeconds(securityProperties.getSeckillWindowSeconds()));
        if (!allowed) {
            log.warn("秒杀接口触发用户维度限流 userId={} voucherId={}", userId, voucherId);
            return Result.fail("手速太快了，请稍后再试");
        }

        long orderId = redisIdWorker.nextId("order");

        // 1.执行 Lua：库存判断 + 一人一单判断 + 扣减 + 标记，四步原子完成
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString(), String.valueOf(orderId)
        );

        int r = result == null ? -1 : result.intValue();
        if (r != 0) {
            return Result.fail(r == 1 ? "库存不足！" : "请勿重复下单！");
        }

        // 2.预占成功，投递到 RabbitMQ。送达确认失败时必须就地回滚预占，
        //   否则会出现“前端拿到订单号、后台没有订单、库存还被扣了”的资损
        VoucherOrder voucherOrder = new VoucherOrder();
        voucherOrder.setId(orderId);
        voucherOrder.setUserId(userId);
        voucherOrder.setVoucherId(voucherId);
        try {
            seckillOrderProducer.send(new SeckillOrderMessage(orderId, userId, voucherId));
        } catch (Exception e) {
            log.error("秒杀消息投递失败，回滚预占 orderId={} userId={}", orderId, userId, e);
            seckillCompensator.compensate(voucherOrder);
            seckillCompensator.markFailed(voucherOrder, "消息投递失败: " + e.getMessage());
            return Result.fail("下单繁忙，请稍后再试");
        }

        log.info("秒杀预占成功并已入队 orderId={} userId={} voucherId={}", orderId, userId, voucherId);
        return Result.ok(orderId);
    }

    /**
     * 消息消费入口：真正的落库动作，由 MQ 消费者调用。
     * <p>
     * 只负责“抢到锁 + 事务落库”，失败一律抛出去让上层重试；
     * 补偿动作由死信队列消费者统一执行，避免两处各补一次把库存加多。
     *
     * @return true 落库成功（含幂等命中）
     */
    @Override
    public boolean handleOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        if (!lock.tryLock()) {
            throw new IllegalStateException("获取下单锁失败，稍后重试 orderId=" + voucherOrder.getId());
        }
        try {
            // 异步线程里用 TransactionTemplate 显式开事务（自调用 @Transactional 不生效）
            transactionTemplate.execute(status -> {
                createVoucherOder(voucherOrder);
                return null;
            });
            log.info("订单落库成功 orderId={} userId={} voucherId={}",
                    voucherOrder.getId(), userId, voucherOrder.getVoucherId());
            return true;
        } finally {
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }

    @Transactional
    public void createVoucherOder(VoucherOrder voucherOrder) {
        // 5.幂等：同一订单号被重复投递（至少一次投递语义）时直接跳过
        if (getById(voucherOrder.getId()) != null) {
            log.info("订单已存在，幂等跳过 orderId={}", voucherOrder.getId());
            return;
        }

        // 6.一人一单
        Long userId = voucherOrder.getUserId();

        // 6.1.查询订单
        long count = query().eq("user_id", userId)
                .eq("voucher_id", voucherOrder.getVoucherId()).count();

        // 6.2 DB 复查是 Redis 之外的最后一道闸，命中时抛异常让事务回滚，
        //    由死信消费者据此补偿 Redis 预占
        if (count > 0) {
            throw new IllegalStateException("用户已购买过该优惠券, userId=" + userId
                    + ", voucherId=" + voucherOrder.getVoucherId());
        }

        // 7.扣减库存
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherOrder.getVoucherId()).gt("stock", 0)
                .update();
        if (!success) {
            throw new IllegalStateException("数据库库存不足, voucherId=" + voucherOrder.getVoucherId());
        }

        // 8.创建订单
        save(voucherOrder);
    }
}
