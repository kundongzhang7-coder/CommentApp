package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.config.SecurityProperties;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.RedisRateLimiter;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

import javax.annotation.PostConstruct;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
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

//    private static final BlockingQueue<VoucherOrderTask> oderTasks = new ArrayBlockingQueue<>(1024 * 1024);
    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

    @PostConstruct
    public void init() {
        try {
            // 启动自愈：确保消费组存在。Redis 重启/清库后 stream.orders 会丢，这里自动重建(MKSTREAM)，避免消费者 NOGROUP 死循环刷屏
            stringRedisTemplate.execute((RedisCallback<Object>) connection ->
                    connection.xGroupCreate("stream.orders".getBytes(StandardCharsets.UTF_8),
                            "g1", ReadOffset.from("0"), true));
        } catch (Exception e) {
            // 组已存在会抛 BUSYGROUP，属正常，忽略
            log.info("消费组已存在，无需重建: {}", e.getMessage());
        }
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }

    private class VoucherOrderHandler implements Runnable {
        private static final String queueName = "stream.orders";
        @Override
        public void run() {
            while (true) {
                try {
                    //1.获取消息队列中订单任务 XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS stream.order >
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queueName, ReadOffset.lastConsumed())
                    );

                    //2.判断消息获取是否成功
                    if (list == null || list.isEmpty()) {
                        //2.1失败则无消息，继续循环
                        continue;
                    }

                    //3 解析并处理：由处理结果决定 ACK、重试还是补偿后放弃
                    process(list.get(0));
                } catch (Exception e) {
                    log.error("处理订单异常", e);
                    handlePendingList();
                }
            }
        }

        private void handlePendingList() {
            while (true) {
                try {
                    //1.获取pending-list中订单任务 XREADGROUP GROUP g1 c1 COUNT 1 STREAMS stream.order 0
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create(queueName, ReadOffset.from("0"))
                    );

                    //2.判断消息获取是否成功
                    if (list == null || list.isEmpty()) {
                        //2.1失败则pending-list无异常消息，结束循环
                        break;
                    }

                    //3 逐条处理 pending-list 中的历史消息（同样受重试上限约束，不会卡死消费者线程）
                    for (MapRecord<String, Object, Object> record : list) {
                        process(record);
                    }
                } catch (Exception e) {
                    log.error("处理pending-list订单异常", e);
                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(ex);
                    }
                }
            }
        }

        /**
         * 处理单条消息，按结果决定三种去向：
         * 1) 落库成功 -> ACK，并清掉重试计数；
         * 2) 暂时处理不了（例如没抢到用户级锁）-> 累计重试次数，留在 pending-list 等下一轮；
         * 3) 重试超过上限 -> 补偿 Redis 预占 + 记录失败快照 + ACK，
         *    避免一条坏消息让消费线程永久重复处理（原实现是 while(true) 死循环 + 只 sleep）。
         */
        private void process(MapRecord<String, Object, Object> record) {
            VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(record.getValue(), new VoucherOrder(), true);

            if (handleVoucherOder(voucherOrder)) {
                acknowledge(record.getId());
                stringRedisTemplate.delete(ORDER_RETRY_KEY + record.getId());
                return;
            }

            String retryKey = ORDER_RETRY_KEY + record.getId();
            Long retry = stringRedisTemplate.opsForValue().increment(retryKey);
            stringRedisTemplate.expire(retryKey, Duration.ofHours(1));
            if (retry != null && retry >= ORDER_MAX_RETRY) {
                log.error("订单任务重试超过上限，补偿后放弃 orderId={} retry={}", voucherOrder.getId(), retry);
                compensate(voucherOrder);
                markFailed(voucherOrder, "消费重试超过 " + ORDER_MAX_RETRY + " 次");
                acknowledge(record.getId());
                stringRedisTemplate.delete(retryKey);
            } else {
                log.warn("订单任务暂未处理成功，等待下次重试 orderId={} retry={}", voucherOrder.getId(), retry);
            }
        }

        private void acknowledge(RecordId recordId) {
            stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", recordId);
        }
    }

//    private class VoucherOrderHandler implements Runnable {
//        @Override
//        public void run() {
//            while (true) {
//                try {
//                    //1.获取订单任务
//                    VoucherOrderTask task = oderTasks.take();
//                    //2.创建订单
//                    handleVoucherOder(task);
//                } catch (Exception e) {
//                    log.error("处理订单异常", e);
//                }
//            }
//        }
//    }

    /**
     * 处理一条订单任务。
     *
     * @return true 表示本条消息已经"处理完毕"（成功落库，或失败后已补偿并记账，可以 ACK）；
     * false 表示暂时处理不了（锁竞争失败等），应当保留在 pending-list 等待重试
     */
    private boolean handleVoucherOder(VoucherOrder voucherOrder) {
        //1.获取用户
        Long userId = voucherOrder.getUserId();
        //2.创建锁对象
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        //3.获取锁
        boolean isLock = lock.tryLock();
        //4.判断是否获取锁成功
        if (!isLock) {
            //获取锁失败：不 ACK，交给重试机制，不能像原实现那样直接丢消息
            log.warn("获取下单锁失败，稍后重试 orderId={} userId={}", voucherOrder.getId(), userId);
            return false;
        }

        try {
            //通过TransactionTemplate在异步线程中包事务（不依赖AopContext代理，保证事务生效）
            transactionTemplate.execute(status -> {
                createVoucherOder(voucherOrder);
                return null;
            });
            log.info("订单落库成功 orderId={} userId={} voucherId={}",
                    voucherOrder.getId(), userId, voucherOrder.getVoucherId());
            return true;
        } catch (Exception e) {
            //事务已回滚，DB 里没有订单；但 Lua 已经扣过库存、写过一人一单，
            //不补偿就会"库存永久少一件 + 该用户永远买不了"，这里把预占还回去并记录失败快照
            log.error("订单落库失败，执行 Redis 侧补偿 orderId={} userId={}", voucherOrder.getId(), userId, e);
            compensate(voucherOrder);
            markFailed(voucherOrder, e.getMessage());
            return true;
        } finally {
            //释放锁
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }

    /**
     * 秒杀预占补偿：把 Lua 扣掉的库存加回去、把一人一单的 Set 标记移除。
     * <p>
     * 用 SETNX 标记保证幂等——同一订单被重复处理时只补偿一次，否则会把库存越加越多。
     */
    private void compensate(VoucherOrder voucherOrder) {
        String markKey = ORDER_COMPENSATED_KEY + voucherOrder.getId();
        Boolean first = stringRedisTemplate.opsForValue().setIfAbsent(markKey, "1", Duration.ofHours(2));
        if (!Boolean.TRUE.equals(first)) {
            log.info("该订单已补偿过，跳过 orderId={}", voucherOrder.getId());
            return;
        }
        try {
            String stockKey = SECKILL_STOCK_KEY + voucherOrder.getVoucherId();
            //只有库存预热还在时才回加，避免 key 过期后 INCR 凭空造出一个值为 1 的库存
            if (Boolean.TRUE.equals(stringRedisTemplate.hasKey(stockKey))) {
                stringRedisTemplate.opsForValue().increment(stockKey);
            }
            stringRedisTemplate.opsForSet().remove(
                    SECKILL_ORDER_KEY + voucherOrder.getVoucherId(),
                    voucherOrder.getUserId().toString());
            log.warn("已回滚秒杀预占 orderId={} voucherId={} userId={}",
                    voucherOrder.getId(), voucherOrder.getVoucherId(), voucherOrder.getUserId());
        } catch (Exception e) {
            log.error("回滚秒杀预占失败，需人工介入 orderId={}", voucherOrder.getId(), e);
        }
    }

    /**
     * 落库最终失败的任务快照，保留 7 天便于人工核对与补单。
     */
    private void markFailed(VoucherOrder voucherOrder, String reason) {
        try {
            String key = ORDER_FAIL_KEY + voucherOrder.getId();
            stringRedisTemplate.opsForValue()
                    .set(key, JSONUtil.toJsonStr(voucherOrder) + "|" + reason, Duration.ofDays(7));
        } catch (Exception e) {
            log.error("记录失败订单快照异常 orderId={}", voucherOrder.getId(), e);
        }
    }

    @Override
    public Result seckillVoucher(Long voucherId) {
        UserDTO user = UserHolder.getUser();
        if (user == null) {
            return Result.fail("请先登录");
        }
        Long userId = user.getId();

        //0.用户维度滑动窗口限流：拦住脚本连点。放在 Lua 之前，超限请求不参与库存争抢，
        //   既能挡住刷单，也不会污染一人一单集合（否则正常用户想重试时会被误判"已下单"）
        boolean allowed = rateLimiter.tryAcquire(
                LIMIT_SECKILL_KEY + userId + ":" + voucherId,
                securityProperties.getSeckillUserMax(),
                Duration.ofSeconds(securityProperties.getSeckillWindowSeconds()));
        if (!allowed) {
            log.warn("秒杀接口触发用户维度限流 userId={} voucherId={}", userId, voucherId);
            return Result.fail("手速太快了，请稍后再试");
        }

        long orderId = redisIdWorker.nextId("order");
        //1.执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString(), String.valueOf(orderId)
        );

        //2.判断结果是否为0
        int r = result.intValue();
        if (r != 0) {
            //2.1 非0则无购买资格
            return Result.fail(r == 1 ? "库存不足！" : "请勿重复下单！");
        }

        //3.获取锁成功，Lua已把订单消息写入Redis Stream，直接返回订单id
        return Result.ok(orderId);
    }

//    @Override
//    public Result seckillVoucher(Long voucherId) {
//        Long userId = UserHolder.getUser().getId();
//        //1.执行lua脚本
//        Long result = stringRedisTemplate.execute(
//                      SECKILL_SCRIPT,
//                      Collections.emptyList(),
//                      voucherId.toString(), userId.toString());
//
//        //2.判断结果是否为0
//        int r = result.intValue();
//        if (r != 0) {
//            //2.1 非0则无购买资格
//            return Result.fail(r == 1 ? "库存不足！" : "请勿重复下单！");
//        }
//
//        //2.2 0则有购买资格，将下单信息保存到阻塞队列
//        VoucherOrder voucherOrder = new VoucherOrder();
//
//        //2.3设置订单id
//        long orderId = redisIdWorker.nextId("order");
//        voucherOrder.setId(orderId);
//
//        //2.4设置用户id
//        voucherOrder.setUserId(userId);
//
//        //2.5设置优惠券id
//        voucherOrder.setVoucherId(voucherId);
//
//        //2.6获取事务代理（当前在代理方法内，可拿到），随订单放入阻塞队列由异步线程落库
//        IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
//        oderTasks.add(new VoucherOrderTask(voucherOrder, proxy));
//
//        //3.返回订单id
//        return Result.ok(orderId);
//    }

    /**
     * 秒杀优惠券
     *
     * @param voucherOrder
     */
//    @Override
//    public Result seckillVoucher(Long voucherId) {
//        //1.查询优惠券
//        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
//
//        //2.判断秒杀是否开始
//        if (voucher.getBeginTime().isAfter(LocalDateTime.now())) {
//            return Result.fail("秒杀尚未开始！");
//        }
//
//        //3.判断秒杀是否结束
//        if (voucher.getEndTime().isBefore(LocalDateTime.now())) {
//            return Result.fail("秒杀已经结束！");
//        }
//
//        //4.判断库存是否充足
//        if (voucher.getStock() <= 0) {
//            return Result.fail("库存不足！");
//        }
//
//        //8.返回订单id
//        Long userId = UserHolder.getUser().getId();
//
//        //创建锁对象
//        //SimpleRedisLook lock = new SimpleRedisLook("order:" + userId, stringRedisTemplate);
//        RLock lock = redissonClient.getLock("lock:order:" + userId);
//
//        //获取锁
//        if (!lock.tryLock()) {
//            return Result.fail("请勿重复下单！");
//        }
//
//        try {
//            //获取代理对象（事务）
//            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
//            return proxy.createVoucherOder(voucherId);
//        } catch (IllegalStateException e) {
//            throw new RuntimeException(e);
//        } finally {
//            //释放锁
//            lock.unlock();
//        }
//    }

    @Transactional
    public void createVoucherOder(VoucherOrder voucherOrder) {
        //5.幂等：同一订单号被重复投递（Stream 至少一次语义 + pending 重试）时直接跳过
        if (getById(voucherOrder.getId()) != null) {
            log.info("订单已存在，幂等跳过 orderId={}", voucherOrder.getId());
            return;
        }

        //6.一人一单
        Long userId = voucherOrder.getUserId();

        //6.1.查询订单
        long count = query().eq("user_id", userId)
                .eq("voucher_id", voucherOrder.getVoucherId()).count();

        //6.2判断订单是否存在：DB 复查是 Redis 之外的最后一道闸，
        //    命中时抛异常让事务回滚，由消费者据此补偿 Redis 预占（原实现只打日志后 return，
        //    会留下"Redis 已扣库存但 DB 无订单"的不一致）
        if (count > 0) {
            throw new IllegalStateException("用户已购买过该优惠券, userId=" + userId
                    + ", voucherId=" + voucherOrder.getVoucherId());
        }

        //7.扣减库存
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherOrder.getVoucherId()).gt("stock", 0)
                .update();
        if (!success) {
            //扣减失败同样抛异常，触发回滚与补偿
            throw new IllegalStateException("数据库库存不足, voucherId=" + voucherOrder.getVoucherId());
        }

        //8.创建订单
        save(voucherOrder);

    }
}
