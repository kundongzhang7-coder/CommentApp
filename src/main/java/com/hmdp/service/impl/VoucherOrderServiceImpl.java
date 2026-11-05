package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.RedisIdWorker;
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

                    //3 解析消息中的订单任务
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);

                    //4 创建订单
                    handleVoucherOder(voucherOrder);

                    //5.ACK确认
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());
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

                    //3 解析消息中的订单任务
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);

                    //4 创建订单
                    handleVoucherOder(voucherOrder);

                    //5.ACK确认 SACK stream.order g1 id
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());
                } catch (Exception e) {
                    log.error("处理pending-list订单异常", e);
                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            }
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

    private void handleVoucherOder(VoucherOrder voucherOrder) {
        //1.获取用户
        Long userId = voucherOrder.getUserId();
        //2.创建锁对象
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        //3.获取锁
        boolean isLock = lock.tryLock();
        //4.判断是否获取锁成功
        if (!isLock) {
            //获取锁失败，返回错误或重试
            log.error("请勿重复下单！");
            return;
        }

        try {
            //通过TransactionTemplate在异步线程中包事务（不依赖AopContext代理，保证事务生效）
            transactionTemplate.execute(status -> {
                createVoucherOder(voucherOrder);
                return null;
            });
        } finally {
            //释放锁
            lock.unlock();
        }
    }

    @Override
    public Result seckillVoucher(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
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
        //5.一人一单
        Long userId = voucherOrder.getUserId();

        //5.1.查询订单
        long count = query().eq("user_id", userId)
                .eq("voucher_id", voucherOrder.getVoucherId()).count();

        //5.2判断订单是否存在
        if (count > 0) {
            log.error("用户已经购买过该优惠券！");
            return;
        }
        //6.扣减库存
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherOrder.getVoucherId()).gt("stock", 0)
                .update();
        if (!success) {
            //扣减失败
            log.error("库存不足！");
            return;
        }

        //7.创建订单
        save(voucherOrder);

    }
}
