package com.hmdp;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.concurrent.TimeUnit;

@Slf4j
@SpringBootTest
public class RedissonTest {
    @Autowired
    private RedissonClient redissonClient;
    @Autowired
    private RedissonClient redissonClient2;
    @Autowired
    private RedissonClient redissonClient3;

    private RLock lock;

    @BeforeEach
    void setUp() {
        RLock rLock1 = redissonClient.getLock("lock:order");
        RLock rLock2 = redissonClient2.getLock("lock:order");
        RLock rLock3 = redissonClient3.getLock("lock:order");

        // 创建一个联锁
        lock = redissonClient.getMultiLock(rLock1, rLock2, rLock3);
    }

    @Test
    void method1() throws InterruptedException {
        // 尝试获取锁
        boolean isLocked = lock.tryLock(1L,  TimeUnit.SECONDS);
        if(!isLocked) {
            log.error("未获取到锁......1");
            return;
        }
        try {
            log.info("获取到锁......1");
            // 同一线程重入加锁，计数 1 -> 2
            lock.lock();
            log.info("重入加锁......1");

            // 解锁一次，计数 2 -> 1
            lock.unlock();
            log.info("解锁一次......1");

        } finally {
            // 再解锁一次，计数 1 -> 0，key 消失
            lock.unlock();
            log.info("释放锁......1");
        }
    }

    @Test
    void method2() throws InterruptedException {
        // 尝试获取锁
        boolean isLocked = lock.tryLock(1L, TimeUnit.SECONDS);
        if(!isLocked) {
            log.error("未获取到锁......2");
            return;
        }
        try {
            log.info("获取到锁......2");
            // 同一线程重入加锁，计数 1 -> 2
            lock.lock();
            log.info("重入加锁......2");

            // 解锁一次，计数 2 -> 1
            lock.unlock();
            log.info("解锁一次......2");

        } finally {
            // 再解锁一次，计数 1 -> 0，key 消失
            lock.unlock();
            log.info("释放锁......2");
        }
    }
}
