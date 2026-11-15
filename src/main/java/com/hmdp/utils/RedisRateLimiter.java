package com.hmdp.utils;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.util.Collections;
import java.util.UUID;

/**
 * 基于 Redis ZSet 的滑动窗口限流器。
 * <p>
 * 与"固定窗口 + INCR + EXPIRE"相比，滑动窗口不会出现窗口边界双倍放行；
 * 与"先 ZCARD 再 ZADD 的四次调用"相比，判定与记账在同一个 Lua 内完成，
 * 并发下不会出现"多个请求同时看到未超限"。
 */
@Slf4j
@Component
public class RedisRateLimiter {

    private static final DefaultRedisScript<Long> ACQUIRE_SCRIPT = new DefaultRedisScript<>();
    private static final DefaultRedisScript<Long> RECORD_SCRIPT = new DefaultRedisScript<>();

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @PostConstruct
    public void init() {
        ACQUIRE_SCRIPT.setLocation(new ClassPathResource("lua/rate_limit.lua"));
        ACQUIRE_SCRIPT.setResultType(Long.class);
        RECORD_SCRIPT.setLocation(new ClassPathResource("lua/rate_limit_count.lua"));
        RECORD_SCRIPT.setResultType(Long.class);
    }

    /**
     * 尝试获取一次配额：窗口内累计次数 < max 时放行并记账，否则拒绝且不记账。
     *
     * @param key    限流维度 key，例如 limit:login:ip:127.0.0.1
     * @param max    窗口内允许的最大次数
     * @param window 窗口长度
     * @return true 放行；false 已超限
     */
    public boolean tryAcquire(String key, int max, Duration window) {
        if (max <= 0 || window == null || window.isNegative() || window.isZero()) {
            throw new IllegalArgumentException("限流参数非法: max=" + max + ", window=" + window);
        }
        long now = System.currentTimeMillis();
        Long result = stringRedisTemplate.execute(
                ACQUIRE_SCRIPT,
                Collections.singletonList(key),
                String.valueOf(now),
                String.valueOf(now - window.toMillis()),
                String.valueOf(window.toMillis()),
                String.valueOf(max),
                now + ":" + UUID.randomUUID().toString().substring(0, 8)
        );
        return result != null && result > 0;
    }

    /**
     * 只记账、不拦截，返回本次记账后窗口内的累计次数。用于"只告警"的辅助维度。
     */
    public long record(String key, Duration window) {
        long now = System.currentTimeMillis();
        Long result = stringRedisTemplate.execute(
                RECORD_SCRIPT,
                Collections.singletonList(key),
                String.valueOf(now),
                String.valueOf(now - window.toMillis()),
                String.valueOf(window.toMillis()),
                now + ":" + UUID.randomUUID().toString().substring(0, 8)
        );
        return result == null ? 0L : result;
    }

    /**
     * 清空某个维度的计数（例如登录成功后清掉该手机号的失败计数）。
     */
    public void reset(String key) {
        try {
            stringRedisTemplate.delete(key);
        } catch (Exception e) {
            log.warn("清理限流计数失败 key={}", key, e);
        }
    }
}
