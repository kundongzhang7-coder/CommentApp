package com.hmdp.utils;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

@Component
public class RedisIdWorker {
    /**
     * 开始时间戳
     */
    private static final long BEGIN_TIMESTAMP = 1664582400L;
    /**
     * 序列号位数
     */
    private static final long COUNT_BITS = 32;

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    public long nextId(String keyPrefix) {
        //1.生成时间戳
        long now = LocalDateTime.now().toEpochSecond(ZoneOffset.UTC);
        long timestamp = now - BEGIN_TIMESTAMP;

        //2.生成序列号
        //2.1获取当前日期，精确到天
        String date = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy:MM:dd"));
        long count = stringRedisTemplate.opsForValue().increment("incr:" + keyPrefix + ":" + date);

        //3.拼接并返回
        return timestamp << COUNT_BITS | count;
    }
}
