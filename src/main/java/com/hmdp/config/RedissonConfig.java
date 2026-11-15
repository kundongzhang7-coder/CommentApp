package com.hmdp.config;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Redisson 客户端配置。
 * <p>
 * 原来 2、3 号客户端是教程用来演示"多个独立的 Redisson 客户端"的，业务代码并没有注入它们；
 * 但声明成常驻 Bean 后，任何只跑一个 Redis 的环境（包括本机单节点、CI、Docker 单实例）
 * 都会因为连不上 6380/6381 而整个应用启动失败。
 * 这里改成按配置启用：默认关闭，需要做多节点实验时把 hmdp.redis.extra-nodes-enabled 置为 true。
 */
@Configuration
public class RedissonConfig {

    @Bean
    public RedissonClient redissonClient() {
        // 配置
        Config config = new Config();
        config.useSingleServer().setAddress("redis://127.0.0.1:6379").setPassword("123456");

        // 配置RedissonClient对象
        return Redisson.create(config);
    }

    @Bean
    @ConditionalOnProperty(name = "hmdp.redis.extra-nodes-enabled", havingValue = "true")
    public RedissonClient redissonClient2() {
        // 配置
        Config config = new Config();
        config.useSingleServer().setAddress("redis://127.0.0.1:6380").setPassword("123456");

        // 配置RedissonClient对象
        return Redisson.create(config);
    }

    @Bean
    @ConditionalOnProperty(name = "hmdp.redis.extra-nodes-enabled", havingValue = "true")
    public RedissonClient redissonClient3() {
        // 配置
        Config config = new Config();
        config.useSingleServer().setAddress("redis://127.0.0.1:6381").setPassword("123456");

        // 配置RedissonClient对象
        return Redisson.create(config);
    }
}
