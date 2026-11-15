package com.hmdp.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * 安全相关配置。
 * <p>
 * trusted-proxies 为空表示"前面没有反向代理"，此时完全忽略 X-Forwarded-For，
 * 只用 remoteAddr 做限流维度；只有部署了 Nginx 且希望按真实客户端 IP 限流时，
 * 才把代理自身的地址（或网段）填进来。
 */
@Data
@Component
@ConfigurationProperties(prefix = "hmdp.security")
public class SecurityProperties {

    /**
     * 可信反向代理地址，支持精确 IP 与 CIDR，例如 127.0.0.1、10.0.0.0/8。
     */
    private List<String> trustedProxies = new ArrayList<>();

    /**
     * 登录接口 IP 维度限流：窗口内最大次数。
     */
    private int loginIpMax = 10;

    /**
     * 登录接口 IP 维度限流：窗口长度（分钟）。
     */
    private long loginWindowMinutes = 10;

    /**
     * 登录失败按手机号维度的告警阈值（只告警不拦截，避免他人恶意刷锁死正常用户）。
     */
    private int loginUserAlarmThreshold = 20;

    /**
     * 秒杀接口用户维度限流：窗口内最大次数。
     */
    private int seckillUserMax = 10;

    /**
     * 秒杀接口用户维度限流：窗口长度（秒）。
     */
    private long seckillWindowSeconds = 10;
}
