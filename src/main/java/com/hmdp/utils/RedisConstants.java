package com.hmdp.utils;

public class RedisConstants {
    public static final String LOGIN_CODE_KEY = "login:code:";
    public static final Long LOGIN_CODE_TTL = 2L;
    public static final String LOGIN_USER_KEY = "login:token:";
    public static final Long LOGIN_USER_TTL = 30L;

    // ---- 限流相关 key（滑动窗口）----
    public static final String LIMIT_LOGIN_IP_KEY = "limit:login:ip:";
    public static final String LIMIT_LOGIN_USER_KEY = "limit:login:user:";
    public static final String LIMIT_CODE_KEY = "limit:code:";
    public static final Long LIMIT_CODE_TTL = 60L; // 同一手机号 60 秒内只允许发送一次验证码
    public static final String LIMIT_SECKILL_KEY = "limit:seckill:";

    public static final Long CACHE_NULL_TTL = 2L;

    public static final Long CACHE_SHOP_TTL = 30L;
    public static final String CACHE_SHOP_KEY = "cache:shop:";
    public static final String CACHE_SHOP_TYPE_KEY = "cache:shop-type:";

    public static final String LOCK_SHOP_KEY = "lock:shop:";
    public static final Long LOCK_SHOP_TTL = 10L;

    public static final String SECKILL_STOCK_KEY = "seckill:stock:";
    public static final String SECKILL_ORDER_KEY = "seckill:order:";

    // ---- 秒杀异步链路可靠性 ----
    public static final String ORDER_RETRY_KEY = "order:retry:";            // 消息消费失败次数
    public static final String ORDER_COMPENSATED_KEY = "seckill:compensated:"; // 库存补偿幂等标记
    public static final String ORDER_FAIL_KEY = "order:fail:";              // 落库最终失败的任务快照
    public static final int ORDER_MAX_RETRY = 3;                            // 单条消息的最大重试次数

    public static final String BLOG_LIKED_KEY = "blog:liked:";
    public static final String FEED_KEY = "feed:";
    public static final String SHOP_GEO_KEY = "shop:geo:";
    public static final String USER_SIGN_KEY = "sign:";
}
