package com.hmdp.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * 消息队列的可调参数（面试/压测时不用改代码，改配置即可）。
 */
@Data
@Component
@ConfigurationProperties(prefix = "hmdp.mq")
public class MqProperties {

    /** 消费端本地重试次数：超过后拒绝消息，转入死信队列做补偿 */
    private int retryMax = 3;

    /** 重试退避基数（毫秒），第 n 次重试等待 n * 该值 */
    private long retryBackoffMillis = 200;
}
