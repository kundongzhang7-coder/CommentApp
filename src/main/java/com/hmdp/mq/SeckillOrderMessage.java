package com.hmdp.mq;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * 秒杀下单消息体。
 * <p>
 * 只放落库必需的三要素，不放实体：消息越瘦，积压时占用的 broker 内存越少，
 * 也避免实体字段演进（比如加字段）导致历史消息反序列化失败。
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SeckillOrderMessage implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 订单号（RedisIdWorker 生成的全局唯一 id，同时用作消费幂等键） */
    private Long orderId;

    /** 用户 id */
    private Long userId;

    /** 优惠券 id */
    private Long voucherId;
}
