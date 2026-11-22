package com.hmdp.mq;

/**
 * 消息队列常量。
 * <p>
 * 交换机/队列全部声明为 durable，消息投递时设置持久化，
 * 保证 broker 重启后「已确认入队但未消费」的订单任务不丢。
 */
public final class MqConstants {

    /** 秒杀下单业务交换机（direct） */
    public static final String SECKILL_EXCHANGE = "seckill.order.exchange";

    /** 秒杀下单队列：正常链路 */
    public static final String SECKILL_ORDER_QUEUE = "seckill.order.queue";

    /** 秒杀下单路由键 */
    public static final String SECKILL_ORDER_ROUTING_KEY = "seckill.order";

    /** 死信交换机 */
    public static final String SECKILL_DLX = "seckill.order.dlx";

    /** 死信队列：重试耗尽 / 被拒绝的消息落到这里做补偿 */
    public static final String SECKILL_DLQ = "seckill.order.dlq";

    /** 死信路由键 */
    public static final String SECKILL_DLQ_ROUTING_KEY = "seckill.order.dlq";

    private MqConstants() {
    }
}
