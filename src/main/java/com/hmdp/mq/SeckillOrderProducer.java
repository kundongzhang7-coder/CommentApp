package com.hmdp.mq;

import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

/**
 * 秒杀下单消息生产者。
 * <p>
 * 这里做的是「同步等待 broker 确认」而不是 fire-and-forget：
 * Redis Lua 已经把库存扣了、一人一单也标记了，如果消息其实没进队列却返回订单号给前端，
 * 就会出现“用户看到下单成功、后台永远没有订单”的资损问题。
 * 所以只有 confirm 拿到 ack 且没有被退回时，才认为下单请求成立。
 */
@Slf4j
@Component
public class SeckillOrderProducer {

    /** 等待 broker 确认的超时时间：秒杀接口是同步响应，不能无限等 */
    private static final long CONFIRM_TIMEOUT_SECONDS = 3L;

    private final RabbitTemplate rabbitTemplate;

    public SeckillOrderProducer(RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;
    }

    /**
     * 投递订单消息，失败抛出异常由调用方决定补偿。
     *
     * @throws IllegalStateException broker 拒收、超时未确认或消息未路由到任何队列
     */
    public void send(SeckillOrderMessage message) {
        String messageId = String.valueOf(message.getOrderId());
        CorrelationData correlationData = new CorrelationData(messageId);
        try {
            rabbitTemplate.convertAndSend(
                    MqConstants.SECKILL_EXCHANGE,
                    MqConstants.SECKILL_ORDER_ROUTING_KEY,
                    message,
                    msg -> {
                        msg.getMessageProperties().setMessageId(messageId);
                        // 持久化：broker 落盘后才算真正收下这条消息
                        msg.getMessageProperties().setDeliveryMode(MessageDeliveryMode.PERSISTENT);
                        return msg;
                    },
                    correlationData);

            CorrelationData.Confirm confirm = correlationData.getFuture()
                    .get(CONFIRM_TIMEOUT_SECONDS, TimeUnit.SECONDS);

            if (confirm == null || !confirm.isAck()) {
                throw new IllegalStateException("broker 未确认消息: orderId=" + messageId
                        + ", reason=" + (confirm == null ? "confirm timeout" : confirm.getReason()));
            }
            // 交换机存在但没有任何队列匹配路由键时，ack 依然可能为 true，要靠 returned 判断
            if (correlationData.getReturned() != null) {
                throw new IllegalStateException("消息未路由到队列: orderId=" + messageId
                        + ", replyText=" + correlationData.getReturned().getReplyText());
            }
            log.info("秒杀消息已入队 orderId={} userId={} voucherId={}",
                    message.getOrderId(), message.getUserId(), message.getVoucherId());
        } catch (IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("秒杀消息投递失败 orderId=" + messageId, e);
        }
    }
}
