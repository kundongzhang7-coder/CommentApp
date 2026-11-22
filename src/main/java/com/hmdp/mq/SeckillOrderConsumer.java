package com.hmdp.mq;

import com.hmdp.config.MqProperties;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.service.IVoucherOrderService;
import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * 秒杀下单消费者：手动 ACK + 本地重试，重试耗尽后拒绝消息转入死信队列。
 * <p>
 * 与 Redis Stream 那套的区别：
 * - Stream 的 pending-list 只能靠 XREADGROUP 0 轮询捞回，重试状态得自己存 Redis；
 * - RabbitMQ 有 broker 侧的投递确认与 unacked 队列，nack 之后由 broker 负责路由到 DLX，
 *   应用侧不用再维护 pending 扫描线程，失败消息的去向是 broker 保证的。
 */
@Slf4j
@Component
public class SeckillOrderConsumer {

    private final IVoucherOrderService voucherOrderService;
    private final MqProperties mqProperties;

    public SeckillOrderConsumer(IVoucherOrderService voucherOrderService, MqProperties mqProperties) {
        this.voucherOrderService = voucherOrderService;
        this.mqProperties = mqProperties;
    }

    @RabbitListener(queues = MqConstants.SECKILL_ORDER_QUEUE,
            containerFactory = "seckillOrderContainerFactory")
    public void onMessage(SeckillOrderMessage message, Message amqpMessage, Channel channel) throws IOException {
        long deliveryTag = amqpMessage.getMessageProperties().getDeliveryTag();
        VoucherOrder order = toEntity(message);
        int maxRetry = mqProperties.getRetryMax();

        for (int attempt = 1; attempt <= maxRetry; attempt++) {
            try {
                if (voucherOrderService.handleOrder(order)) {
                    // 落库成功（或幂等命中已存在的订单）才 ACK
                    channel.basicAck(deliveryTag, false);
                    return;
                }
            } catch (Exception e) {
                log.warn("订单落库第 {}/{} 次失败 orderId={} reason={}",
                        attempt, maxRetry, order.getId(), e.getMessage());
            }
            if (attempt < maxRetry) {
                sleepQuietly(mqProperties.getRetryBackoffMillis() * attempt);
            }
        }

        // 重试耗尽：requeue=false，让 broker 按队列上的死信配置投到 seckill.order.dlq
        channel.basicNack(deliveryTag, false, false);
        log.error("订单任务重试耗尽，已转入死信队列 orderId={}", order.getId());
    }

    private void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private VoucherOrder toEntity(SeckillOrderMessage message) {
        VoucherOrder order = new VoucherOrder();
        order.setId(message.getOrderId());
        order.setUserId(message.getUserId());
        order.setVoucherId(message.getVoucherId());
        return order;
    }
}
