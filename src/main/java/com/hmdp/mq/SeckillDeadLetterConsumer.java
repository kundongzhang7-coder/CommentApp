package com.hmdp.mq;

import com.hmdp.entity.VoucherOrder;
import com.hmdp.service.IVoucherOrderService;
import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * 死信队列消费者：消息重试耗尽后的最后一站。
 * <p>
 * 做三件事：① 核对订单是否其实已落库（避免误补偿）；② 回滚 Redis 预占（库存 + 一人一单）；
 * ③ 写失败快照，留给人工补单或对账。
 * 处理完必须 ACK——死信再堆积没有意义，问题订单已经记在 order:fail:* 里了。
 */
@Slf4j
@Component
public class SeckillDeadLetterConsumer {

    private final IVoucherOrderService voucherOrderService;
    private final SeckillCompensator compensator;

    public SeckillDeadLetterConsumer(IVoucherOrderService voucherOrderService, SeckillCompensator compensator) {
        this.voucherOrderService = voucherOrderService;
        this.compensator = compensator;
    }

    @RabbitListener(queues = MqConstants.SECKILL_DLQ, containerFactory = "seckillDlqContainerFactory")
    public void onDeadLetter(SeckillOrderMessage message, Message amqpMessage, Channel channel) throws IOException {
        long deliveryTag = amqpMessage.getMessageProperties().getDeliveryTag();
        VoucherOrder order = new VoucherOrder();
        order.setId(message.getOrderId());
        order.setUserId(message.getUserId());
        order.setVoucherId(message.getVoucherId());

        log.error("收到死信消息 orderId={} userId={} voucherId={}",
                order.getId(), order.getUserId(), order.getVoucherId());
        try {
            // 先核对：极少数情况是事务已提交、ACK 前节点宕机导致消息被重投后判失败
            if (voucherOrderService.getById(order.getId()) != null) {
                log.warn("死信消息对应的订单已落库，跳过补偿 orderId={}", order.getId());
            } else {
                compensator.compensate(order);
                compensator.markFailed(order, "消费重试耗尽，消息进入死信队列");
            }
        } catch (Exception e) {
            // 补偿本身失败（例如 Redis 不可用）：仍 ACK 避免死信无限堆积，把现场写进日志和快照
            log.error("死信补偿失败，需人工介入 orderId={}", order.getId(), e);
            compensator.markFailed(order, "死信补偿失败: " + e.getMessage());
        } finally {
            channel.basicAck(deliveryTag, false);
        }
    }
}
