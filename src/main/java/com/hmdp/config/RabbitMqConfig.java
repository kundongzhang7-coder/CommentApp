package com.hmdp.config;

import com.hmdp.mq.MqConstants;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.boot.autoconfigure.amqp.RabbitTemplateCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * RabbitMQ 拓扑与消费者容器配置。
 * <pre>
 *  生产者 ──> seckill.order.exchange ──(seckill.order)──> seckill.order.queue
 *                                                            │ 消费重试耗尽 / nack(requeue=false)
 *                                                            ▼
 *                                              seckill.order.dlx ──> seckill.order.dlq
 *                                                          （DLQ 消费者做 Redis 预占补偿 + 失败快照）
 * </pre>
 * 三个关键约束：
 * 1. 交换机、队列、消息全部持久化（durable + PERSISTENT），broker 重启不丢未消费任务；
 * 2. 生产者开启 confirm + return（mandatory），消息没进队列就地补偿，不让前端拿到“假订单号”；
 * 3. 消费者手动 ACK + prefetch=1：一条一条确认，宕机时最多只有 1 条未确认消息需要重投。
 */
@Configuration
public class RabbitMqConfig {

    /** 业务交换机 */
    @Bean
    public DirectExchange seckillOrderExchange() {
        return ExchangeBuilder.directExchange(MqConstants.SECKILL_EXCHANGE).durable(true).build();
    }

    /** 业务队列：绑定死信交换机，消息被 nack 后自动路由到 DLQ */
    @Bean
    public Queue seckillOrderQueue() {
        return QueueBuilder.durable(MqConstants.SECKILL_ORDER_QUEUE)
                .deadLetterExchange(MqConstants.SECKILL_DLX)
                .deadLetterRoutingKey(MqConstants.SECKILL_DLQ_ROUTING_KEY)
                .build();
    }

    @Bean
    public Binding seckillOrderBinding() {
        return BindingBuilder.bind(seckillOrderQueue())
                .to(seckillOrderExchange())
                .with(MqConstants.SECKILL_ORDER_ROUTING_KEY);
    }

    /** 死信交换机 */
    @Bean
    public DirectExchange seckillDeadLetterExchange() {
        return ExchangeBuilder.directExchange(MqConstants.SECKILL_DLX).durable(true).build();
    }

    /** 死信队列 */
    @Bean
    public Queue seckillDeadLetterQueue() {
        return QueueBuilder.durable(MqConstants.SECKILL_DLQ).build();
    }

    @Bean
    public Binding seckillDeadLetterBinding() {
        return BindingBuilder.bind(seckillDeadLetterQueue())
                .to(seckillDeadLetterExchange())
                .with(MqConstants.SECKILL_DLQ_ROUTING_KEY);
    }

    /** 消息体用 JSON，可读、可跨语言排查；Spring Boot 会自动把这个 converter 挂到 RabbitTemplate */
    @Bean
    public MessageConverter jsonMessageConverter() {
        return new Jackson2JsonMessageConverter();
    }

    /** 生产者侧：mandatory=true，路由不到队列时回调 returns，由生产端判定失败并补偿 */
    @Bean
    public RabbitTemplateCustomizer rabbitTemplateCustomizer() {
        return (RabbitTemplate template) -> {
            template.setMandatory(true);
            template.setConfirmCallback((correlationData, ack, cause) -> {
                if (!ack && correlationData != null) {
                    // 具体失败处理在 SeckillOrderProducer 里通过 CorrelationData 的 future 完成，
                    // 这里只兜底打日志，便于排查“needed but not routed”的情况
                    System.err.println("发往 broker 的消息被拒绝 id=" + correlationData.getId() + " cause=" + cause);
                }
            });
            template.setReturnsCallback(returned ->
                    System.err.println("消息未路由到队列: exchange=" + returned.getExchange()
                            + " routingKey=" + returned.getRoutingKey()
                            + " replyText=" + returned.getReplyText()));
        };
    }

    /**
     * 业务队列消费容器：手动 ACK、预取 1、2 个并发消费者。
     * 预取 1 是刻意的——秒杀落库是 IO 密集且每条都要抢 Redisson 锁，
     * 预取太多只会把消息堆在单个消费者的内存里，一是影响 broker 的公平分发，二是宕机时丢的更多。
     */
    @Bean
    public SimpleRabbitListenerContainerFactory seckillOrderContainerFactory(
            ConnectionFactory connectionFactory, MessageConverter jsonMessageConverter) {
        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        factory.setConnectionFactory(connectionFactory);
        factory.setMessageConverter(jsonMessageConverter);
        factory.setAcknowledgeMode(AcknowledgeMode.MANUAL);
        factory.setPrefetchCount(1);
        factory.setConcurrentConsumers(2);
        factory.setMaxConcurrentConsumers(4);
        // 业务异常统一由消费者自己 nack 进死信，容器不再自动重投，避免无限循环
        factory.setDefaultRequeueRejected(false);
        // broker 启动慢于应用时不因“队列不存在”直接停掉容器
        factory.setMissingQueuesFatal(false);
        return factory;
    }

    /** 死信队列消费容器：单并发，预取 5，同样手动 ACK */
    @Bean
    public SimpleRabbitListenerContainerFactory seckillDlqContainerFactory(
            ConnectionFactory connectionFactory, MessageConverter jsonMessageConverter) {
        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        factory.setConnectionFactory(connectionFactory);
        factory.setMessageConverter(jsonMessageConverter);
        factory.setAcknowledgeMode(AcknowledgeMode.MANUAL);
        factory.setPrefetchCount(5);
        factory.setMissingQueuesFatal(false);
        return factory;
    }
}
