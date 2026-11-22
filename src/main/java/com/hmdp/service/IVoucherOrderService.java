package com.hmdp.service;

import com.baomidou.mybatisplus.extension.service.IService;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;

/**
 * <p>
 *  服务类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
public interface IVoucherOrderService extends IService<VoucherOrder> {

    /**
     * 秒杀优惠券
     * @param voucherId
     * @return
     */
    Result seckillVoucher(Long voucherId);

    void createVoucherOder(VoucherOrder voucherOrder);

    /**
     * 消息消费入口：执行真正的落库（抢锁 + 事务），失败抛出异常交给消费者重试。
     *
     * @param voucherOrder 从 MQ 消息还原出的订单任务
     * @return true 表示落库成功（含幂等命中）
     */
    boolean handleOrder(VoucherOrder voucherOrder);
}
