package com.alibaba.middleware.race.model;

import java.io.Serializable;
import java.util.Random;


/**
 * 我们后台RocketMq存储的交易消息模型类似于PaymentMessage，选手也可以自定义
 * 订单消息模型，只要模型中各个字段的类型和顺序和PaymentMessage一样，即可用Kryo
 * 反序列出消息
 */

public class SimplePaymentMessage implements Serializable{

    private static final long serialVersionUID = -4721410670774102273L;
    
    private long orderId; //订单ID

    private double payAmount; //金额

    /**
     * 支付平台
     * 0，pC
     * 1，无线
     */
    private short payPlatform; //支付平台

    /**
     * 付款记录创建时间
     */
    private int createTime; //13位数，毫秒级时间戳，初赛要求的时间都是指该时间

    //Kryo默认需要无参数构造函数
    public SimplePaymentMessage() {
    }


    public long getOrderId() {
        return orderId;
    }

    public void setOrderId(long orderId) {
        this.orderId = orderId;
    }

    public double getPayAmount() {
        return payAmount;
    }

    public void setPayAmount(double payAmount) {
        this.payAmount = payAmount;
    }



    public int getCreateTime() {
        return createTime;
    }

    public void setCreateTime(int createTime) {
        this.createTime = createTime;
    }

    public short getPayPlatform() {
        return payPlatform;
    }
    
    public void setPayPlatform(short payPlatform) {
        this.payPlatform = payPlatform;
    }
}
