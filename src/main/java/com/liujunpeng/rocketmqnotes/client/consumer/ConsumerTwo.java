package com.liujunpeng.rocketmqnotes.client.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

/**
 * @Description: 消费者2
 * @Author: liujunpeng
 * @Date: 2021/7/14 15:20
 * @Version: 1.0
 */
public class ConsumerTwo {
    public static void main(String[] args) throws Exception {
        //创建消费者，推送方式消费
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("consumer-group-name");
        //设置nameServe
        consumer.setNamesrvAddr("www.liujunpeng.com:9876");
        // 订阅一个或者多个Topic，以及Tag来过滤需要消费的消息
        consumer.subscribe("sync-message", "*");
        //集群模式消费
        consumer.setMessageModel(MessageModel.BROADCASTING);
        //注册回调实现类来处理从broker拉取回来的消息
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            msgs.forEach(messageExt -> System.out.println(new String(messageExt.getBody())));
            // 标记该消息已经被成功消费
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        //启动消费者
        consumer.start();
        System.out.println("启动成功");
    }
}
