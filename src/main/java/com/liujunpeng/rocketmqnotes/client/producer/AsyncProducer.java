package com.liujunpeng.rocketmqnotes.client.producer;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

import java.nio.charset.StandardCharsets;

/**
 * @Description: 异步消息发送
 * @Author: liujunpeng
 * @Date: 2021/7/14 14:59
 * @Version: 1.0
 */
public class AsyncProducer {
    public static void main(String[] args) throws Exception {

        //实例化生产者
        DefaultMQProducer producer = new DefaultMQProducer("producer-group-name");
        //设置nameServe地址
        producer.setNamesrvAddr("www.liujunpeng.com:9876");
        //启动生产者
        producer.start();
        //设置异步模式下消息重试投递次数
        producer.setRetryTimesWhenSendAsyncFailed(0);

        for (int i = 0; i <= 100; i++) {
            final int index = i;
            Message message = new Message("async-message", "async-tag", "Order", "Hello RocketMQ".getBytes(StandardCharsets.UTF_8));
            producer.send(message, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    System.out.printf("%-10d OK %s %n", index,
                            sendResult.getMsgId());
                }

                @Override
                public void onException(Throwable throwable) {
                    System.out.printf("%-10d Exception %s %n", index, throwable);
                    throwable.printStackTrace();
                }
            });
            Thread.sleep(500);
        }
        producer.shutdown();
    }
}
