package com.liujunpeng.rocketmqnotes.client.producer;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;

import java.nio.charset.StandardCharsets;

/**
 * @Description: 单项消息发送
 * @Author: liujunpeng
 * @Date: 2021/7/14 15:12
 * @Version: 1.0
 */
public class OnewayProducer {
    public static void main(String[] args) throws Exception {

        //创建生产者
        DefaultMQProducer producer = new DefaultMQProducer("producer-group-name");
        //设置nameServe地址
        producer.setNamesrvAddr("www.liujunpeng.com:9876");
        //启动
        producer.start();

        for (int i = 0; i <= 100; i++) {
            Message message = new Message("oneway-message", "oneway-tag", ("Hello RocketMQ" + i).getBytes(StandardCharsets.UTF_8));
            // 发送单向消息，没有任何返回结果
            producer.sendOneway(message);
            Thread.sleep(500);
        }
        //停止生产者
        producer.shutdown();
    }
}
