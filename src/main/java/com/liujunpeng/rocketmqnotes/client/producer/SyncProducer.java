package com.liujunpeng.rocketmqnotes.client.producer;


import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.nio.charset.StandardCharsets;

/**
 * @Description: 同步消息发送
 * @Author: liujunpeng
 * @Date: 2021/7/14 15:12
 * @Version: 1.0
 */
public class SyncProducer {
    public static void main(String[] args) throws MQClientException, MQBrokerException, RemotingException, InterruptedException {
        //实例化消息生产者Producer指定组名
        DefaultMQProducer producer = new DefaultMQProducer("producer-group-name");
        //设置nameserve地址
        producer.setNamesrvAddr("www.liujunpeng.com:9876");
        //启动
        producer.start();

        for (int i = 0; i <= 100; i++) {
            //创建消息，指定tipic，tag
            Message message = new Message("async-message", "sync-tag", ("HelloRocketMQ" + i).getBytes(StandardCharsets.UTF_8));
            // 发送消息到一个Broker
            SendResult sendResult = producer.send(message);
            System.out.println("同步发送消息返回结果：" + sendResult);
            Thread.sleep(1000);
        }
        //关闭mq
        producer.shutdown();
    }
}
