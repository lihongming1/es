package com.espro.jest.kafka;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaConsumer {
    /**
     * 监听seckill主题,有消息就读取
     * @param message
     */
    @KafkaListener(topics = {"test_es"})
    public void receiveMessage(String message){
        //收到通道的消息之后执行秒杀操作

        System.out.println(message);

    }
}
