package co.zs.service.impl;

import co.zs.service.IMessageSender;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * @author shuai
 * @date 2020/03/20 14:50
 */
@Service
@Transactional
public class MessageSender implements IMessageSender {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Override
    public void sendMessage(String topic, String key, String value) {
        kafkaTemplate.send(new ProducerRecord<>(topic, key, value));
    }
}
