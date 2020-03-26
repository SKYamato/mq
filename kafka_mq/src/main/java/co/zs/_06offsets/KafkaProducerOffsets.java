package co.zs._06offsets;

import co.zs.util.KafkaUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * @author shuai
 * @date 2020/03/19 15:07
 */
public class KafkaProducerOffsets {
    public static void main(String[] args) {
        KafkaProducer<String, String> producer = KafkaUtil.createKafkaProducer();
        //发送消息
        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<>("topic01", 1, "key" + i, "value" + i));
        }
        KafkaUtil.closeProducer(producer);
    }
}
