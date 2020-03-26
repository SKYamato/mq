package co.zs._03patition;

import co.zs.util.KafkaUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * 生产消息
 *
 * @author shuai
 * @date 2020/03/19 9:15
 */
public class KafkaProducerQuickStart {
    public static void main(String[] args) {
        KafkaProducer<String, String> producer = KafkaUtil.createKafkaProducerWithPartition();
        //生产者发送消息
        for (int i = 0; i < 30; i++) {
            //创建消息
            ProducerRecord<String, String> record =
                    new ProducerRecord<>("topic01", "value" + i);
                    //new ProducerRecord<>("topic01", "key" + i, "value" + i);
            //发送消息
            producer.send(record);
        }
        KafkaUtil.closeProducer(producer);
    }
}
