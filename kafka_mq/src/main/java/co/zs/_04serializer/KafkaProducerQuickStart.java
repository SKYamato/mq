package co.zs._04serializer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Date;
import java.util.Properties;

/**
 * kafka生产者
 *
 * @author shuai
 * @date 2020/03/19 14:29
 */
public class KafkaProducerQuickStart {
    public static void main(String[] args) {
        Properties pros = new Properties();
        //连接地址
        pros.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "CentOS:9092");
        //序列化
        pros.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        pros.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, UserDefineSerializer.class.getName());
        //获取producer
        KafkaProducer<String, User> producer = new KafkaProducer<String, User>(pros);
        //发送消息
        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<>("topic02", "key" + i, new User(i, "user" + i, new Date())));
        }
        //关闭producer
        producer.close();
    }
}
