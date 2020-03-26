package co.zs._05interceptors;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * 配置拦截器
 *
 * @author shuai
 * @date 2020/03/19 14:46
 */
public class KafkaProducerInterceptors {
    public static void main(String[] args) {
        Properties pros = new Properties();
        //连接地址
        pros.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "CentOS:9092");
        //序列化
        pros.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        pros.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //配置拦截器
        pros.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, UserDefineProducerInterceptor.class.getName());
        //获取producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(pros);
        //发送消息
        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<>("topic01", "key" + i, "value" + i));
        }
        //关闭
        producer.close();
    }
}
