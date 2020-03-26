package co.zs._07acks_retry;

import co.zs.util.KafkaUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * 生产消息
 *
 * @author shuai
 * @date 2020/03/19 9:15
 */
public class KafkaProducerAcks {

    public static void main(String[] args) {
        Properties props = KafkaUtil.getProducerBaseProperties();
        /**
         * 设置kafka acks和retry
         */
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        /**
         * 设置重试次数，不包含第一次发送，如果尝试发送三次失败则系统放弃发送
         */
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        /**
         * 设置超时响应时间为1ms
         */
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 1);

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        //生产者发送消息
        //创建消息
        ProducerRecord<String, String> record =
                new ProducerRecord<>("topic01", "ack", "test ack");
        //发送消息
        producer.send(record);
        producer.flush();
        KafkaUtil.closeProducer(producer);
    }
}
