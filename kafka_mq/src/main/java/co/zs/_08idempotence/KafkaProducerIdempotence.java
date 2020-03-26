package co.zs._08idempotence;

import co.zs.util.KafkaUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * 生产消息
 *
 * @author shuai
 * @date 2020/03/20 10:06
 */
public class KafkaProducerIdempotence {

    public static void main(String[] args) {
        Properties props = KafkaUtil.getProducerBaseProperties();
        /**
         * 开启幂等性
         */
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        /**
         * 没有正确应答的次数，超过该值时，会阻塞客户端，默认是 5
         */
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        //必须设置kafka acks和retry
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        //必须设置重试次数，不包含第一次发送，如果尝试发送三次失败则系统放弃发送
        props.put(ProducerConfig.RETRIES_CONFIG, 3);

        //测试设置超时响应时间为1ms
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10);
        //创建生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        //生产者发送消息
        //创建消息
        ProducerRecord<String, String> record =
                new ProducerRecord<>("topic01", "idempotence", "test idempotence");
        //发送消息
        producer.send(record);
        producer.flush();
        KafkaUtil.closeProducer(producer);
    }
}
