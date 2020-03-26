package co.zs._09tranactions.v1_producer_only;

import co.zs.util.KafkaUtil;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * kafka消费者 设置隔离级别
 *
 * @author shuai
 * @date 2020/03/20 10:06
 */
public class KafkaConsumerTransactionReadCommitted {
    public static void main(String[] args) {
        //创建订阅形式的KafkaConsumer
        Properties props = KafkaUtil.getConsumerBaseProperties();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "se1");
        /**
         * 设置消费者事务隔离级别：read_committed
         */
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG,"read_committed");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        //订阅相关的topics
        consumer.subscribe(Arrays.asList("topic01"));
        //处理消息信息
        KafkaUtil.getConsumerResult(consumer);
    }
}
