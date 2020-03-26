package co.zs._08idempotence;

import co.zs.util.KafkaUtil;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * @author shuai
 * @date 2020/03/20 10:06
 */
public class KafkaConsumerIdempotence {
    public static void main(String[] args) {
        Properties pros = KafkaUtil.getConsumerBaseProperties();
        pros.put(ConsumerConfig.GROUP_ID_CONFIG, "gc1");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(pros);
        //订阅相关的topics
        consumer.subscribe(Arrays.asList("topic01"));
        //处理结果
        KafkaUtil.getConsumerResult(consumer);
    }
}
