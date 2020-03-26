package co.zs._07acks_retry;

import co.zs.util.KafkaUtil;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * 默认consumer初始offset策略
 *
 * @author shuai
 * @date 2020/03/19 15:07
 */
public class KafkaConsumerAcks {
    public static void main(String[] args) {
        Properties pros = KafkaUtil.getConsumerBaseProperties();
        pros.put(ConsumerConfig.GROUP_ID_CONFIG, "jc1");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(pros);
        //订阅相关的topics
        consumer.subscribe(Arrays.asList("topic01"));
        //处理结果
        KafkaUtil.getConsumerResult(consumer);
    }
}
