package co.zs._06offsets.v3;

import co.zs.util.KafkaUtil;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * 配置offset自动提交的时间间隔（默认5s）
 *
 * @author shuai
 * @date 2020/03/19 15:07
 */
public class KafkaConsumerOffsetsAutoCommit {
    public static void main(String[] args) {
        Properties pros = KafkaUtil.getConsumerBaseProperties();
        pros.put(ConsumerConfig.GROUP_ID_CONFIG, "a3p");
        pros.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        /**
         * 配置offset自动提交的时间间隔，5000ms
         */
        pros.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 10000);
        /**
         * offset偏移量自动提交
         */
        pros.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(pros);
        //订阅相关的topics
        consumer.subscribe(Arrays.asList("topic01"));
        //处理结果
        KafkaUtil.getConsumerResult(consumer);
    }
}
