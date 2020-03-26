package co.zs._04serializer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;

/**
 * kafka消费组
 *
 * @author shuai
 * @date 2020/03/19 14:35
 */
public class KafkaConsumerQuickStart {
    public static void main(String[] args) {
        Properties pros = new Properties();
        //连接地址
        pros.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "CentOS:9092");
        //反序列化
        pros.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        pros.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, UserDefineDeserializer.class.getName());
        //配置消费组
        pros.put(ConsumerConfig.GROUP_ID_CONFIG, "g2");
        //获取消费者
        KafkaConsumer<String, User> consumer = new KafkaConsumer<>(pros);
        //订阅消息
        consumer.subscribe(Arrays.asList("topic02"));
        //处理结果
        while (true) {
            //每1s轮询一次
            ConsumerRecords<String, User> consumerRecords = consumer.poll(Duration.ofSeconds(1));
            //从队列中去到了数据
            if (!consumerRecords.isEmpty()) {
                Iterator<ConsumerRecord<String, User>> recordIterator = consumerRecords.iterator();
                while (recordIterator.hasNext()) {
                    //获取一个消息
                    ConsumerRecord<String, User> record = recordIterator.next();
                    //topic
                    String topic = record.topic();
                    //分区信息
                    int partition = record.partition();
                    //偏移量
                    long offset = record.offset();
                    //key
                    String key = record.key();
                    //value
                    User user = record.value();
                    //时间戳
                    long timestamp = record.timestamp();
                    System.out.println("topic:" + topic + "\t"
                            + "offset:" + offset + "\t"
                            + "partition:" + partition + "\t"
                            + "key:" + key + "\t"
                            + "value:" + user.toString() + "\t"
                            + "timestamp:" + timestamp);
                }
            }
        }
    }
}
