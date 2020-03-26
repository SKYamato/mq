package co.zs._06offsets.v4;

import co.zs.util.KafkaUtil;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;

/**
 * 手动提交offset
 *
 * @author shuai
 * @date 2020/03/19 15:07
 */
public class KafkaConsumerOffsetsAutoCommit {
    public static void main(String[] args) {
        Properties pros = KafkaUtil.getConsumerBaseProperties();
        pros.put(ConsumerConfig.GROUP_ID_CONFIG, "a4p");
        pros.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        /**
         * offset偏移量不自动提交
         */
        pros.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(pros);
        //订阅相关的topics
        consumer.subscribe(Arrays.asList("topic01"));
        //处理结果
        while (true) {
            //每1s轮询一次
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(1));
            //从队列中去到了数据
            if (!consumerRecords.isEmpty()) {
                Iterator<ConsumerRecord<String, String>> recordIterator = consumerRecords.iterator();
                /**
                 * 记录分区消费的元数据信息
                 * key为分区位置，offset元数据信息
                 */
                Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();

                while (recordIterator.hasNext()) {
                    //获取一个消息
                    ConsumerRecord<String, String> record = recordIterator.next();
                    //topic
                    String topic = record.topic();
                    //分区信息
                    int partition = record.partition();
                    //偏移量
                    long offset = record.offset();
                    //key
                    String key = record.key();
                    //value
                    String value = record.value();
                    //时间戳
                    long timestamp = record.timestamp();
                    /**
                     * 记录消费分区的偏移量，因为每次提交的都是偏移量+1，所以最后一条数据没有提交成功，会一直读取到最后一条数据
                     */
                    //offsets.put(new TopicPartition(topic, partition), new OffsetAndMetadata(offset));
                    /**
                     * 此时提交了最后一条数据的位置，规避重复消费的问题
                     */
                    offsets.put(new TopicPartition(topic, partition), new OffsetAndMetadata(offset + 1));
                    /**
                     * 异步提交，重写回调方法
                     */
                    consumer.commitAsync(offsets, new OffsetCommitCallback() {
                        @Override
                        public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                            System.out.println("offsets:" + offsets + "\texception:" + exception);
                        }
                    });
                    System.out.println("topic:" + topic + "\t"
                            + "offset:" + offset + "\t"
                            + "partition:" + partition + "\t"
                            + "key:" + key + "\t"
                            + "value:" + value + "\t"
                            + "timestamp:" + timestamp);

                }
            }
        }
    }
}
