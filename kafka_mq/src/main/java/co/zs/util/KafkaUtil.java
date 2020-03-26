package co.zs.util;

import co.zs._03patition.UserDefinePartitioner;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.*;

/**
 * kafka工具类
 *
 * @author shuai
 * @date 2020/03/19 9:16
 */
public class KafkaUtil {
    /**
     * 创建kafka客户端
     *
     * @return kafkaAdminClient
     */
    public static KafkaAdminClient createClient() {
        //设置链接参数
        Properties props = new Properties();
        //需要在host文件中做ip和主机名的映射
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "CentOS:9092");
        return (KafkaAdminClient) KafkaAdminClient.create(props);
    }

    /**
     * 构建生产者
     *
     * @return
     */
    public static KafkaProducer<String, String>     buildProducerWithTransaction() {
        //设置链接参数
        Properties props = new Properties();
        //需要在host文件中做ip和主机名的映射
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "CentOS:9092");
        //序列化key-value
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        /**
         * 配置transaction id必须唯一
         */
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transaction-id-" + UUID.randomUUID().toString());
        /**
         * 配置kafka的批处理大小，默认16384
         */
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 1024);
        /**
         * 配置kafka的等待时间，默认是0（batch未满，时间到了后发送数据）
         */
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        /**
         * 配置retry和幂等
         */
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 20000);
        props.put(ProducerConfig.RETRIES_CONFIG, 3);

        return new KafkaProducer<>(props);
    }


    /**
     * 构建消费者
     *
     * @return
     */
    public static KafkaConsumer<String, String> buildConsumerWithTransaction(String groupId) {
        //设置链接参数
        Properties props = new Properties();
        //需要在host文件中做ip和主机名的映射
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "CentOS:9092");
        //反序列化key-value
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //分组id
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        /**
         * 设置消费者事务隔离级别：read_committed
         */
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        /**
         * 必须关闭消费者offset自动提交
         */
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return new KafkaConsumer<>(props);
    }

    public static void getConsumerResultWithTransactions(KafkaProducer<String, String> producer, KafkaConsumer<String, String> consumer, String groupId) {
        while (true) {
            //每1s轮询一次
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(1));
            //从队列中去到了数据
            if (!consumerRecords.isEmpty()) {
                Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                Iterator<ConsumerRecord<String, String>> recordIterator = consumerRecords.iterator();
                /**
                 * 开启事务
                 */
                producer.beginTransaction();
                //业务处理
                try {
                    while (recordIterator.hasNext()) {
                        //获取一个消息
                        ConsumerRecord<String, String> record = recordIterator.next();
                        /**
                         * 存储元数据
                         */
                        offsets.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1));
                        /**
                         * 业务逻辑
                         */
                        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("topic02", record.key(), record.value() + "_copy");
                        producer.send(producerRecord);
                        /**
                         * 提交事务
                         */
                        //提交消费者的偏移量
                        producer.sendOffsetsToTransaction(offsets, groupId);
                        //提交生产者的偏移量
                        producer.commitTransaction();
                    }
                } catch (Exception e) {
                    System.out.println("出现异常" + e.getMessage());
                    /**
                     * 终止事务
                     */
                    producer.abortTransaction();
                }
            }
        }
    }

    /**
     * 创建KafkaProducer
     *
     * @return kafkaProducer
     */
    public static KafkaProducer<String, String> createKafkaProducer() {
        Properties props = getProducerBaseProperties();
        return new KafkaProducer<>(props);
    }

    /**
     * 自定义partition
     *
     * @return
     */
    public static KafkaProducer<String, String> createKafkaProducerWithPartition() {
        Properties props = getProducerBaseProperties();
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, UserDefinePartitioner.class.getName());
        return new KafkaProducer<>(props);
    }

    /**
     * 获取kafka生产者配置信息
     *
     * @return
     */
    public static Properties getProducerBaseProperties() {
        //设置链接参数
        Properties props = new Properties();
        //需要在host文件中做ip和主机名的映射
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "CentOS:9092");
        //序列化key-value
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return props;
    }

    /**
     * 创建带分组的KafkaConsumer
     *
     * @return kafkaConsumer
     */
    public static KafkaConsumer<String, String> createKafkaConsumerWithGroup(String groupId) {
        Properties props = getConsumerBaseProperties();
        //配置消费者的组信息
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        return new KafkaConsumer<>(props);
    }

    /**
     * 创建未分组的kafkaConsumer
     *
     * @return kafkaConsumer
     */
    public static KafkaConsumer<String, String> createKafkaConsumerWithOutGroup() {
        Properties props = getConsumerBaseProperties();
        return new KafkaConsumer<>(props);
    }

    /**
     * 获取KafkaConsumer配置信息
     *
     * @return properties
     */
    public static Properties getConsumerBaseProperties() {
        //设置链接参数
        Properties props = new Properties();
        //需要在host文件中做ip和主机名的映射
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "CentOS:9092");
        //反序列化key-value
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return props;
    }

    public static void getConsumerResult(KafkaConsumer<String, String> consumer) {
        while (true) {
            //每1s轮询一次
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(1));
            //从队列中去到了数据
            if (!consumerRecords.isEmpty()) {
                Iterator<ConsumerRecord<String, String>> recordIterator = consumerRecords.iterator();
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

    /**
     * 关闭客户端
     *
     * @param client
     */
    public static void closeClient(KafkaAdminClient client) {
        client.close();
    }

    /**
     * 关闭生产者
     *
     * @param producer
     */
    public static void closeProducer(KafkaProducer<String, String> producer) {
        producer.close();
    }

    /**
     * 关闭消费者
     *
     * @param consumer
     */
    public static void closeConsumer(KafkaConsumer<String, String> consumer) {
        consumer.close();
    }
}
