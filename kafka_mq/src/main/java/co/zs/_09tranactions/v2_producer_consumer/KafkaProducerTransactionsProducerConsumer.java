package co.zs._09tranactions.v2_producer_consumer;

import co.zs.util.KafkaUtil;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Arrays;

/**
 * 生产者Only
 *
 * @author shuai
 * @date 2020/03/20 10:36
 */
public class KafkaProducerTransactionsProducerConsumer {

    public static void main(String[] args) {
        KafkaProducer<String, String> producer = KafkaUtil.buildProducerWithTransaction();
        KafkaConsumer<String, String> consumer = KafkaUtil.buildConsumerWithTransaction("cp1");
        /**
         * 1、初始化事务
         */
        producer.initTransactions();
        /**
         * 消费者订阅事务
         */
        consumer.subscribe(Arrays.asList("topic01"));
        KafkaUtil.getConsumerResultWithTransactions(producer, consumer, "cp1");

        try {
            /**
             * 2、开启事务
             */
            producer.beginTransaction();
            //生产者发送消息
            for (int i = 0; i < 10; i++) {

                //发送消息
                producer.flush();
            }
            /**
             * 3、事务提交
             */
            producer.commitTransaction();
        } catch (Exception e) {
            System.out.println("出现错误：" + e.getMessage());
            /**
             * 4、终止事务
             */
            producer.abortTransaction();
        } finally {
            KafkaUtil.closeProducer(producer);
        }
    }
}
