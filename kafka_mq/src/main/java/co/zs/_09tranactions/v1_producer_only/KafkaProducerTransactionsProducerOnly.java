package co.zs._09tranactions.v1_producer_only;

import co.zs.util.KafkaUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * 生产者Only
 *
 * @author shuai
 * @date 2020/03/20 10:06
 */
public class KafkaProducerTransactionsProducerOnly {

    public static void main(String[] args) {
        KafkaProducer<String, String> producer = KafkaUtil.buildProducerWithTransaction();
        /**
         * 1、初始化事务
         */
        producer.initTransactions();
        try {
            /**
             * 2、开启事务
             */
            producer.beginTransaction();
            //生产者发送消息
            for (int i = 0; i < 10; i++) {
                //测试异常
                if (i == 8) {
                    int j = 1 / 0;
                }
                //创建消息
                ProducerRecord<String, String> record =
                        new ProducerRecord<>("topic01", "transaction" + i, "error data" + i);
                //发送消息
                producer.send(record);
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
