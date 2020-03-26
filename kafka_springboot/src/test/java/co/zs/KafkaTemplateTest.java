package co.zs;

import co.zs.service.IMessageSender;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * @author shuai
 * @date 2020/03/20 14:38
 */
@SpringBootTest(classes = KafkaSpringBootApplication.class)
@RunWith(SpringRunner.class)
public class KafkaTemplateTest {
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    /**
     * 非事务下执行
     */
    @Test
    public void testSend01() {
        kafkaTemplate.send(new ProducerRecord<String, String>("topic01", "springboot_01", "from_springboot"));
    }

    /**
     * 事务环境下执行
     */
    @Test
    public void testSend02() {
        kafkaTemplate.executeInTransaction(new KafkaOperations.OperationsCallback<String, String, Object>() {
            @Override
            public Object doInOperations(KafkaOperations<String, String> kafkaOperations) {
                kafkaOperations.send(new ProducerRecord<String, String>("topic01", "springboot_02", "from_springboot_with_transaction"));
                return null;
            }
        });
    }
}
