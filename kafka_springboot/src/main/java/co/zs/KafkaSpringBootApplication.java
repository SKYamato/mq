package co.zs;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.KafkaListeners;
import org.springframework.messaging.handler.annotation.SendTo;

import java.io.IOException;

/**
 * @author shuai
 * @date 2020/03/20 14:22
 */
@SpringBootApplication
public class KafkaSpringBootApplication {
    public static void main(String[] args) throws IOException {
        SpringApplication.run(KafkaSpringBootApplication.class, args);
        System.in.read();
    }

    @KafkaListeners(
            value = {
                    @KafkaListener(topics = {"topic02"})
            }
    )

    public void recevice01(ConsumerRecord<String, String> record) {
        System.out.println("record:" + record);
    }

//---------------------------------------增加返回值-----------------------------------------//

    /**
     * 监听topic01的数据，写入topic02
     *
     * @param record
     * @return
     */
    @KafkaListeners(
            value = {
                    @KafkaListener(topics = {"topic01"})
            }
    )
    @SendTo("topic02")
    public String recevice02(ConsumerRecord<String, String> record) {
        return record.value() + "\t" + "_copy";
    }
}
