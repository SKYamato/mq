package co.zs;

import co.zs.service.IMessageSender;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * @author shuai
 * @date 2020/03/20 14:54
 */
@SpringBootTest(classes = KafkaSpringBootApplication.class)
@RunWith(SpringRunner.class)
public class MessageSenderTest {
    @Autowired
    private IMessageSender messageSender;

    /**
     * 发送消息
     */
    @Test
    public void testMessageSender() {
        messageSender.sendMessage("topic01", "springboot_03", "from_spring_boot_service");
    }
}
