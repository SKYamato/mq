package co.zs._04pub_sub;

import lombok.SneakyThrows;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

/**
 * 订阅消息消费者
 *
 * @author shuai
 * @date 2020/03/24 11:22
 */
public class ActiveMQConsumerTopic {
    @lombok.SneakyThrows
    public static void main(String[] args) {
        //1、获取连接工厂
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(
                ActiveMQConnectionFactory.DEFAULT_USER,
                ActiveMQConnectionFactory.DEFAULT_PASSWORD,
                "tcp://localhost:61616");

        //2、获取一个ActiveMQ连接
        Connection connection = connectionFactory.createConnection();
        connection.start();

        //3、获取session
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        /**
         * 4、获取topic的destination，消费者会从这里取消息
         */
        Destination topic = session.createTopic("user_topic");

        //5、创建consumer
        MessageConsumer consumer = session.createConsumer(topic);

        /**
         * 6、使用监听器获取方法
         */
        consumer.setMessageListener(new MessageListener() {
            @SneakyThrows
            public void onMessage(Message message) {
                System.out.println(((TextMessage) message).getText());
            }
        });
    }
}