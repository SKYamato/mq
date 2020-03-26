package co.zs._01quick_start.v2;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

/**
 * 消息独占 消费者
 *
 * @author shuai
 * @date 2020/03/24 11:22
 */
public class ActiveMQConsumerExclusive {
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

        //4、获取destination，消费者会从这里取消息
        /**
         * 独占消费者
         *    ?consumer.exclusive=true
         */
        Destination queue = session.createQueue("user?consumer.exclusive=true");

        //5、创建consumer，获取消息
        MessageConsumer consumer = session.createConsumer(queue);

        while (true) {
            TextMessage receive = (TextMessage) consumer.receive();
            System.out.println(receive.getText());
        }
    }
}
