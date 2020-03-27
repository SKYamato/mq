package co.zs._07prefetch;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

/**
 * 消息消费者
 *
 * @author shuai
 * @date 2020/03/24 11:22
 */
public class ConsumerPrefetch {
    public static void main(String[] args) throws Exception {
        //1、获取连接工厂
        /**
         * 创建连接时，接受MQ主动推送的消息时，声明prefetchSize缓冲区大小（内存中未消费的消息）
         *    jms.prefetchPolicy.all=1
         *    jms.prefetchPolicy.queuePrefetch=1&jms.prefetchPolicy.topicPrefetch=1
         *
         */
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(
                ActiveMQConnectionFactory.DEFAULT_USER,
                ActiveMQConnectionFactory.DEFAULT_PASSWORD,
                "tcp://localhost:61616?jms.prefetchPolicy.queuePrefetch=1&jms.prefetchPolicy.topicPrefetch=1");

        //2、获取一个ActiveMQ连接
        Connection connection = connectionFactory.createConnection();
        connection.start();

        //3、获取session
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        //4、获取destination，消费者会从这里取消息
        /**
         * 针对destination设置缓冲区大小（内存中未消费的消息）
         */
        Destination queue = session.createQueue("user？consumer.prefetchSize=10");

        //5、创建consumer，获取消息
        MessageConsumer consumer = session.createConsumer(queue);

        while (true) {
            TextMessage receive = (TextMessage) consumer.receive();
            System.out.println(receive.getText());
        }
    }
}
