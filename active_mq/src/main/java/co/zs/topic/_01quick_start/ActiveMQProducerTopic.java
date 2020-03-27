package co.zs.topic._01quick_start;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

/**
 * 广播消息生产者
 *
 * @author shuai
 * @date 2020/03/24 10:50
 */
public class ActiveMQProducerTopic {
    public static void main(String[] args) throws Exception {
        //1、获取连接工厂
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(
                ActiveMQConnectionFactory.DEFAULT_USER,
                ActiveMQConnectionFactory.DEFAULT_PASSWORD,
                "tcp://localhost:61616");

        //2、获取一个ActiveMQ连接
        Connection connection = connectionFactory.createConnection();
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        /**
         * 获取topic的destination，消费者会从这里取消息
         */
        Destination topic = session.createTopic("user_topic");
//        /**
//         * 获取本连接内可见的临时topic
//         */
//        Destination temporaryTopic = session.createTemporaryTopic();

        //5、创建producer，
        MessageProducer producer = session.createProducer(topic);

        /**
         * 设置time to alive
         * 超时进入死信队列
         */
        producer.setTimeToLive(1000);

        //写入消息
        for (int i = 0; i < 10; i++) {
            TextMessage message = session.createTextMessage("topic_message" + i);
            producer.send(message);
        }

        //6、关闭连接
        connection.close();
    }
}
