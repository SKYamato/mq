package co.zs.topic._02retroactive;

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
                "tcp://localhost:5671");

        //2、获取一个ActiveMQ连接
        Connection connection = connectionFactory.createConnection();
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination topic = session.createTopic("user_topic");

        //5、创建producer，
        MessageProducer producer = session.createProducer(topic);

        //写入消息
        for (int i = 0; i < 10; i++) {
            TextMessage message = session.createTextMessage("topic_message" + i);
            producer.send(message);
        }

        //6、关闭连接
        connection.close();
    }
}
