package co.zs._01quick_start.v3;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

/**
 * 消息生产者消息分组
 *
 * @author shuai
 * @date 2020/03/24 10:50
 */
public class ActiveMQProducerProperty {
    @lombok.SneakyThrows
    public static void main(String[] args) {
        //1、获取连接工厂
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(
                ActiveMQConnectionFactory.DEFAULT_USER,
                ActiveMQConnectionFactory.DEFAULT_PASSWORD,
                "tcp://localhost:61616");

        //2、获取一个ActiveMQ连接
        Connection connection = connectionFactory.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        //4、获取destination，消费者会从这里取消息
        Queue queue = session.createQueue("user");

        //5、创建producer，写入消息
        for (int i = 0; i < 20; i++) {
            MessageProducer producer = session.createProducer(queue);
            TextMessage message = session.createTextMessage("message" + i);
            /**
             * 消息分组
             */
            message.setLongProperty("week", i % 7);
            producer.send(message);
            //Thread.sleep(1000);
        }

        //6、关闭连接
        connection.close();
    }
}
