package co.zs.topic._02retroactive;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

/**
 * 订阅消息消费者消息回溯
 * http://activemq.apache.org/retroactive-consumer.html
 *
 * @author shuai
 * @date 2020/03/24 11:22
 */
public class ActiveMQConsumerTopic {
    public static void main(String[] args) throws Exception {
        //1、获取连接工厂
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(
                ActiveMQConnectionFactory.DEFAULT_USER,
                ActiveMQConnectionFactory.DEFAULT_PASSWORD,
                "tcp://localhost:5671");

        //2、获取一个ActiveMQ连接
        Connection connection = connectionFactory.createConnection();
        connection.start();

        //3、获取session
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        /**
         * 4、获取topic的destination，消费者会从这里取消息
         */
        Destination topic = session.createTopic("user_topic?consumer.retroactive=true");

        //5、创建consumer
        MessageConsumer consumer = session.createConsumer(topic);

        /**
         * 6、使用监听器获取方法
         */
        consumer.setMessageListener(new MessageListener() {
            public void onMessage(Message message) {
                try {
                    System.out.println(((TextMessage) message).getText());
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        });
    }
}
