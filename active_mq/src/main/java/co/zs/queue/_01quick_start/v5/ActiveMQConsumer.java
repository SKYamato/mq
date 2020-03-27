package co.zs.queue._01quick_start.v5;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

/**
 * 消息消费者
 *
 * @author shuai
 * @date 2020/03/24 11:22
 */
public class ActiveMQConsumer {
    public static void main(String[] args) throws Exception {
        //1、获取连接工厂
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(
                ActiveMQConnectionFactory.DEFAULT_USER,
                ActiveMQConnectionFactory.DEFAULT_PASSWORD,
                "auto+nio://127.0.0.1:5671");

        //2、获取一个ActiveMQ连接
        Connection connection = connectionFactory.createConnection();
        connection.start();

        //3、获取session
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        //4、获取destination，消费者会从这里取消息
        Destination queue = session.createQueue("user");

        //5、创建consumer，获取消息
        MessageConsumer consumer = session.createConsumer(queue);

        consumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                try {
                    System.out.println("接收一条消息");
                    System.out.println(message);
                    //发送确认消息
                    Destination jmsReplyTo = message.getJMSReplyTo();
                    System.out.println(jmsReplyTo);
                    System.out.println("发送确认消息");
                    MessageProducer producer = session.createProducer(jmsReplyTo);
                    producer.send(session.createTextMessage());
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        });
    }
}
