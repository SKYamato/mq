package co.zs.queue._12_queue_requestor;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

/**
 * 消息消费者
 *
 * @author shuai
 * @date 2020/03/24 11:22
 */
public class QueueConsumer {
    public static void main(String[] args) throws Exception {
        //1、获取连接工厂
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(
                ActiveMQConnectionFactory.DEFAULT_USER,
                ActiveMQConnectionFactory.DEFAULT_PASSWORD,
                "tcp://122.51.90.185:61616");

        //2、获取一个ActiveMQ连接
        Connection connection = connectionFactory.createConnection();
        connection.start();

        //3、获取session
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        //4、获取destination，消费者会从这里取消息
        Destination queue = session.createQueue("user");

        //5、创建consumer，获取消息
        MessageConsumer consumer = session.createConsumer(queue);

        System.out.println("consumer初始化完成");

        consumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                Destination jmsReplyTo = null;
                try {
                    /**
                     * 向临时destination发送消息确认
                     */
                    System.out.print("----发送消息确认");
                    jmsReplyTo = message.getJMSReplyTo();
                    MessageProducer producer = session.createProducer(jmsReplyTo);
                    producer.send(session.createMessage());
                    System.out.println("----接收到消息");
                    System.out.println(((TextMessage)message).getText());
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        });
    }
}
