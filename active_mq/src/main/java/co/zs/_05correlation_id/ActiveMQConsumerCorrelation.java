package co.zs._05correlation_id;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

/**
 * 消息消费者，使用correlationId相互通信
 *
 * @author shuai
 * @date 2020/03/24 11:22
 */
public class ActiveMQConsumerCorrelation {
    public static void main(String[] args) throws Exception {
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
        Destination queue = session.createQueue("user");

        //5、创建consumer，获取消息
        MessageConsumer consumer = session.createConsumer(queue, "type='C'");

        consumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                try {
                    System.out.println("接收到消息『" + ((TextMessage) message).getText() + "』");
                    /**
                     * 指定给消息生产者发送反馈信息
                     */
                    MessageProducer producer = session.createProducer(queue);
                    TextMessage textMessage = session.createTextMessage("收到。。。");
                    textMessage.setStringProperty("type", "P");
                    textMessage.setJMSCorrelationID(message.getJMSCorrelationID());
                    System.out.println("发送消息反馈");
                    producer.send(textMessage);

                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        });
    }
}
