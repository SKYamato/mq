package co.zs.queue._11_correlation_id;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import java.util.UUID;

/**
 * 消息生产者，使用correlationId相互通信
 *
 * @author shuai
 * @date 2020/03/24 10:50
 */
public class ActiveMQProducerCorrelation {
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

        //4、获取destination，消费者会从这里取消息
        Queue queue = session.createQueue("user");

        //5、创建producer，写入消息
        MessageProducer producer = session.createProducer(queue);

        System.out.println("producer初始化成功。。。");

        TextMessage message = session.createTextMessage("message");
        /**
         * 创建JMSCorrelationID
         */
        String cid = UUID.randomUUID().toString();
        message.setJMSCorrelationID(cid);
        message.setStringProperty("type", "C");

        producer.send(message);
        System.out.println(message.getText() + " ：消息发送完成");

        MessageConsumer consumer = session.createConsumer(queue, "JMSCorrelationID='" + cid + "' AND type='P'");
        consumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                try {
                    System.out.println("收到消息反馈『" + ((TextMessage) message).getText() + "』");
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        });
        //6、关闭连接
        //connection.close();
    }
}
