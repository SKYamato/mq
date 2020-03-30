package co.zs.queue._05_reply_to;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

/**
 * 使用reply to实现生产者消费者同步
 *
 * @author shuai
 * @date 2020/03/24 10:50
 */
public class ActiveMQProducerReplyTo {

    public static void main(String[] args) throws Exception {
        //1、获取连接工厂
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(
                ActiveMQConnectionFactory.DEFAULT_USER,
                ActiveMQConnectionFactory.DEFAULT_PASSWORD,
                "tcp://127.0.0.1:5671");

        //2、获取一个ActiveMQ连接
        Connection connection = connectionFactory.createConnection();
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        //4、获取destination，消费者会从这里取消息
        Queue queue = session.createQueue("user");

        //5、创建producer，写入消息
        MessageProducer producer = session.createProducer(queue);
        TextMessage message = session.createTextMessage("message");
        /**
         * 指定reply to的destination地址
         */
        TemporaryQueue temporaryQueue = session.createTemporaryQueue();
        message.setJMSReplyTo(temporaryQueue);
        producer.send(message);
        System.out.println("消息发送完毕");

        /**
         * 监听临时destination
         */
        MessageConsumer consumer = session.createConsumer(temporaryQueue);
        consumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                System.out.println("收到消息确认");
            }
        });

        //6、关闭连接
        //connection.close();
    }
}
