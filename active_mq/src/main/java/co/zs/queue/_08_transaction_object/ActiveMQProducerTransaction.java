package co.zs.queue._08_transaction_object;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

/**
 * 消息生产者事务提交、消费优先级设置
 *
 * @author shuai
 * @date 2020/03/24 10:50
 */
public class ActiveMQProducerTransaction {
    public static void main(String[] args) throws Exception {
        //1、获取连接工厂
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(
                ActiveMQConnectionFactory.DEFAULT_USER,
                ActiveMQConnectionFactory.DEFAULT_PASSWORD,
                "tcp://localhost:61616"
        );
        //2、获取一个ActiveMQ连接
        Connection connection = connectionFactory.createConnection();
        connection.start();

        /**
         * 3、获取session
         * 开启事务:
         *    true/false;
         * ack:
         *    AUTO_ACKNOWLEDGE      - 自动发送ack，之后remove消息
         *    CLIENT_ACKNOWLEDGE    - 手动发送ack。未发送ack时，可以重复消息消费
         *    DUPS_OK_ACKNOWLEDGE   - 不需要发送ack确认
         *    SESSION_TRANSACTED    - 事务模式确认
         */
        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);

        //4、获取destination，消费者会从这里取消息
        Queue queue = session.createQueue("user_object");

        //5、获取producer
        MessageProducer producer = session.createProducer(queue);
        //写入消息
        //sendObj(session, producer);
        //sendBytes(session, producer);
        sendMap(session, producer);
        /**
         * 提交事务
         */
        session.commit();

        //6、关闭连接
        connection.close();
    }

    /**
     * 发送序列化对象
     *
     * @param session
     * @param producer
     * @throws JMSException
     */
    private static void sendObj(Session session, MessageProducer producer) throws JMSException {
        for (int i = 0; i < 30; i++) {
            User user = new User("zhang:" + i, i, i);
            ObjectMessage message = session.createObjectMessage(user);

            /**
             * message
             *    设置消息持久化策略，优先级：1 ~ 9， 超时时间
             */
            producer.send(message, DeliveryMode.PERSISTENT, i % 8 + 1, 6000);

//            /**
//             * producer设置持久化策略
//             */
//            producer.setDeliveryMode(DeliveryMode.PERSISTENT);
//            /**
//             * producer整体设置消息优先级：1 ~ 9
//             * （需要修改配置文件<policyEntry queue="queueName" prioritizedMessages="true"/>）
//             */
//            if (i % 3 == 0) {
//                producer.setPriority(9);
//            } else {
//                producer.setPriority(1);
//            }
//            producer.send(message);

            //Thread.sleep(1000);
        }
    }

    /**
     * 发送字节数组
     *
     * @param session
     * @param producer
     * @throws JMSException
     */
    private static void sendBytes(Session session, MessageProducer producer) throws JMSException {
        BytesMessage bytesMessage = session.createBytesMessage();
        bytesMessage.writeUTF("你好");
        producer.send(bytesMessage);
    }

    /**
     * 发送key-value类型消息
     *
     * @param session
     * @param producer
     * @throws JMSException
     */
    private static void sendMap(Session session, MessageProducer producer) throws JMSException {
        MapMessage mapMessage = session.createMapMessage();
        mapMessage.setString("name", "zhang");
        mapMessage.setInt("age", 18);
        mapMessage.setDouble("price", 2233.23);
        producer.send(mapMessage);
    }
}
