package co.zs.queue._02acknowledge;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

/**
 * 消息消费者
 * AUTO_ACKNOWLEDGE可能会出现的问题：
 * 1、AUTO_ACKNOWLEDGE造成消息丢失:receive()方法执行时、MessageListener的onmessage()执行时立即确认（即执行消费业务前提交ack）
 * 2、AUTO_ACKNOWLEDGE造成消息乱序:
 *
 * @author shuai
 * @date 2020/03/24 11:22
 */
public class ActiveMQConsumerAcknowledge {
    public static void main(String[] args) throws Exception {
        //1、获取连接工厂
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(
                ActiveMQConnectionFactory.DEFAULT_USER,
                ActiveMQConnectionFactory.DEFAULT_PASSWORD,
                "tcp://localhost:61616");

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
        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);

        //4、获取destination，消费者会从这里取消息
        Destination queue = session.createQueue("user");

        //5、创建consumer，获取消息
        MessageConsumer consumer = session.createConsumer(queue);

        while (true) {
            TextMessage receive = (TextMessage) consumer.receive();
            /**
             * 手动提交
             */
            receive.acknowledge();
            //{commandId = 5, responseRequired = true, messageId = ID:DESKTOP-6N8733Q-54985-1585039599713-1:1:1:1:1, originalDestination = null, originalTransactionId = null, producerId = ID:DESKTOP-6N8733Q-54985-1585039599713-1:1:1:1, destination = queue://user, transactionId = null, expiration = 0, timestamp = 1585039599865, arrival = 0, brokerInTime = 1585039599866, brokerOutTime = 1585039599867, correlationId = null, replyTo = null, persistent = true, type = null, priority = 4, groupID = null, groupSequence = 0, targetConsumerId = null, compressed = false, userID = null, content = null, marshalledProperties = null, dataStructure = null, redeliveryCounter = 0, size = 0, properties = null, readOnlyProperties = true, readOnlyBody = true, droppable = false, text = create_message0}
            System.out.println(receive.getText());
        }
    }
}
