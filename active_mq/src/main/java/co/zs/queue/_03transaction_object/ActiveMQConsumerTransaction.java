package co.zs.queue._03transaction_object;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import java.util.Arrays;

/**
 * 消息消费者事务消费
 *
 * @author shuai
 * @date 2020/03/24 11:22
 */
public class ActiveMQConsumerTransaction {

    public static void main(String[] args) throws Exception {
        //1、获取连接工厂
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(
                ActiveMQConnectionFactory.DEFAULT_USER,
                ActiveMQConnectionFactory.DEFAULT_PASSWORD,
                "tcp://localhost:61616");

        /**
         * 添加信任的序列化类
         */
        connectionFactory.setTrustedPackages(Arrays.asList(User.class.getPackage().getName()));

        //2、获取一个ActiveMQ连接
        Connection connection = connectionFactory.createConnection();
        connection.start();

        /**
         * 3、获取session
         * 开启事务:
         *    true/false;
         * ack:（关闭事务时有效）
         *    AUTO_ACKNOWLEDGE      - 自动发送ack，之后remove消息
         *    CLIENT_ACKNOWLEDGE    - 手动发送ack。未发送ack时，可以重复消息消费
         *    DUPS_OK_ACKNOWLEDGE   - 不需要发送ack确认
         *    SESSION_TRANSACTED    - 事务模式确认
         */
        Session session = connection.createSession(true, Session.CLIENT_ACKNOWLEDGE);

        //4、获取destination，消费者会从这里取消息
        Destination queue = session.createQueue("user_object");

        //5、创建consumer
        MessageConsumer consumer = session.createConsumer(queue);

        //获取消息
        for (int i = 1; ; i++) {
            Message message = consumer.receive();

            if (message instanceof TextMessage) {
                System.out.println(((TextMessage) message).getText());
            } else if (message instanceof ObjectMessage) {
                System.out.println(((ObjectMessage) message).getObject().toString());
            } else if (message instanceof BytesMessage) {
                System.out.println(((BytesMessage) message).readUTF());
            } else if (message instanceof MapMessage) {
                System.out.println(message);
                System.out.println(((MapMessage)message).getString("name"));
                System.out.println(((MapMessage)message).getInt("age"));
                System.out.println(((MapMessage)message).getDouble("price"));
            }

            /**
             * 手动提交
             */
            session.commit();
        }
    }
}
