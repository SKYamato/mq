package co.zs.queue._02acknowledge;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

/**
 * 消息生产者
 * AUTO_ACKNOWLEDGE可能会出现的问题：
 * 1、AUTO_ACKNOWLEDGE造成消息丢失:receive()方法执行时、MessageListener的onmessage()执行时立即确认（即执行消费业务前提交ack）
 * 2、多消费者的ACKNOWLEDGE会造成消息乱序
 *
 * @author shuai
 * @date 2020/03/24 10:50
 */
public class ActiveMQProducerAcknowledge {
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
         * ack:（关闭事务时有效）
         *    AUTO_ACKNOWLEDGE      - 自动发送ack，之后remove消息
         *    CLIENT_ACKNOWLEDGE    - 手动发送ack。未发送ack时，可以重复消息消费
         *    DUPS_OK_ACKNOWLEDGE   - 啊
         *    SESSION_TRANSACTED    - 事务模式确认
         */
        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);

        //4、获取destination，消费者会从这里取消息
        Queue queue = session.createQueue("user");

        //5、producer写入消息
        MessageProducer producer = session.createProducer(queue);
        for (int i = 0; i < 20; i++) {
            TextMessage message = session.createTextMessage("create_message" + i);
            producer.send(message);
            Thread.sleep(1000);
        }

        //6、关闭连接
        connection.close();
    }
}
