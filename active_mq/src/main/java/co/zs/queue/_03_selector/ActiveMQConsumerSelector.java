package co.zs.queue._03_selector;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

/**
 * 消息消费者分组，流量定向分发负载均衡
 *  exclusive 可能会造成消息堆积
 *
 * @author shuai
 * @date 2020/03/24 11:22
 */
public class ActiveMQConsumerSelector {
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
        /**
         * 设置消息分组选择器，指定复合条件的消息消费
         */
        MessageConsumer consumer = session.createConsumer(queue, "week=1");

        while (true) {
            TextMessage receive = (TextMessage) consumer.receive();
            System.out.println(receive.getText());
        }
    }
}
