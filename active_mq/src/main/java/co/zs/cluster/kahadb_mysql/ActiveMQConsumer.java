package co.zs.cluster.kahadb_mysql;

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
                "failover(nio://localhost:5671,nio://localhost:5672)?Randomize=false");

        //2、获取一个ActiveMQ连接
        Connection connection = connectionFactory.createConnection();
        connection.start();

        //3、获取session
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        //4、获取destination，消费者会从这里取消息
        Destination queue = session.createQueue("user");

        //5、创建consumer，获取消息
        MessageConsumer consumer = session.createConsumer(queue);

        while (true) {
            TextMessage receive = (TextMessage) consumer.receive();
            System.out.println(receive.getText());
        }
    }
}
