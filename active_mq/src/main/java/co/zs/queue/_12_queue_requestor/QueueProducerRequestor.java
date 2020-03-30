package co.zs.queue._12_queue_requestor;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

/**
 * 消息生产者 发送同步消息
 *
 * @author shuai
 * @date 2020/03/24 10:50
 */
public class QueueProducerRequestor {
    public static void main(String[] args) throws Exception {
        //1、获取连接工厂
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(
                ActiveMQConnectionFactory.DEFAULT_USER,
                ActiveMQConnectionFactory.DEFAULT_PASSWORD,
                "tcp://122.51.90.185:61616");

        //2、获取一个ActiveMQ连接
        ActiveMQConnection connection = (ActiveMQConnection) connectionFactory.createConnection();
        connection.start();

        QueueSession session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);

        //4、获取destination，消费者会从这里取消息
        Queue queue = session.createQueue("user");

        /**
         * 创建QueueRequestor对象
         */
        QueueRequestor queueRequestor = new QueueRequestor(session, queue);

        System.out.println("producer初始化完成");

        for (int i = 0; i < 2; i++) {

            TextMessage message = session.createTextMessage("message");

            System.out.println("发送请求,等待消息确认");
            /**
             * 向broker发送一个请求，等待响应
             */
            Message responseMsg = queueRequestor.request(message);

            System.out.println(responseMsg);

            System.out.println("---请求发送完成---");

        }

        //6、关闭连接
        connection.close();
    }
}
