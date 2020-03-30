package co.zs.queue._09_async_callback;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQMessageProducer;
import org.apache.activemq.AsyncCallback;

import javax.jms.*;
import java.util.concurrent.CountDownLatch;

/**
 * @author shuai
 * @date 2020/03/27 11:28
 */
public class ProducerAsyncCallBack {
    public static void main(String[] args) throws Exception {

        CountDownLatch countDownLatch = new CountDownLatch(1);

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

        //5、创建producer，
        ActiveMQMessageProducer producer = (ActiveMQMessageProducer) session.createProducer(queue);
        TextMessage message = session.createTextMessage("message");
        //发送消息
        producer.send(message, new AsyncCallback() {
            /**
             * MQ成功接收消息
             */
            @Override
            public void onSuccess() {
                countDownLatch.countDown();
                //后续业务代码
            }

            /**
             * 遇到异常
             * @param exception
             */
            @Override
            public void onException(JMSException exception) {
                System.out.println(exception.getStackTrace());
            }
        });

        countDownLatch.await();

        //6、关闭连接
        connection.close();
    }
}
