package co.zs.queue._10_time;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQTextMessage;

import javax.jms.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

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
                "tcp://localhost:61616");

        //2、获取一个ActiveMQ连接
        Connection connection = connectionFactory.createConnection();
        connection.start();

        //3、获取session
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        //4、获取destination，消费者会从这里取消息
        Destination queue = session.createQueue("user");

        //5、创建consumer，获取消息
        MessageConsumer consumer = session.createConsumer(queue);

        consumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                try {
                    DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
                    System.out.println("消息生成时间：" + format.format(new Date(message.getJMSTimestamp())));
                    System.out.println("进入Broker时间：" + format.format(new Date(((ActiveMQTextMessage) message).getBrokerInTime())));
                    System.out.println("出来Broker时间：" + format.format(new Date(((ActiveMQTextMessage) message).getBrokerOutTime())));

                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        });
    }
}
