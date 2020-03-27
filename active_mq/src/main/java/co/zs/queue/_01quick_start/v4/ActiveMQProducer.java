package co.zs.queue._01quick_start.v4;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ScheduledMessage;

import javax.jms.*;

/**
 * 消息生产者 设置同步、异步；ScheduledMessage支持
 *
 * @author shuai
 * @date 2020/03/24 10:50
 */
public class ActiveMQProducer {
    public static void main(String[] args) throws Exception {
        //1、获取连接工厂
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(
                ActiveMQConnectionFactory.DEFAULT_USER,
                ActiveMQConnectionFactory.DEFAULT_PASSWORD,
                "tcp://localhost:61616");

        //2、获取一个ActiveMQ连接
//        /**
//         * 2.1、设置异步发送消息
//         */
//        connectionFactory.setSendAcksAsync(true);
        ActiveMQConnection connection = (ActiveMQConnection) connectionFactory.createConnection();
        connection.start();

        /**
         * 2.1、设置异步发送消息
         */
        connection.setAlwaysSyncSend(true);

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        //4、获取destination，消费者会从这里取消息
        Destination queue = session.createQueue("user");

        //5、创建producer，写入消息
        MessageProducer producer = session.createProducer(queue);
        TextMessage message = session.createTextMessage("message1");
        /**
         * schedulerSupport
         * config文件配置<broker schedulerSupport="true"></broker>
         */
        long delay = 5 * 1000;
        /**
         * repeat是int类型
         */
        int repeat = 3;
        long period = 2 * 1000;
        /**
         * 延迟时间：ms
         */
        message.setLongProperty(ScheduledMessage.AMQ_SCHEDULED_DELAY, delay);
        /**
         * 额外重复发送次数
         */
        message.setIntProperty(ScheduledMessage.AMQ_SCHEDULED_REPEAT, repeat);
        /**
         * 重复发送间隔时间：ms
         */
        message.setLongProperty(ScheduledMessage.AMQ_SCHEDULED_PERIOD, period);
        /**
         * 自定义消息头信息用于消费者过滤消息
         */
        message.setIntProperty("numberOfMsg", 1);

        producer.send(message);

        /**
         * 发送第二条消息
         */
        TextMessage message2 = session.createTextMessage("message2");
        /**
         * 自定义消息头信息用于消费者过滤消息
         */
        message2.setIntProperty("numberOfMsg", 2);
        producer.send(message2);

        /**
         * 自定义消息头信息用于消费者过滤消息
         */
        TextMessage message3 = session.createTextMessage("message3");
        message3.setIntProperty("numberOfMsg", 3);
        producer.send(message3);

        //6、关闭连接
        connection.close();
    }
}
