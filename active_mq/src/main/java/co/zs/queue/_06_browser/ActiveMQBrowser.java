package co.zs.queue._06_browser;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQQueue;

import javax.jms.*;
import java.util.Enumeration;

/**
 * Browser View
 *
 * @author shuai
 * @date 2020/03/24 11:22
 */
public class ActiveMQBrowser {
    public static void main(String[] args) throws JMSException {
        //1、获取连接工厂
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(
                ActiveMQConnectionFactory.DEFAULT_USER,
                ActiveMQConnectionFactory.DEFAULT_PASSWORD,
                "auto+nio://127.0.0.1:5671");

        //2、获取一个ActiveMQ连接
        Connection connection = connectionFactory.createConnection();
        connection.start();

        //3、获取session
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        //4、创建destination
        Queue queue = new ActiveMQQueue("user");

        /**
         * 创建browser
         */
        QueueBrowser browser = session.createBrowser(queue);

        Enumeration enumeration = browser.getEnumeration();
        /**
         * 一次取出所有未消费的消息
         */
        while (enumeration.hasMoreElements()) {
            TextMessage message = (TextMessage) enumeration.nextElement();
            System.out.println(message);
        }

    }
}
