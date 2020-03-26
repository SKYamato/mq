package co.zs.service.impl;

import co.zs.service.ProducerService;
import org.apache.activemq.command.ActiveMQQueue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jms.core.JmsMessagingTemplate;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.stereotype.Service;

import javax.jms.*;
import java.util.List;

/**
 * @author shuai
 * @date 2020/03/25 16:50
 */
@Service
public class ProducerServiceImpl implements ProducerService {

    @Autowired
    private JmsMessagingTemplate jmsMessagingTemplate;

    @Autowired
    private JmsTemplate jmsTemplate;

    @Override
    public void sendTopic(String destination, List<String> msgs) {
        jmsMessagingTemplate.convertAndSend(destination, msgs);
    }

    @Override
    public void sendTopicDiyMsg(String destination, String msg) {
        ConnectionFactory connectionFactory = jmsTemplate.getConnectionFactory();

        jmsTemplate.send(destination, (session) -> {
            TextMessage diy_msg = session.createTextMessage(msg);
            return diy_msg;
        });
    }

    @Override
    public void sendQueue(String destination, List<String> msgs) {
        jmsMessagingTemplate.convertAndSend(new ActiveMQQueue(destination), msgs);
    }
}
