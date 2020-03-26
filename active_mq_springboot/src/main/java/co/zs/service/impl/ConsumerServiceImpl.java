package co.zs.service.impl;

import co.zs.service.ConsumerService;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.stereotype.Service;

/**
 * @author shuai
 * @date 2020/03/25 16:57
 */
@Service
public class ConsumerServiceImpl implements ConsumerService {

    @Override
    @JmsListener(destination = "springboot", containerFactory = "jmsListenerContainerTopic")
    public void receive(String msg) {
        System.out.println(msg);
    }
}
