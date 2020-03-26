package co.zs.service;

import java.util.List;

/**
 * @author shuai
 * @date 2020/03/25 16:48
 */
public interface ProducerService {

    /**
     * 发送消息
     *
     * @param destination
     * @param msgs
     */
    void sendTopic(String destination, List<String> msgs);

    /**
     * 发送消息
     *
     * @param destination
     * @param msg
     */
    void sendTopicDiyMsg(String destination, String msg);

    /**
     * 发送消息
     *
     * @param destination
     * @param msgs
     */
    void sendQueue(String destination, List<String> msgs);
}
