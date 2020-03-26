package co.zs.service;

/**
 * @author shuai
 * @date 2020/03/20 14:49
 */
public interface IMessageSender {

    /**
     * 发送消息
     *
     * @param topic
     * @param key
     * @param value
     */
    void sendMessage(String topic, String key, String value);
}
