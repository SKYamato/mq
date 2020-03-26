package co.zs.service;

/**
 * @author shuai
 * @date 2020/03/25 16:56
 */
public interface ConsumerService {
    /**
     * 接收消息
     *
     * @param msg
     */
    void receive(String msg);
}
