package co.zs.controller;

import co.zs.service.ProducerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Arrays;

/**
 * @author shuai
 * @date 2020/03/25 16:47
 */
@RestController
public class MainController {

    @Autowired
    private ProducerService producerService;

    /**
     * 发送消息
     *
     * @return
     */
    @RequestMapping("/send")
    public String send() {
        //producerService.sendTopic("springboot", Arrays.asList("msg_from_springboot"));
        producerService.sendTopicDiyMsg("springboot", "diy_msg");
        return "OK";
    }
}
