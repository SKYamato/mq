package co.zs._02quick_start.comsumer;

import co.zs.util.KafkaUtil;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/**
 * kafka消费者
 *
 * @author shuai
 * @date 2020/03/19 9:48
 */
public class KafkaConsumerQuickStart {

    public static void main(String[] args) {
        //组管理自动订阅：实现消费伸缩性、提高消费者容错力
        subscription();
        //手动指定：消费者相互独立
        //specify();
    }

    /**
     * 订阅消息
     */
    private static void subscription() {
        //创建订阅形式的KafkaConsumer
        KafkaConsumer<String, String> consumer = KafkaUtil.createKafkaConsumerWithGroup("c2");
        //订阅相关的topics
        consumer.subscribe(Pattern.compile("^topic.*"));
        //处理消息信息
        KafkaUtil.getConsumerResult(consumer);
    }

    /**
     * 手动指定消费分区
     */
    private static void specify() {
        //手动指定消费分区
        KafkaConsumer<String, String> consumer = KafkaUtil.createKafkaConsumerWithOutGroup();
        TopicPartition partition = new TopicPartition("topic01", 0);
        List<TopicPartition> partitions = Arrays.asList(partition);
        consumer.assign(partitions);
        //指定消费分区的位置
        //consumer.seekToBeginning(partitions);
        consumer.seek(partition, 1);
        //遍历结果
        KafkaUtil.getConsumerResult(consumer);
    }
}
