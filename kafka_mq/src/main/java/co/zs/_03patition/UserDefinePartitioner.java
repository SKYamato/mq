package co.zs._03patition;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 自定义Partitioner
 * 默认类DefaultPartitioner
 *
 * @author shuai
 * @date 2020/03/19 11:18
 */
public class UserDefinePartitioner implements Partitioner {
    /**
     * 记录轮询次数
     */
    private AtomicInteger counter = new AtomicInteger(0);

    /**
     * 返回分区号
     * 不同版本该方法调用会有不同实现
     *
     * @param topic
     * @param key
     * @param keyBytes
     * @param value
     * @param valueBytes
     * @param cluster
     * @return
     */
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        //获取所有可以的分区数
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();
        if (keyBytes == null) {
            //存在key
            int andIncrement = counter.getAndIncrement();
            return (andIncrement & Integer.MAX_VALUE) % numPartitions;
        } else {
            //存在key，hash值转换成正数
            return Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions;
        }
    }

    /**
     * 关闭后的回调函数
     */
    @Override
    public void close() {
        System.out.println("close");
    }

    /**
     * 生命周期方法
     *
     * @param configs
     */
    @Override
    public void configure(Map<String, ?> configs) {
        System.out.println("config");
    }
}
