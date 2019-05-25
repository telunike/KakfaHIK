package kafka;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

/**
 * kafka生产者测试类
 */
public class KafkaProducerTest {

    private final Producer<String, String> producer;

    public final static String TOPIC = "test";

    private KafkaProducerTest() {
        Properties properties = new Properties();
        //存放kafka ip地址以及端口
        properties.put("metadata.broker.list", "192.168.56.72:9092");
        //配置value序列化列表
        properties.put("serializer.class", "kafka.serializer.StringEncoder");
        //配置key序列化列表
        properties.put("key.serializer.class", "kafka.serializer.StringEncoder");

        /**
         * 0:这意味着生产者producer不等待来自broker同步完成的确认，继续发送下一条消息
         * 1：意味着producer在leader成功收到数据并确认后发送下一条消息
         * -1：意味着producer在follower副本确认到数据收到后才算是一次完成
         */
        properties.put("request.required.acks", "-1");
        producer = new Producer<String, String>(new ProducerConfig(properties));
    }

    void produce() {
        int message = 1;
        final int COUNT = 10;
        while (message < COUNT) {
            String key = String.valueOf(message);
            String data = "hello kafka message" + key;
            producer.send(new KeyedMessage<String, String>(TOPIC, data));
            System.out.println(data);
            message++;
        }
    }

    public static void main(String[] args) {
        new KafkaProducerTest().produce();
    }
}
