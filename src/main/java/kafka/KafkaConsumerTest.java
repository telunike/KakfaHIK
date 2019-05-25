package kafka;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * kafka消费者测试类
 */
public class KafkaConsumerTest {

    private final ConsumerConnector consumer;

    private KafkaConsumerTest() {
        Properties properties = new Properties();

        properties.put("zookeeper.connect", "192.168.56.74:2181");

        properties.put("group.id", "testgroup");

        properties.put("serializer,class", "kafka.serializer.StringEncoder");

        ConsumerConfig config = new ConsumerConfig(properties);

        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(config);
    }

    void consume() {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(KafkaProducerTest.TOPIC, new Integer(1));

        StringDecoder keyDecoder = new StringDecoder(new VerifiableProperties());
        StringDecoder valueDecoder = new StringDecoder(new VerifiableProperties());
        Map<String, List<KafkaStream<String, String>>> consumerMap =
                consumer.createMessageStreams(topicCountMap, keyDecoder, valueDecoder);

        KafkaStream<String, String> stream = consumerMap.get(KafkaProducerTest.TOPIC).get(0);

        ConsumerIterator<String, String> iterator = stream.iterator();
        while (iterator.hasNext()) {
            System.out.println("接收到的数据为：" + iterator.next().message());
        }
    }

    public static void main(String[] args) {
        new KafkaConsumerTest().consume();
    }

}
