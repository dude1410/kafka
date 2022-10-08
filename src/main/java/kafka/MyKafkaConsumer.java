package kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class MyKafkaConsumer {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(MyKafkaConsumer.class);

        String bootStrapServer = "192.168.3.32:9092";
        String topic = "first_topic";
        String groupId = "my-fourth-app";

        // create Consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // or "latest"

        // create Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // subscribe consumer to our topic(s)
        consumer.subscribe(Collections.singleton(topic)); // or Arrays.asList("first_topic", "second_topic")

        // poll for new data
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1)); // once a sec

            for (ConsumerRecord<String, String> record : records) {
                logger.info(parseRecord(record));
            }
        }
    }

    private static String parseRecord(ConsumerRecord<String, String> record) {
        StringBuilder builder = new StringBuilder();
        builder.append("The message was received successfully\n")
                .append("from topic ")
                .append(record.topic())
                .append(" from partition ")
                .append(record.partition())
                .append(" with offset ")
                .append(record.offset())
                .append(" at timestamp ")
                .append(record.timestamp())
        .append(" >>> key = ")
        .append(record.key())
        .append( " >>> value = ")
        .append(record.value());

        return builder.toString();
    }
}
