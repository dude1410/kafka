package kafkaBsasics;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class MyKafkaConsumerAssignSeek {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(MyKafkaConsumerAssignSeek.class);

        String bootStrapServer = "192.168.3.32:9092";
        String topic = "first_topic";

        // create Consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // or "latest"

        // create Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // assign an seek are mostly used to replay data or fetch a specific message
        TopicPartition partitionToReadFrom = new TopicPartition(topic, 0);
        long offsetToReadFrom = 15L;
        consumer.assign(Arrays.asList(partitionToReadFrom));

        // seek
        consumer.seek(partitionToReadFrom, offsetToReadFrom);

        int numberOfMessagesToRead = 5;
        boolean keepOnReading = true;
        int numberOfMessagesReadSoFar = 0;

        // poll for new data
        while (keepOnReading) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1)); // once a sec

            for (ConsumerRecord<String, String> record : records) {
                numberOfMessagesReadSoFar += 1;
                logger.info(parseRecord(record));
                if (numberOfMessagesReadSoFar >= numberOfMessagesToRead) {
                    keepOnReading = false;
                    break;
                }
            }
        }
        logger.info("Exiting the application");
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
