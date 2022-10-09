package kafkaBsasics;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class MyKafkaProducerWithCallback {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(MyKafkaProducerWithCallback.class);

        String bootStrapServer = "192.168.3.29:9092";
        String topic = "first_topic";

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);


        for (int i = 0; i < 20; i++) {

            String messageText = "message with callback with number " + i;

            // create producer record
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, messageText);

            // send data - asynchronous
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // executes every time a record is sent successfully or an exception is thrown
                    if (e == null) {
                        // the record was sent successfully
                        logger.info(parseMetadata(recordMetadata));
                    } else {
                        logger.error("Error when sending message: " + e);
                    }
                }
            });
        }
        producer.flush();
        producer.close();
    }

    private static String parseMetadata(RecordMetadata recordMetadata) {
        StringBuilder builder = new StringBuilder();
        builder.append("The message was sent successfully\n")
                .append("to topic ")
                .append(recordMetadata.topic())
                .append(" to partition ")
                .append(recordMetadata.partition())
                .append(" with offset ")
                .append(recordMetadata.offset())
                .append(" at timestamp ")
                .append(recordMetadata.timestamp());

        return builder.toString();
    }
}
