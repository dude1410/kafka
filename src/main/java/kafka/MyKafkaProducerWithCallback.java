package kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class MyKafkaProducerWithCallback {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(MyKafkaProducerWithCallback.class);

        String bootStrapServer = "192.168.3.29:9092";

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);


        for (int i = 0; i < 20; i++) {
            // create producer record
            ProducerRecord<String, String> record = new ProducerRecord<>("first_topic",
                    "message with callback with number " + i);

            // send data - asynchronous
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // executes every time a record is sent successfully or an exception is thrown
                    if (e == null) {
                        // the record was sent successfully
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
                        logger.info(builder.toString());
                    } else {
                        logger.error("Error when sending message: " + e);
                    }
                }
            });
        }
        producer.flush();
        producer.close();
    }
}
