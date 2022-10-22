import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ElasticSearchConsumerMedium {

    private static String hostname;
    private static String username;
    private static String password;

    private static String bootStrapServer = "192.168.3.37:9092";
    private static String topic = "twitter_tweets";
    private static String groupId = "my-elastic-app";

    private static JsonParser jsonParser;

    public static void main(String[] args) throws IOException {

        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class);

        initialize();
        RestHighLevelClient client = createClient();

        KafkaConsumer<String, String> consumer = createConsumer();

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMinutes(1)); // once a min

            logger.info("Recived " + records.count() + " records");

            for (ConsumerRecord<String, String> record : records) {
                logger.info(parseRecord(record));

                /** the right way
                 * the real tweet is a json file itself */
//                    String json = record.value();

                /** 1st strategy for idempotent */
                String messageId = record.topic() + "_" + record.partition() + "_" + record.offset();

                /** 2nd strategy for idempotent
                 * get if from twitter tweet */
//                String messageId = extractIdFromTweet(record.value());

                String json = "{\"message\": \"" + record.value() + "\"}";

                IndexRequest indexRequest = new IndexRequest("twitter", "tweets", messageId)
                        .source(json, XContentType.JSON);

                IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);

                String id = indexResponse.getId();

                logger.info("message sending success -> Id is " + id);
            }
            logger.info("Committing the offset");
            consumer.commitSync();
            logger.info("Offsets have been committed");
        }

//        client.close();
    }

    private static void initialize() {
        try (InputStream input = ElasticSearchConsumer.class.getClassLoader().getResourceAsStream("config.properties")) {

            Properties prop = new Properties();
            if (input == null) {
                System.out.println("Sorry, unable to find config.properties");
                return;
            }
            //load a properties file from class path, inside static method
            prop.load(input);
            //get the property values
            hostname = prop.getProperty("elastic.hostname");
            username = prop.getProperty("elastic.username");
            password = prop.getProperty("elastic.password");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static RestHighLevelClient createClient() {

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

        RestClientBuilder builder = RestClient.builder(
                new HttpHost(hostname, 443, "https")).setHttpClientConfigCallback(
                new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                        return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });

        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
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
                .append(" >>> value = ")
                .append(record.value());

        return builder.toString();
    }

    private static KafkaConsumer<String, String> createConsumer() {

        // create Consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // or "latest"
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // disable auto commit of offsets
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");

        // create Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // subscribe consumer to ou r topic(s)
        consumer.subscribe(Collections.singleton(topic)); // or Arrays.asList("first_topic", "second_topic")

        return consumer;
    }

    private static String extractIdFromTweet(String json) {
        return jsonParser
                .parse(json)
                .getAsJsonObject()
                .get("id_str")
                .getAsString();
    }
}
