import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class StreamFilterTweets {

    public static String server = "127.0.0.1:9092";
    public static String appId = "kafka-streams";
    public static String topic = "twitter_tweets";
    public static String newTopic = "important_tweets";


    private static JsonParser jsonParser;

    public static void main(String[] args) {

        // create properties
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, server);
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        // create a topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, String> inputTopic = streamsBuilder.stream(topic);
        KStream<String, String> filteredStream = inputTopic.filter(
                // filter tweets which has a user of over 10000 followers
                (k, tweet) -> extractUserFollowersFromTweet(tweet) > 10000
        );
        filteredStream.to(newTopic);

        // build the topology
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);

        // start stream application
        kafkaStreams.start();

    }

    private static Integer extractUserFollowersFromTweet(String json) {

        try {
            return jsonParser
                    .parse(json)
                    .getAsJsonObject()
                    .get("user")
                    .getAsJsonObject()
                    .get("followers_count")
                    .getAsInt();
        } catch (NullPointerException e) {
            return 0;
        }

    }
}
