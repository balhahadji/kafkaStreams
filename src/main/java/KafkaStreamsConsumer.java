import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class KafkaStreamsConsumer {
    public static void main(String[] args) {
        new KafkaStreamsConsumer().start();
    }

    private void start() {
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "k-stream-app");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, String> kStream = streamsBuilder.stream("testTopic1", Consumed.with(Serdes.String(), Serdes.String()));
        KTable<Windowed<String>, Long> resultStream =
                kStream.flatMapValues(textLine -> Arrays.asList(textLine.split("\\W+")))
                        .map((k, v) -> new KeyValue<>(k, v.toLowerCase()))
                        .groupBy((k, v) -> v)
                        .windowedBy(TimeWindows.of(Duration.ofSeconds(5)))
                        .count(Materialized.as("count-analytics"));
        resultStream.toStream().map((k, v) -> new KeyValue<>(k.window().startTime() + "__" + k.window().endTime() + "___" + k.key(), v))
                .to("resultTopic", (Produced<String, Long>) Produced.with(Serdes.String(), Serdes.Long()));

        Topology topology = streamsBuilder.build();
        KafkaStreams kafkaStreams = new KafkaStreams(topology, properties);
        kafkaStreams.start();
    }
}
