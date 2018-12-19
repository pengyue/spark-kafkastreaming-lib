package integration.helper;

import com.godepth.apache.spark.kafkastreaming.spark.SparkStreamConsumer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CollectingStringSparkStreamConsumer implements SparkStreamConsumer<String> {

    public static final Map<String, List<Iterable<String>>> COLLECTED_MESSAGES = Collections.synchronizedMap(new HashMap<>());

    public void consume(
        String topic,
        int partition,
        Iterable<String> messages
    ) {
        COLLECTED_MESSAGES
            .computeIfAbsent(
                topic,
                x -> new ArrayList<>()
            )
            .add(messages);
    }
}
