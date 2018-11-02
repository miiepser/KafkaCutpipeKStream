package de.samples.baist.kafka;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;


@Component
public class PipeKStream implements CommandLineRunner {

    private static final Logger LOG = LoggerFactory.getLogger(PipeKStream.class);

    public static String FROM_TOPIC = "test-producer";
    public static String TO_TOPIC = "test-consumer";
//    private final KafkaStreams streams;
//    private final CountDownLatch latch;

    private static final Properties props = new Properties();

    {
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pipe-mod");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092, localhost:9093");

        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    }


    private static void parseProperties(String[] optionalProps) {
        for (int i = 0; i < CollectionUtils.size(optionalProps); ++i) {
            final String currentArg = optionalProps[i];
            switch (currentArg) {
                case "bootstrap.servers":
                    props.put(currentArg, optionalProps[++i]);
                    break;
                case "send.topic":
                    TO_TOPIC = optionalProps[++i];
                    break;
                case "listen.topic":
                    FROM_TOPIC = optionalProps[++i];
                    break;
                default:
                    break;
            }
        }
        addPropertyIfSet("bootstrap.servers");
        TO_TOPIC=  StringUtils.defaultIfEmpty(System.getenv("send.topic"), TO_TOPIC);
        FROM_TOPIC = StringUtils.defaultIfEmpty(System.getenv("listen.topic"), FROM_TOPIC);

    }


    private static void addPropertyIfSet(final String propertyName) {
        final String propertyValue = System.getenv(propertyName);
        if(null != propertyValue){
            props.put(propertyName, propertyValue);
        }
    }

    @Override
    public void run(String... args) throws Exception {

        LOG.info("Ready to start, parsing the given configuration...");
        parseProperties(args);
        LOG.info("Listening to Topic {}, sending to Topic {} and received configured broker server {} ", FROM_TOPIC, TO_TOPIC, props.getProperty("bootstrap.servers"));

        final StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> b = builder.stream(FROM_TOPIC);
        //b.flatMapValues(s-> Arrays.asList(cutPrefix(s)));
        b.flatMapValues(PipeKStream::cutPrefix).to(TO_TOPIC);

        //b.to(TO_TOPIC);
        LOG.info("Create topology");
        final Topology topology = builder.build();
        LOG.info("Create topology");

        final KafkaStreams streams = new KafkaStreams(topology, props);
        LOG.info("Create topology");

        final CountDownLatch latch = new CountDownLatch(1);
        LOG.info("Create topology");
        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });
        streams.start();

    }

    private static Collection<String> cutPrefix(String message) {
        return Arrays.asList(message.substring(message.indexOf(": ") + 2));
    }
}
