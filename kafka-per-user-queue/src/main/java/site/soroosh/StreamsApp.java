package site.soroosh;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;
import site.soroosh.processor.LoanRequestProcessor;
import site.soroosh.processor.LoanResultProcessor;
import site.soroosh.schema.InflightState;
import site.soroosh.schema.LoanRequest;
import site.soroosh.schema.LoanResult;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class StreamsApp {
    private <T extends SpecificRecord> SpecificAvroSerde<T> getValueSerde(Class<T> clazz) {
        SpecificAvroSerde<T> avroValueSerde = new SpecificAvroSerde<>();
        Map<String, String> avroSerdeConfig = Collections.singletonMap("schema.registry.url", AppProps.SCHEMA_REGISTRY_URL);

        avroValueSerde.configure(avroSerdeConfig, false);

        return avroValueSerde;
    }

    private Serde<String> getKeySerde() {
        return Serdes.String();
    }

    private Properties buildKafkaStreamsProps() {
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, AppProps.APPLICATION_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, AppProps.BOOTSTRAP_URL);
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        props.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false);

        return props;
    }

    private Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        builder.addStateStore(
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(AppProps.INFLIGHT_STATE_STORE_NAME),
                        getKeySerde(),
                        getValueSerde(InflightState.class)
                )
        );

        builder.addStateStore(
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(AppProps.PENDING_REQUESTS_STORE_NAME),
                        getKeySerde(),
                        getValueSerde(LoanRequest.class)
                )
        );

        builder
                .stream(AppProps.LOAN_REQUESTS_TOPIC, Consumed.with(
                        getKeySerde(),
                        getValueSerde(LoanRequest.class)
                ))
                .process(
                        LoanRequestProcessor::new,
                        AppProps.INFLIGHT_STATE_STORE_NAME,
                        AppProps.PENDING_REQUESTS_STORE_NAME
                )
                .to(AppProps.SERIALIZED_LOAN_REQUESTS_TOPIC, Produced.with(getKeySerde(), getValueSerde(LoanRequest.class)));

        builder
                .stream(AppProps.LOAN_RESULTS_TOPIC, Consumed.with(getKeySerde(), getValueSerde(LoanResult.class)))
                .process(
                        LoanResultProcessor::new,
                        AppProps.INFLIGHT_STATE_STORE_NAME,
                        AppProps.PENDING_REQUESTS_STORE_NAME
                )
                .to(AppProps.SERIALIZED_LOAN_REQUESTS_TOPIC, Produced.with(getKeySerde(), getValueSerde(LoanRequest.class)));

        return builder.build();
    }

    private KafkaStreams buildKafkaStreams() {
        Topology topology = buildTopology();
        Properties props = buildKafkaStreamsProps();

        return new KafkaStreams(topology, props);
    }

    public void start() {
        final var kafkaStreams = buildKafkaStreams();

        kafkaStreams.start();
    }
}
