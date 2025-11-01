package site.soroosh;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.specific.SpecificRecord;
import org.apache.hc.client5.http.utils.Hex;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import site.soroosh.schema.LoanRequest;
import site.soroosh.schema.LoanResult;

import java.util.Properties;
import java.util.Random;

public class MessageSimulator {
    private final Random random = new Random();
    private final String randomUserSuffix = getRandomUserSuffix(random);
    private final Producer<String, SpecificRecord> producer;

    MessageSimulator() {
        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppProps.BOOTSTRAP_URL);
        props.put(ProducerConfig.LINGER_MS_CONFIG, "1000");
        props.put(ProducerConfig.ACKS_CONFIG, "0");
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "335544320000");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "999940");

        props.put("schema.registry.url", AppProps.SCHEMA_REGISTRY_URL);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);

        producer = new KafkaProducer<>(props);
    }

    void sendRequests(int userCount, int requestPerUser) {
        for (int i = 0; i < userCount; i++) {
            var userId = "user-" + randomUserSuffix + "-" + i;

            for (int j = 0; j < requestPerUser; j++) {
                var requestId = "request-" + j;
                var randomAmount = random.nextLong(1, 100_000_000);
                var producerRecord = new ProducerRecord<String, SpecificRecord>(
                        AppProps.LOAN_REQUESTS_TOPIC,
                        userId,
                        new LoanRequest(
                                requestId,
                                randomAmount
                        ));

                producer.send(producerRecord);
            }
        }

        producer.flush();
    }

    void sendResults(int userCount) {
        for (int i = 0; i < userCount; i++) {
            var userId = "user-" + randomUserSuffix + "-" + i;
            var requestId = "request-" + 0;
            var result = "success";
            var producerRecord = new ProducerRecord<String, SpecificRecord>(
                    AppProps.LOAN_RESULTS_TOPIC,
                    userId,
                    new LoanResult(
                            requestId,
                            result
                    ));

            producer.send(producerRecord);

        }

        producer.flush();
    }

    private String getRandomUserSuffix(Random random) {
        var userRandomSuffixBytes = new byte[5];

        random.nextBytes(userRandomSuffixBytes);

        return Hex.encodeHexString(userRandomSuffixBytes);
    }
}
