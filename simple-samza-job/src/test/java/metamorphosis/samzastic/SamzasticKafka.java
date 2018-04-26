package metamorphosis.samzastic;

import static junit.framework.TestCase.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;


public class SamzasticKafka {

    private String broker = "kafka:9092";
    private String topic = "sometopic";
    private int expectedMessages = 999999;
    private long waitMaxMillis = 20000;
    private long start;
    private int receivedMessages = 0;

    public SamzasticKafka(String broker) {
        this.broker = broker;
    }

    public static Producer<String, String> getProducer(String broker) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "words-integration-test-producer-client");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer(props);
    }

    public static Consumer<String, String> getConsumer(String broker) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "words-integration-test-consumer-group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new KafkaConsumer(props);
    }

    public static SamzasticKafka whenBroker(String broker) {
        return new SamzasticKafka(broker);
    }

    public SamzasticKafka topic(String topic) {
        this.topic = topic;
        return this;
    }

    public void containsMessage(String memorablePhrase)
        throws ExecutionException, InterruptedException {
        Producer<String, String> producer = SamzasticKafka.getProducer(broker);

        ProducerRecord record = new ProducerRecord(topic, memorablePhrase, memorablePhrase);

        producer.send(record).get();

        producer.flush();
        producer.close();
    }

    public static SamzasticKafka thenBroker(String broker) {
        return new SamzasticKafka(broker);
    }

    public SamzasticKafka receivesMessages(int expectedMessages) {
        this.expectedMessages = expectedMessages;
        return this;
    }

    public SamzasticKafka inLessThan(Duration duration) {
        this.waitMaxMillis = duration.toMillis();
        return this;
    }

    public void andMessages(
        java.util.function.Consumer<List<ConsumerRecord<String, String>>> messagesValidation
    ) {
        Consumer<String, String> consumer = SamzasticKafka.getConsumer(broker);
        consumer.subscribe(Collections.singleton(topic));

        start = System.currentTimeMillis();
        List<ConsumerRecord<String, String>> allConsumerRecords = new ArrayList<>();

        while (shouldWaitForMessages() && hasUnreceivedMessages()) {

            final ConsumerRecords<String, String> consumerRecords = consumer.poll(100);

            for (ConsumerRecord record : consumerRecords) {
                allConsumerRecords.add(record);
                receivedMessages++;
            }

            consumer.commitAsync();
        }

        consumer.close();
        messagesValidation.accept(allConsumerRecords);
    }

    private boolean hasUnreceivedMessages() {
        return receivedMessages < 12;
    }

    private boolean shouldWaitForMessages() {
        return System.currentTimeMillis() < start + 10000;
    }
}
