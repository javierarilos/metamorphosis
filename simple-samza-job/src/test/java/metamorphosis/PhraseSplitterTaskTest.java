package metamorphosis;

import static junit.framework.TestCase.assertTrue;
import static metamorphosis.samzastic.SamzasticKafka.thenBroker;
import static metamorphosis.samzastic.SamzasticKafka.whenBroker;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Test;

public class PhraseSplitterTaskTest {

    private Consumer<List<ConsumerRecord<String, String>>> containForgiveness = records -> {
        Set<String> allValues = records.stream()
            .filter(Objects::nonNull)
            .map(ConsumerRecord::value)
            .filter(Objects::nonNull)
            .collect(Collectors.toSet());

        assertTrue(
            allValues.contains("forgiveness")
        );
    };

    @Test
    public void givenAllIsRunning_whenGraceHopperPhraseInPhraseTopic_thenForgivenessInWordsTopic()
        throws ExecutionException, InterruptedException {
        // given everything is running
        // TODO: AUTOMATE START OF KAFKA AND SAMZA, CLEANOUT OF TOPICS

        // when phrase in phrases-topic
        String graceHopperPhrase = "It is easier to ask forgiveness than it is to get permission";

        whenBroker("kafka:9092")
            .topic("phrases-topic")
            .containsMessage(graceHopperPhrase);

        // then
        thenBroker("kafka:9092")
            .topic("words-topic")
            .receivesMessages(12)
            .inLessThan(Duration.ofSeconds(10))
            .andMessages(containForgiveness);
    }

    @Test
    public void givenEverythingIsRunning_whenPhraseInTopic_thenPermissionIsThere()
        throws ExecutionException, InterruptedException {
        // given

        // when phrase in phrases-topic

        String graceHopperPhrase = "It is easier to ask forgiveness than it is to get permission";
        String anonymousPhrase = "Arguing with an engineer is like fighting a pig in mud after the first few hours you realise they enjoy it";
        whenBroker("kafka:9092")
            .topic("phrases-topic")
            .containsMessage(graceHopperPhrase);

        // then

        thenBroker("kafka:9092")
            .topic("words-topic")
            .receivesMessages(12)
            .inLessThan(Duration.ofSeconds(10))
            .andMessages(allConsumerRecords -> {
                Set<String> allValues = allConsumerRecords.stream()
                    .filter(Objects::nonNull)
                    .map(ConsumerRecord::value)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toSet());

                assertTrue(
                    allValues.contains("permission")
                );
            });
    }


    @Test
    public void givenEverythingIsRunning_whenPhraseInTopic_thenEasierIsThere()
        throws ExecutionException, InterruptedException {
        // given

        // when phrase in phrases-topic

        String graceHopperPhrase = "It is easier to ask forgiveness than it is to get permission";
        String anonymousPhrase = "Arguing with an engineer is like fighting a pig in mud after the first few hours you realise they enjoy it";
        whenBroker("kafka:9092")
            .topic("phrases-topic")
            .containsMessage(graceHopperPhrase);

        // then
        thenBroker("kafka:9092")
            .topic("words-topic")
            .receivesMessages(12)
            .inLessThan(Duration.ofSeconds(10))
            .andMessages(allConsumerRecords -> {
                Set<String> allValues = allConsumerRecords.stream()
                    .filter(Objects::nonNull)
                    .map(ConsumerRecord::value)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toSet());

                assertTrue(
                    allValues.contains("easier")
                );
            });
    }


}
