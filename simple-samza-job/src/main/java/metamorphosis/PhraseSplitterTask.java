package metamorphosis;

import java.util.Arrays;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PhraseSplitterTask implements StreamTask {

    private static Logger log = LoggerFactory.getLogger(PhraseSplitterTask.class);
    private static final SystemStream wordsOutputStream = new SystemStream("kafka-system", "words-topic");

    @Override
    public void process(
        IncomingMessageEnvelope incomingMessageEnvelope,
        MessageCollector messageCollector,
        TaskCoordinator taskCoordinator
    ) throws Exception {

        log.info("################################################################");

        if (incomingMessageEnvelope == null) {
            log.warn("!!!!!!!!!!!!!!!!!!!!!!!!!!!!! incomingMessageEnvelope == null");
            return;
        }

        if (incomingMessageEnvelope.getKey() == null) {
            log.warn("!!!!!!!!!!!!!!!!!!!!!!!!!!!!! incomingMessageEnvelope.getKey() == null");
        } else {
            log.info(">>>>>>> KEY={}", incomingMessageEnvelope.getKey().toString());
        }

        if (incomingMessageEnvelope.getMessage() == null) {
            log.warn("!!!!!!!!!!!!!!!!!!!!!!!!!!!!! incomingMessageEnvelope.getMessage() == null");
        } else {
            String phrase = incomingMessageEnvelope.getMessage().toString();
            log.info(">>>>>>> MSG={}", phrase);

            Arrays.stream(phrase.split("\\s+"))
                .map(word -> new OutgoingMessageEnvelope(wordsOutputStream, word, word))
                .forEach(msg -> messageCollector.send(msg));

        }

        log.info(">>>>>>> OFFSET={}", incomingMessageEnvelope.getOffset());

        log.info("<<<<<<<<<==== handled message.");

    }
}
