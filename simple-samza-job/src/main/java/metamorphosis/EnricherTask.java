package metamorphosis;

import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EnricherTask implements StreamTask {

    private static Logger log = LoggerFactory.getLogger(EnricherTask.class);

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
            log.info(">>>>>>> MSG={}", incomingMessageEnvelope.getMessage().toString());
        }

        log.info(">>>>>>> OFFSET={}", incomingMessageEnvelope.getOffset());

        log.info("<<<<<<<<<==== handled message.");

    }
}
