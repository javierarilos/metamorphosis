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
    public void process(IncomingMessageEnvelope incomingMessageEnvelope,
        MessageCollector messageCollector, TaskCoordinator taskCoordinator) throws Exception {

        String key = incomingMessageEnvelope.getKey().toString();
        String msg = incomingMessageEnvelope.getMessage().toString();
        String offset = incomingMessageEnvelope.getOffset();

        log.info("====>>>>>>>>> received a message. key={} msg={}", key, msg);

        log.info("<<<<<<<<<==== handled message.");

    }
}
