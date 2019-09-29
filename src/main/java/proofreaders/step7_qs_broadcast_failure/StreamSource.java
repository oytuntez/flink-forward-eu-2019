package proofreaders.step7_qs_broadcast_failure;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import proofreaders.common.queue.entity.Event;

public class StreamSource extends RichSourceFunction<Event> {
    private volatile boolean isRunning = true;
    private volatile boolean collectedInitialEvents = false;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void cancel() {
        try {
            close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        isRunning = false;
    }

    private boolean isRunning() {
        return isRunning;
    }

    @Override
    public void run(SourceContext<Event> sourceContext) {
        while (isRunning()) {
            if (!collectedInitialEvents) {
                synchronized (sourceContext.getCheckpointLock()) {
                    Event[] events = ElementSource.getElements();
                    for (Event event : events) {
                        sourceContext.collect(event);
                    }

                    collectedInitialEvents = true;
                }

                Thread.yield();
            }
        }
    }
}

