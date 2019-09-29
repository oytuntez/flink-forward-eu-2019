package proofreaders.step6_broadcast_tidy;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import proofreaders.common.EventType;
import proofreaders.common.LanguagePair;
import proofreaders.common.queue.entity.Event;
import proofreaders.common.queue.entity.GenericPayload;

import java.util.Date;

public class ElementSource {
    public static DataStream<Event> buildSource(StreamExecutionEnvironment env) {
        Date now1 = new Date(2019, 10, 1, 1, 1, 1);
        GenericPayload pl1 = new GenericPayload();
        pl1.setVendorId(1L);
        pl1.setClientId(2L);
        pl1.setLanguagePair(new LanguagePair("en-US", "de"));
        Event event1 = new Event(EventType.PROOFREADER_TAKEN_PROJECT, pl1, now1);

        Date now2 = new Date(2019, 10, 1, 2, 1, 1);
        GenericPayload pl2 = new GenericPayload();
        pl2.setVendorId(2L);
        pl2.setClientId(2L);
        pl2.setLanguagePair(new LanguagePair("en-US", "fr"));
        Event event2 = new Event(EventType.PROOFREADER_TAKEN_PROJECT, pl2, now2);

        Date now3 = new Date(2019, 10, 1, 3, 1, 1);
        GenericPayload pl3 = new GenericPayload();
        pl3.setVendorId(1L);
        pl3.setClientId(2L);
        pl3.setLanguagePair(new LanguagePair("en-US", "fr"));
        Event event3 = new Event(EventType.PROOFREADER_TAKEN_PROJECT, pl3, now3);

        Date now4 = new Date(2019, 10, 2, 3, 1, 1);
        GenericPayload pl4 = new GenericPayload();
        pl4.setProjectId(1L);
        pl4.setClientId(2L);
        pl4.setLanguagePair(new LanguagePair("en-US", "fr"));
        Event event4 = new Event(EventType.PROJECT_CREATED, pl4, now4);

        return env.fromElements(event1, event2, event3, event4);
    }
}
