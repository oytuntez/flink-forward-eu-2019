package proofreaders.step6_broadcast_tidy;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import proofreaders.common.ClientLanguagePair;
import proofreaders.common.ClientProofreader;
import proofreaders.common.ClientProofreadersList;
import proofreaders.common.EventType;
import proofreaders.common.key.ClientProofreaderLanguagePairKeySelector;
import proofreaders.common.queue.entity.Event;
import proofreaders.common.queue.entity.GenericPayload;

import java.util.Objects;

public class ClientProofreaders {
    //// EVOLUTION = source stream is now received from outside world.
    private DataStream<Event> sourceStream;
    private DataStream<ClientProofreadersList> resultStream;
    private BroadcastStream<ClientProofreadersList> broadcastStream;
    public static MapStateDescriptor<ClientLanguagePair, ClientProofreadersList> clientProofreadersStateDescriptor = new MapStateDescriptor<>(
            "clientProofreaders",
            TypeInformation.of(new TypeHint<ClientLanguagePair>() {}),
            TypeInformation.of(new TypeHint<ClientProofreadersList>() {}));


    public ClientProofreaders(DataStream<Event> sourceStream) {
        this.sourceStream = sourceStream;
    }

    public void run() {
        // Source stream filter
        DataStream<Event> stream = sourceStream
                .filter((FilterFunction<Event>) event -> Objects.equals(EventType.PROOFREADER_TAKEN_PROJECT, event.getEventType()))
                .uid("filter-events-for-client-proofreaders")
                .name("Filter events for ClientProofreaders");

        // Source entity transformation to business entity
        DataStream<ClientProofreader> clientProofreaderStream = stream.process(
                new ProcessFunction<Event, ClientProofreader>() {
                    @Override
                    public void processElement(Event workerEvent, Context context, Collector<ClientProofreader> collector) throws Exception {
                        GenericPayload proofreader = workerEvent.getPayload();
                        Long clientId = proofreader.getClientId();
                        collector.collect(new ClientProofreader(clientId, proofreader.getLanguagePair(), proofreader.getVendorId()));
                    }
                }
        ).uid("client-proofreader-stream").name("Client Proofreader Stream");

        // Core business operation - accumulate client's proofreaders
        DataStream<ClientProofreadersList> proofreaderStream = clientProofreaderStream
                .keyBy(new ClientProofreaderLanguagePairKeySelector())
                .window(EventTimeSessionWindows.withGap(Time.minutes(3)))
                .process(new ClientProofreadersAccumulator())
                .uid("client-proofreaders-accumulator")
                .name("Client Proofreaders Accumulator");

        // Broadcast the result of ClientProofreaders
        BroadcastStream<ClientProofreadersList> clientProofreadersListBroadcastStream = proofreaderStream.broadcast(clientProofreadersStateDescriptor);

        this.resultStream = proofreaderStream;
        this.broadcastStream = clientProofreadersListBroadcastStream;
    }

    public DataStream<ClientProofreadersList> getResultStream() {
        return resultStream;
    }

    public BroadcastStream<ClientProofreadersList> getBroadcastStream() {
        return broadcastStream;
    }

    public static MapStateDescriptor<ClientLanguagePair, ClientProofreadersList> getBroadcastDescriptor() {
        return clientProofreadersStateDescriptor;
    }
}
