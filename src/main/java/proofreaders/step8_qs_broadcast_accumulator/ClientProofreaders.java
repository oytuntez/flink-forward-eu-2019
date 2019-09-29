package proofreaders.step8_qs_broadcast_accumulator;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import proofreaders.common.ClientLanguagePair;
import proofreaders.common.ClientProofreader;
import proofreaders.common.ClientProofreadersList;
import proofreaders.common.EventType;
import proofreaders.common.key.ClientProofreaderLanguagePairKeySelector;
import proofreaders.common.key.ClientProofreaderListLanguagePairKeySelector;
import proofreaders.common.queue.entity.Event;
import proofreaders.common.queue.entity.GenericPayload;
import proofreaders.step8_qs_broadcast_accumulator.annotations.QueryableStateEndpoint;

import java.util.Objects;

@QueryableStateEndpoint(endpoint = "client-proofreaders")
public class ClientProofreaders extends AbstractBusinessOperator<ClientLanguagePair, ClientProofreadersList, MapState> {
    private static String broadcastStateName = "clientProofreaders";
    private static MapStateDescriptor<ClientLanguagePair, ClientProofreadersList> broadcastStateDescriptor = new MapStateDescriptor<>(
            broadcastStateName,
            TypeInformation.of(new TypeHint<ClientLanguagePair>() {}),
            TypeInformation.of(new TypeHint<ClientProofreadersList>() {}));

    private DataStream<Event> sourceStream;
    private DataStream<ClientProofreadersList> resultStream;
    private BroadcastStream<ClientProofreadersList> broadcastStream;

    public ClientProofreaders() {
    }

    public ClientProofreaders(DataStream<Event> sourceStream) {
        super();

        this.sourceStream = sourceStream;
    }

    @Override
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
                    public void processElement(Event workerEvent, Context context, Collector<ClientProofreader> collector) {
                        GenericPayload proofreader = workerEvent.getPayload();
                        Long clientId = proofreader.getClientId();
                        collector.collect(new ClientProofreader(clientId, proofreader.getLanguagePair(), proofreader.getVendorId()));
                    }
                }
        ).uid("client-proofreader-stream").name("Client Proofreader Stream");

        clientProofreaderStream.print("clientProofreaderStream");

        // Core business operation - accumulate client's proofreaders
        DataStream<ClientProofreadersList> proofreaderStream = clientProofreaderStream
                .keyBy(new ClientProofreaderLanguagePairKeySelector())
                //.window(EventTimeSessionWindows.withGap(Time.seconds(10)))
                .process(new ClientProofreadersAccumulator())
                .uid("client-proofreaders-accumulator")
                .name("Client Proofreaders Accumulator");

        proofreaderStream.print("proofreaderStream");

        // Broadcast the result of ClientProofreaders
        BroadcastStream<ClientProofreadersList> clientProofreadersListBroadcastStream = proofreaderStream.broadcast(broadcastStateDescriptor);

        this.queryableAsValueMap(proofreaderStream, new ClientProofreaderListLanguagePairKeySelector());

        this.resultStream = proofreaderStream;
        this.broadcastStream = clientProofreadersListBroadcastStream;
    }

    @Override
    public DataStream<ClientProofreadersList> getResultStream() {
        return resultStream;
    }

    @Override
    public BroadcastStream<ClientProofreadersList> getBroadcastStream() {
        return broadcastStream;
    }

    @Override
    public MapStateDescriptor<ClientLanguagePair, ClientProofreadersList> getBroadcastStateDescriptor() {
        return broadcastStateDescriptor;
    }

    @Override
    public String getBroadcastStateName() {
        return broadcastStateName;
    }

    @Override
    public TypeHint<ClientLanguagePair> getStateKeyTypeHint() {
        return new TypeHint<ClientLanguagePair>() {};
    }

    @Override
    public Class<ClientLanguagePair> getStateKeyClass() {
        return ClientLanguagePair.class;
    }
}
