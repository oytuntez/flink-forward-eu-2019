package proofreaders.step3_consume_stream;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import proofreaders.common.ClientLanguagePair;
import proofreaders.common.ClientProofreader;
import proofreaders.common.ClientProofreadersList;
import proofreaders.common.queue.entity.Event;
import proofreaders.common.queue.entity.GenericPayload;

import java.util.Map;

public class InviteLastProofreader extends CoProcessFunction<Event, ClientProofreadersList, ClientProofreader> {
    private transient ValueState<Event> currentProjectState;
    private transient MapState<ClientLanguagePair, ClientProofreadersList> clientProofreadersState;

    // And here is yet another leak from ClientProofreaders.
    private MapStateDescriptor<ClientLanguagePair, ClientProofreadersList> clientProofreadersStateDescriptor = new MapStateDescriptor<>(
            "clientProofreaders",
            TypeInformation.of(new TypeHint<ClientLanguagePair>() {}),
            TypeInformation.of(new TypeHint<ClientProofreadersList>() {}));

    private ValueStateDescriptor<Event> sDesc = new ValueStateDescriptor<>("invite-last-proofreader-current-project", TypeInformation.of(Event.class));


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        currentProjectState = getRuntimeContext().getState(sDesc);
        clientProofreadersState = getRuntimeContext().getMapState(clientProofreadersStateDescriptor);
    }

    @Override
    public void processElement1(Event o, Context context, Collector<ClientProofreader> collector) throws Exception {
        currentProjectState.update(o);

        Iterable<Map.Entry<ClientLanguagePair, ClientProofreadersList>> entries = clientProofreadersState.entries();
        ClientProofreader proofreader = this.pickLastProofreaders(o, entries, collector);

        if (proofreader != null) {
            // This is my resulting proofreader. Let this stream down, in case someone wants to know
            // which proofreader we picked for which project.
            collector.collect(proofreader);

            // I am going to send a message to the proofreader in another process, using the side output.
            String msg = "Hello proofreader #" + proofreader.vendorId + "!";
            context.output(new OutputTag<String>("send-message"){}, msg);
        }
    }

    @Override
    public void processElement2(ClientProofreadersList clientProofreaders, Context context, Collector<ClientProofreader> collector) throws Exception {
        ClientProofreader first = clientProofreaders.get(0);
        if (first != null) {
            clientProofreadersState.put(new ClientLanguagePair(first.clientId, first.languagePair), clientProofreaders);
        }
    }

    private ClientProofreader pickLastProofreaders(Event o, Iterable<Map.Entry<ClientLanguagePair, ClientProofreadersList>> entries, Collector<ClientProofreader> collector) throws Exception {
        GenericPayload payload = o.getPayload();
        ClientLanguagePair clientLanguagePair = new ClientLanguagePair(payload.getClientId(), payload.getLanguagePair());

        for (Map.Entry<ClientLanguagePair, ClientProofreadersList> entry : entries) {
            if (clientLanguagePair.equals(entry.getKey())) {
                ClientProofreadersList list = entry.getValue();

                if (list != null && list.size() > 0) {
                    // Found the last proofreader who worked for this client in this language pair
                    return list.get(list.size() - 1);
                }
            }
        }

        return null;
    }
}
