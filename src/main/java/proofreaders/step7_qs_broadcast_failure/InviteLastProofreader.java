package proofreaders.step7_qs_broadcast_failure;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import proofreaders.common.ClientLanguagePair;
import proofreaders.common.ClientProofreader;
import proofreaders.common.ClientProofreadersList;
import proofreaders.common.LanguagePair;
import proofreaders.common.queue.entity.Event;
import proofreaders.common.queue.entity.GenericPayload;

import java.util.Map;

public class InviteLastProofreader extends KeyedBroadcastProcessFunction<Long, Event, ClientProofreadersList, ClientProofreader> {
    // And here is yet another leak from ClientProofreaders.
    private MapStateDescriptor<ClientLanguagePair, ClientProofreadersList> clientProofreadersBroadcastedStateDescriptor = new MapStateDescriptor<>(
            "clientProofreaders",
            TypeInformation.of(new TypeHint<ClientLanguagePair>() {}),
            TypeInformation.of(new TypeHint<ClientProofreadersList>() {}));

    // this is because broadcast state arrives earlier to this operator as we are using a Collection source.
    // in proper environment, this would not be needed.
    private static ClientProofreader defaultProofreader = new ClientProofreader(2L, new LanguagePair("en-US", "tr"), 1L);

    @Override
    public void processElement(Event o, ReadOnlyContext readOnlyContext, Collector<ClientProofreader> collector) throws Exception {
        Iterable<Map.Entry<ClientLanguagePair, ClientProofreadersList>> entries = readOnlyContext.getBroadcastState(clientProofreadersBroadcastedStateDescriptor).immutableEntries();
        ClientProofreader proofreader = this.pickLastProofreaders(o, entries);

        if (proofreader == null) {
            proofreader = defaultProofreader;
        }

        // This is my resulting proofreader. Let this stream down, in case someone wants to know
        // which proofreader we picked for which project.
        collector.collect(proofreader);

        // I am going to send a message to the proofreader in another process, using the side output.
        String msg = "Hello proofreader #" + proofreader.vendorId + "!";
        readOnlyContext.output(new OutputTag<String>("send-message"){}, msg);
    }

    @Override
    public void processBroadcastElement(ClientProofreadersList clientProofreaders, Context context, Collector<ClientProofreader> collector) throws Exception {
        ClientProofreader first = clientProofreaders.get(0);
        if (first != null) {
            context.getBroadcastState(clientProofreadersBroadcastedStateDescriptor).put(new ClientLanguagePair(first.clientId, first.languagePair), clientProofreaders);
        }
    }

    private ClientProofreader pickLastProofreaders(Event o, Iterable<Map.Entry<ClientLanguagePair, ClientProofreadersList>> entries) {
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
