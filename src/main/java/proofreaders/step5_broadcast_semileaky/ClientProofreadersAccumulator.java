package proofreaders.step5_broadcast_semileaky;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import proofreaders.common.ClientLanguagePair;
import proofreaders.common.ClientProofreader;
import proofreaders.common.ClientProofreadersList;

import java.util.List;

public class ClientProofreadersAccumulator extends ProcessWindowFunction<ClientProofreader, ClientProofreadersList, ClientLanguagePair, TimeWindow> {
    private ValueState<ClientProofreadersList> state;

    @Override
    public void open(Configuration params) {
        ValueStateDescriptor<ClientProofreadersList> descriptor = new ValueStateDescriptor<>(
                "clientProofreaders",
                TypeInformation.of(ClientProofreadersList.class));

        state = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void process(ClientLanguagePair clientLanguagePair, Context context, Iterable<ClientProofreader> iterable, Collector<ClientProofreadersList> collector) throws Exception {
        List<ClientProofreader> prfList = Lists.newArrayList(iterable);
        ClientProofreadersList list = state.value();
        if (list == null) {
            list = new ClientProofreadersList();
        }

        if (prfList.size() > 0) {
            list.addAll(prfList);
        }

        state.update(list);
        collector.collect(list);
    }
}
