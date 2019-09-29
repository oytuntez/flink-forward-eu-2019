package proofreaders.step2_split;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import proofreaders.common.ClientProofreadersList;

public class DoSomething extends ProcessFunction<ClientProofreadersList, String> {
    @Override
    public void processElement(ClientProofreadersList o, Context context, Collector<String> collector) throws Exception {
        collector.collect(o.toString());
    }
}
