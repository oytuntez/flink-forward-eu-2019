package proofreaders.step7_qs_broadcast_failure;

import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;

public interface IBusinessOperator {
    void run();
    DataStream getResultStream();
    BroadcastStream getBroadcastStream();
}
