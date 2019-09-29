package proofreaders.step8_qs_broadcast_accumulator;

import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;

public interface IBusinessOperator {
    void run();
    DataStream getResultStream();
    BroadcastStream getBroadcastStream();
}
