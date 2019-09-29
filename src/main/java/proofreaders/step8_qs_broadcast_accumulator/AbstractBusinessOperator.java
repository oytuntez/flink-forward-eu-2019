package proofreaders.step8_qs_broadcast_accumulator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.javalin.http.Context;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.queryablestate.exceptions.UnknownKeyOrNamespaceException;
import org.apache.flink.queryablestate.exceptions.UnknownKvStateIdException;
import org.apache.flink.queryablestate.exceptions.UnknownKvStateKeyGroupLocationException;
import org.apache.flink.queryablestate.exceptions.UnknownLocationException;
import org.apache.flink.queryablestate.network.BadRequestException;
import org.apache.flink.runtime.query.UnknownKvStateLocation;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import proofreaders.common.queue.boundary.Jackson;
import proofreaders.step8_qs_broadcast_accumulator.query.QueryClientHelper;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

abstract public class AbstractBusinessOperator<KEY, VALUE, S extends State> implements IBusinessOperator, Serializable {
    abstract public void run();
    abstract public DataStream getResultStream();
    abstract public BroadcastStream getBroadcastStream();
    abstract public MapStateDescriptor<KEY, VALUE> getBroadcastStateDescriptor();
    abstract public String getBroadcastStateName();
    abstract public TypeHint<KEY> getStateKeyTypeHint();
    abstract public Class<KEY> getStateKeyClass();

    private static Logger log = LoggerFactory.getLogger(AbstractBusinessOperator.class);

    AbstractBusinessOperator() {
        this.getBroadcastStateDescriptor().setQueryable(this.getBroadcastStateName());
    }

    // accumulate value, make it queryable and broadcast it
    //public void queryableAsValue(DataStream<VALUE> broadcastStream, KeySelector<VALUE, KEY> keySelector) {
    //    //noinspection unchecked
    //    AccumulateValue<VALUE> acc = new AccumulateValue<>((ValueStateDescriptor<VALUE>) this.getBroadcastStateDescriptor());
    //    keyAccumulateBroadcast(broadcastStream, acc, keySelector);
    //}

    // accumulate value, make it queryable and broadcast it
    public void queryableAsValueMap(DataStream<VALUE> broadcastStream, KeySelector<VALUE, KEY> keySelector) {
        AccumulateMapValue<KEY, VALUE> acc = new AccumulateMapValue<>(this.getBroadcastStateDescriptor(), keySelector);
        keyAccumulateBroadcast(broadcastStream, acc, keySelector);
    }

    // accumulate value, make it queryable and broadcast it
    //public void queryableAsValueList(DataStream<VALUE> broadcastStream, KeySelector<VALUE, KEY> keySelector) {
    //    AccumulateListValue<VALUE> acc = new AccumulateListValue<>((ValueStateDescriptor<ArrayList>) this.getBroadcastStateDescriptor());
    //    keyAccumulateBroadcast(broadcastStream, acc, keySelector);
    //}

    private void keyAccumulateBroadcast(DataStream<VALUE> broadcastStream, RichFlatMapFunction accumulator, KeySelector keySelector) {
        //noinspection unchecked
        broadcastStream
                .keyBy(keySelector)
                .flatMap(accumulator);
    }

    public void query(Context ctx) throws Exception {
        String jobId = ctx.req.getHeader("JOB_ID");
        QueryClientHelper helper = QueryClientHelper.getInstance(jobId);

        KEY key = this.createKeyFromRequest(ctx);
        if (key == null) {
            throw new BadRequestException("", "Key is empty or not compatible");
        }

        StateDescriptor descriptor = this.getBroadcastStateDescriptor();
        String stateName = this.getBroadcastStateName();

        CompletableFuture<S> resultFuture;
        try {
            //noinspection unchecked
            resultFuture = helper.getKvState(
                    stateName,
                    key,
                    this.getStateKeyTypeHint(),
                    descriptor
            );
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

        String response;

        try {
            response = this.convertStateToJSON(resultFuture.join());
        } catch (Exception e) {
            e.printStackTrace();
            Throwable cause = e.getCause();
            response = e.getClass().getName() + ": " + e.getMessage();

            ObjectNode responseJson = JsonNodeFactory.instance.objectNode();
            responseJson.put("details", response);
            responseJson.put("type", cause.getClass().getName());
            responseJson.put("title", e.getClass().getName());

            if (cause instanceof UnknownKvStateLocation || cause instanceof UnknownKeyOrNamespaceException
                    || cause instanceof UnknownKvStateIdException || cause instanceof UnknownKvStateKeyGroupLocationException
                    || cause instanceof UnknownLocationException) {
                responseJson.put("status", 404);
                ctx.status(404);
            } else {
                responseJson.put("status", 500);
                ctx.status(500);
            }

            response = responseJson.toString();
        }

        assert response != null;
        ctx.result(response);
    }

    private KEY createKeyFromRequest(Context ctx) throws BadRequestException {
        if (ctx == null) {
            return null;
        }

        String requestData;

        try {
            requestData = ctx.req.getReader().lines().collect(Collectors.joining());
        } catch (IOException e) {
            e.printStackTrace();
            throw new BadRequestException("", "Key is empty or not compatible");
        }

        if (requestData == null || requestData.length() < 1) {
            throw new BadRequestException("", "Key is empty or not compatible");
        }

        ObjectMapper mapper = new ObjectMapper();
        KEY key;

        try {
            key = mapper.readValue(requestData, this.getStateKeyClass());
        } catch (IOException e) {
            e.printStackTrace();
            throw new BadRequestException("", "Key is empty or not compatible");
        }

        log.debug("This is the key generated from HTTP request body: " + key.getClass() + " /// " + key.toString());

        return key;
    }

    private String convertStateToJSON(S state) {
        Object value;

        try {
            if (state instanceof ValueState) {
                value = ((ValueState) state).value();
            } else if (state instanceof MapState) {
                value = ((MapState) state).values();
            } else if (state instanceof ListState) {
                value = ((ListState) state).get();
            } else {
                value = null;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }

        try {
            return Jackson.getObjectMapper()
                    .enable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                    .writeValueAsString(value);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return Jackson.toString(value);
        }
    }
}


class AccumulateMapValue<KEY, VALUE> extends RichFlatMapFunction<VALUE, VALUE> {
    private static Logger log = LoggerFactory.getLogger(AccumulateMapValue.class);

    private MapStateDescriptor<KEY, VALUE> descriptor;
    private MapState<KEY, VALUE> checkpointedState;
    private KeySelector<VALUE, KEY> keySelector;

    public AccumulateMapValue(MapStateDescriptor<KEY, VALUE> descriptor, KeySelector<VALUE, KEY> keySelector) {
        this.descriptor = descriptor;
        this.keySelector = keySelector;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        checkpointedState = getRuntimeContext().getMapState(this.descriptor);
    }

    @Override
    public void flatMap(VALUE object, Collector<VALUE> collector) throws Exception {
        KEY key = this.keySelector.getKey(object);
        log.info("map >>>");
        log.info(key.toString());
        log.info(object.toString());
        log.info("<<< map");
        checkpointedState.put(key, object);
    }
}

class AccumulateListValue<VALUE> extends RichFlatMapFunction<VALUE, VALUE> {
    private ValueStateDescriptor<ArrayList> descriptor;
    private ValueState<ArrayList> checkpointedState;

    public AccumulateListValue(ValueStateDescriptor<ArrayList> descriptor) {
        this.descriptor = descriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        checkpointedState = getRuntimeContext().getState(this.descriptor);
        super.open(parameters);
    }


    @Override
    public void flatMap(VALUE object, Collector<VALUE> collector) throws IOException {
        ArrayList<VALUE> currentListValue;
        try {
            currentListValue = checkpointedState.value();
        } catch (IOException e) {
            currentListValue = new ArrayList<>();
        }

        if (currentListValue == null) {
            currentListValue = new ArrayList<>();
        }

        currentListValue.add(object);
        checkpointedState.update(currentListValue);
    }
}

class AccumulateValue<VALUE> extends RichFlatMapFunction<VALUE, VALUE> {
    private ValueStateDescriptor<VALUE> descriptor;
    private ValueState<VALUE> checkpointedState;

    public AccumulateValue(ValueStateDescriptor<VALUE> descriptor) {
        this.descriptor = descriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        checkpointedState = getRuntimeContext().getState(this.descriptor);
        super.open(parameters);
    }


    @Override
    public void flatMap(VALUE object, Collector<VALUE> collector) {
        try {
            checkpointedState.update(object);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}