package proofreaders.step7_qs_broadcast_failure;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.javalin.http.Context;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.queryablestate.exceptions.UnknownKeyOrNamespaceException;
import org.apache.flink.queryablestate.exceptions.UnknownKvStateIdException;
import org.apache.flink.queryablestate.exceptions.UnknownKvStateKeyGroupLocationException;
import org.apache.flink.queryablestate.exceptions.UnknownLocationException;
import org.apache.flink.queryablestate.network.BadRequestException;
import org.apache.flink.runtime.query.UnknownKvStateLocation;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import proofreaders.common.queue.boundary.Jackson;
import proofreaders.step7_qs_broadcast_failure.query.QueryClientHelper;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

abstract public class AbstractBusinessOperator<KEY, S extends State, D extends StateDescriptor> implements IBusinessOperator, Serializable {

    abstract public void run();
    abstract public DataStream getResultStream();
    abstract public BroadcastStream getBroadcastStream();
    abstract public D getBroadcastStateDescriptor();
    abstract public String getBroadcastStateName();
    abstract public TypeHint<KEY> getStateKeyTypeHint();
    abstract public Class<KEY> getStateKeyClass();

    private static Logger log = LoggerFactory.getLogger(AbstractBusinessOperator.class);

    AbstractBusinessOperator() {
        this.getBroadcastStateDescriptor().setQueryable(this.getBroadcastStateName());
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
                value = ((MapState) state).entries();
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
