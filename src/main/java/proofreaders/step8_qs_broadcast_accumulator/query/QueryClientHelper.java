package proofreaders.step8_qs_broadcast_accumulator.query;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.queryablestate.client.QueryableStateClient;
import proofreaders.common.queue.boundary.Config;

import java.util.concurrent.CompletableFuture;

public class QueryClientHelper extends QueryableStateClient implements AutoCloseable {
    private static QueryClientHelper currentInstance;
    private static String previousJobId;
    public static final String QUERYABLE_SERVER_HOST = "QUERYABLE_SERVER_HOST";

    /**
     * ID of the job to query.
     */
    private final JobID jobId;

    /**
     * Creates the queryable state client wrapper.
     *
     * @param jobManagerHost Host for JobManager communication
     * @param jobManagerPort Port for JobManager communication.
     * @param jobId ID of the job to query.
     * @throws Exception Thrown if creating the {@link QueryableStateClient} fails.
     */
    QueryClientHelper(
            String jobManagerHost,
            int jobManagerPort,
            JobID jobId) throws Exception {
        super(jobManagerHost, jobManagerPort);
        this.jobId = jobId;

        ExecutionConfig exeConfig = new ExecutionConfig();
        exeConfig.enableSysoutLogging();
        this.setExecutionConfig(exeConfig);
    }

    public void close() {
        this.shutdownAndWait();
    }

    public static QueryClientHelper getInstance(String jobIdName) throws Exception {
        if (currentInstance == null || (previousJobId != null && !previousJobId.equals(jobIdName))) {
            ParameterTool config = new Config().getConfiguration();
            if (jobIdName == null) {
                jobIdName = "00000000000000000000000000000000";
            }
            previousJobId = jobIdName;
            JobID jobId = JobID.fromHexString(jobIdName);
            currentInstance = new QueryClientHelper(config.get(QUERYABLE_SERVER_HOST), 9069, jobId);
        }

        return currentInstance;
    }

    public <K, S extends State, V> CompletableFuture<S> getKvState(String queryableStateName, K key, TypeHint<K> keyTypeHint, StateDescriptor<S, V> stateDescriptor) {
        return super.getKvState(jobId, queryableStateName, key, keyTypeHint, stateDescriptor);
    }
}
