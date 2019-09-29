package proofreaders.common.queue.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;
import org.apache.flink.shaded.guava18.com.google.common.collect.Maps;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

@Getter
@Setter
public class QueueJob implements Serializable {
    @JsonProperty("class")
    private String className;
    private String method;
    private Map<String, Object> payload = Maps.newHashMap();

    public QueueJob() {
    }

    public QueueJob(String className, String method, Map<String, Object> payload) {
        this.className = className;
        this.method = method;
        this.payload = payload;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QueueJob queueJob = (QueueJob) o;
        return Objects.equals(className, queueJob.className) &&
                Objects.equals(method, queueJob.method) &&
                Objects.equals(payload, queueJob.payload);
    }

    @Override
    public int hashCode() {
        return Objects.hash(className, method, payload);
    }
}
