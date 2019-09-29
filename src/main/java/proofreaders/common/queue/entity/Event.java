package proofreaders.common.queue.entity;


import proofreaders.common.EventType;

import java.io.Serializable;
import java.util.Date;
import java.util.Objects;

public class Event implements Serializable {

    private String event;
    // This is typically the RabbitMQ topic
    private String topic = "events";
    private GenericPayload payload;

    private Date _queuedAt = new Date();

    public Event() {
    }

    public Event(String event, GenericPayload payload, Date _queuedAt) {
        this.event = event;
        this.payload = payload;
        this._queuedAt = _queuedAt;
    }

    public Event(EventType event, GenericPayload payload) {
        this(event, payload, new Date());
    }

    public Event(EventType event, GenericPayload payload, Date _queuedAt) {
        this(event.getName(), payload, _queuedAt);
    }

    public Event(String event, String topic, GenericPayload payload, Date _queuedAt) {
        this.event = event;
        this.topic = topic;
        this.payload = payload;
        this._queuedAt = _queuedAt;
    }

    public Event(EventType event, String topic, GenericPayload payload, Date _queuedAt) {
        this.event = event.getName();
        this.topic = topic;
        this.payload = payload;
        this._queuedAt = _queuedAt;
    }

    public EventType getEventType() {
        return EventType.get(event);
    }

    public Date getQueuedAt() {
        return _queuedAt;
    }

    public String getEvent() {
        return event;
    }

    public void setEvent(String event) {
        this.event = event;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public GenericPayload getPayload() {
        return payload;
    }

    public void setPayload(GenericPayload payload) {
        this.payload = payload;
    }

    public Date get_queuedAt() {
        return _queuedAt;
    }

    public void set_queuedAt(Date _queuedAt) {
        this._queuedAt = _queuedAt;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Event that = (Event) o;
        return Objects.equals(event, that.event) &&
                Objects.equals(topic, that.topic) &&
                Objects.equals(payload, that.payload);
    }

    @Override
    public int hashCode() {
        return Objects.hash(event, topic, payload);
    }

    @Override
    public String toString() {
        return "Event{" +
                "event='" + event + '\'' +
                ", topic='" + topic + '\'' +
                ", payload=" + payload +
                ", _queuedAt=" + _queuedAt +
                '}';
    }

}
