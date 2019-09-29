package proofreaders.step8_qs_broadcast_accumulator;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.OutputTag;
import proofreaders.common.ClientProofreader;
import proofreaders.common.ClientProofreadersList;
import proofreaders.common.EventType;
import proofreaders.common.queue.entity.Event;

import java.util.Objects;

public class Invitation {
    //// EVOLUTION = source stream is now received from outside world.
    private DataStream<Event> sourceStream;
    private DataStream<ClientProofreader> resultStream;

    Invitation(DataStream<Event> sourceStream) {
        this.sourceStream = sourceStream;
    }

    public void run(BroadcastStream<ClientProofreadersList> clientProofreadersListBroadcastStream) {
        // A separate business operation - we will wait for new projects and send message to the last proofreader
        // who worked for the client of the new project.

        // First filter the events for my second business operation.
        DataStream<Event> newProjectStream = sourceStream
                .filter((FilterFunction<Event>) event -> Objects.equals(EventType.PROJECT_CREATED, event.getEventType()))
                .uid("filter-events-for-new-projects")
                .name("Filter events for new projects");

        newProjectStream.print("newProjectStream");

        // Now get the new projects, connect the client's proofreader list, and select the last proofreader who
        // worked for this same client in this language pair.
        SingleOutputStreamOperator<ClientProofreader> proofreadersPickedForProject = newProjectStream.keyBy("payload.projectId")
                .connect(clientProofreadersListBroadcastStream)
                .process(new InviteLastProofreader());

        // Get proofreaders selected for this project, as well as the messages to be sent out.
        proofreadersPickedForProject.print();
        DataStream<String> messagesToSend = proofreadersPickedForProject.getSideOutput(new OutputTag<String>("send-message") {});
        messagesToSend.print("messagesToSend");

        this.resultStream = proofreadersPickedForProject;
    }

    public DataStream<ClientProofreader> getResultStream() {
        return resultStream;
    }
}
