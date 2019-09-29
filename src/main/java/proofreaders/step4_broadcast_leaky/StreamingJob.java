/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package proofreaders.step4_broadcast_leaky;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;
import proofreaders.common.ClientLanguagePair;
import proofreaders.common.ClientProofreader;
import proofreaders.common.ClientProofreadersList;
import proofreaders.common.EventType;
import proofreaders.common.queue.entity.Event;

import java.util.Objects;

public class StreamingJob {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// Source
		DataStream<Event> defaultSourceStream = ElementSource.buildSource(env);

		// Our capsule for client's proofreaders information
		ClientProofreaders cp = new ClientProofreaders(defaultSourceStream);
		cp.run();

		// I don't want to use the resulting stream directly, but I want to broadcast it.
		// Now here I am leaking a good amount of information from ClientProofreaders...
		MapStateDescriptor<ClientLanguagePair, ClientProofreadersList> clientProofreadersStateDescriptor = new MapStateDescriptor<>(
				"clientProofreaders",
				TypeInformation.of(new TypeHint<ClientLanguagePair>() {}),
				TypeInformation.of(new TypeHint<ClientProofreadersList>() {}));

		cp.getResultStream().print();
		BroadcastStream<ClientProofreadersList> clientProofreadersListBroadcastStream = cp.getResultStream()
				.broadcast(clientProofreadersStateDescriptor);

		// A separate business operation - we will wait for new projects and send message to the last proofreader
		// who worked for the client of the new project.

		// First filter the events for my second business operation.
		DataStream<Event> newProjectStream = defaultSourceStream
				.filter((FilterFunction<Event>) event -> Objects.equals(EventType.PROJECT_CREATED, event.getEventType()))
				.uid("filter-events-for-new-projects")
				.name("Filter events for new projects");

		newProjectStream.print();

		// Now get the new projects, connect the client's proofreader list, and select the last proofreader who
		// worked for this same client in this language pair.
		SingleOutputStreamOperator<ClientProofreader> proofreadersPickedForProject = newProjectStream.keyBy("payload.projectId")
				.connect(clientProofreadersListBroadcastStream)
				.process(new InviteLastProofreader());

		// Get proofreaders selected for this project, as well as the messages to be sent out.
		proofreadersPickedForProject.print();
		DataStream<String> messagesToSend = proofreadersPickedForProject.getSideOutput(new OutputTag<String>("send-message") {});
		messagesToSend.print();

		// execute program
		System.out.println(env.getExecutionPlan());
		env.execute("Basic Flink job to collect and share client's previous proofreaders");
	}
}
