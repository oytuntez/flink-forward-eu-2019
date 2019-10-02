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

package proofreaders.step3_consume_stream;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import proofreaders.common.ClientProofreader;
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

		// My first business capsule
		//// EVOLUTION: we now decided the concept of "client's proofreaders" is quite useful in other workflows as well.
		////			this first required us to hide the implementation details of the concept "client's proofreader".
		ClientProofreaders cp = new ClientProofreaders(defaultSourceStream);
		cp.run();

		// First filter the events for my second business operation.
		DataStream<Event> newProjectStream = defaultSourceStream
				.filter((FilterFunction<Event>) event -> Objects.equals(EventType.PROJECT_CREATED, event.getEventType()))
				.uid("filter-events-for-new-projects")
				.name("Filter events for new projects");

		newProjectStream.print();

		// A separate business operation - Do something with the business data "client's proofreaders"
		SingleOutputStreamOperator<ClientProofreader> proofreadersPickedForProject = newProjectStream.keyBy("payload.projectId")
				.connect(cp.getResultStream())
				.process(new InviteLastProofreader())
				.uid("invite-last-proofreader")
				.name("invite last proofreader");

		proofreadersPickedForProject.print("proofreadersPickedForProject");

		// execute program
		System.out.println(env.getExecutionPlan());
		env.execute("step3_consume_stream");
	}
}
