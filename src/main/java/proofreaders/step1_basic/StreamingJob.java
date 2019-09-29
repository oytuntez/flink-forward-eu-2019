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

package proofreaders.step1_basic;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import proofreaders.common.ClientProofreader;
import proofreaders.common.ClientProofreadersList;
import proofreaders.common.EventType;
import proofreaders.common.key.ClientProofreaderLanguagePairKeySelector;
import proofreaders.common.queue.entity.Event;
import proofreaders.common.queue.entity.GenericPayload;

import java.util.Objects;

public class StreamingJob {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// Source
		DataStream<Event> defaultSourceStream = ElementSource.buildSource(env);

		// Source stream filter
		DataStream<Event> stream = defaultSourceStream
				.filter((FilterFunction<Event>) event -> Objects.equals(EventType.PROOFREADER_TAKEN_PROJECT, event.getEventType()))
				.uid("filter-events-for-client-proofreaders")
				.name("Filter events for ClientProofreaders");

		// Source entity transformation to business entity
		DataStream<ClientProofreader> clientProofreaderStream = stream.process(
				new ProcessFunction<Event, ClientProofreader>() {
					@Override
					public void processElement(Event workerEvent, Context context, Collector<ClientProofreader> collector) throws Exception {
						GenericPayload proofreader = workerEvent.getPayload();
						Long clientId = proofreader.getClientId();
						collector.collect(new ClientProofreader(clientId, proofreader.getLanguagePair(), proofreader.getVendorId()));
					}
				}
		).uid("client-proofreader-stream").name("Client Proofreader Stream");

		// Core business operation - accumulate client's proofreaders
		DataStream<ClientProofreadersList> proofreaderStream = clientProofreaderStream
				.keyBy(new ClientProofreaderLanguagePairKeySelector())
				.window(EventTimeSessionWindows.withGap(Time.minutes(3)))
				.process(new ClientProofreadersAccumulator())
				.uid("client-proofreaders-accumulator")
				.name("Client Proofreaders Accumulator");

		// A separate business operation - Do something with the business data "client's proofreaders"
		SingleOutputStreamOperator<String> someResult = proofreaderStream.process(new DoSomething());
		someResult.print();

		// execute program
		System.out.println(env.getExecutionPlan());
		env.execute("Basic Flink job to collect and share client's previous proofreaders");
	}
}
