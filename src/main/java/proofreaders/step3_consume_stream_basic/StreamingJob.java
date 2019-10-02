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

package proofreaders.step3_consume_stream_basic;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import proofreaders.common.queue.entity.Event;

public class StreamingJob {

	public static void main(String[] args) throws java.lang.Exception {
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

		// A separate business operation - Do something with the business data "client's proofreaders"
		cp.getResultStream()
				.process(new DoSomething())
				.print("business with client proofreaders");

		// Yet another separate business operation - Do something with the business data "client's proofreaders"
		cp.getResultStream()
				.process(new DoSomething())
				.print("business 2 with client proofreaders");

		// execute program
		System.out.println(env.getExecutionPlan());
		env.execute("step3_consume_stream_basic");
	}
}
