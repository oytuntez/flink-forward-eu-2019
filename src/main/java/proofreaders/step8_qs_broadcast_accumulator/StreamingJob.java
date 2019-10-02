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

package proofreaders.step8_qs_broadcast_accumulator;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.QueryableStateOptions;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import proofreaders.common.ClientProofreader;
import proofreaders.common.queue.entity.Event;

import static org.apache.flink.streaming.api.CheckpointingMode.AT_LEAST_ONCE;
import static org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;

public class StreamingJob {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		Configuration config = new Configuration();
		config.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
		config.setBoolean(QueryableStateOptions.ENABLE_QUERYABLE_STATE_PROXY_SERVER, true);
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.enableCheckpointing(15000);
		env.getCheckpointConfig().setCheckpointingMode(AT_LEAST_ONCE);// Exactly once requires exact ids
		env.getCheckpointConfig().setMinPauseBetweenCheckpoints(10000);
		env.getCheckpointConfig().setCheckpointTimeout(600000);
		env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
		env.getCheckpointConfig().enableExternalizedCheckpoints(RETAIN_ON_CANCELLATION);

		// Source
		DataStream<Event> defaultSourceStream = env.addSource(new StreamSource());

		// Our job for client's proofreaders information
		ClientProofreaders cp = new ClientProofreaders(defaultSourceStream);
		cp.run();

		// Invitation job. This will fork our default source and also consume broadcasted state clientProofreaders
		Invitation invitation = new Invitation(defaultSourceStream);
		invitation.run(cp.getBroadcastStream());
		// let's get the resulting stream of proofreaders who we have invited to the project
		DataStream<ClientProofreader> invitedProofreaders = invitation.getResultStream();
		invitedProofreaders.print("invitedProofreaders");

		// execute program
		System.out.println(env.getExecutionPlan());
		env.execute("step8_qs_broadcast_accumulator");
	}
}
