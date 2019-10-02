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

package proofreaders.step5_broadcast_leaky_improved;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import proofreaders.common.ClientLanguagePair;
import proofreaders.common.ClientProofreader;
import proofreaders.common.ClientProofreadersList;
import proofreaders.common.queue.entity.Event;

public class StreamingJob {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// Source
		DataStream<Event> defaultSourceStream = ElementSource.buildSource(env);

		// Our job for client's proofreaders information
		ClientProofreaders cp = new ClientProofreaders(defaultSourceStream);
		cp.run();

		// I don't want to use the resulting stream directly, but I want to broadcast it.
		// Now here I am leaking a good amount of information from ClientProofreaders...
		MapStateDescriptor<ClientLanguagePair, ClientProofreadersList> clientProofreadersStateDescriptor = new MapStateDescriptor<>(
				"clientProofreaders",
				TypeInformation.of(new TypeHint<ClientLanguagePair>() {}),
				TypeInformation.of(new TypeHint<ClientProofreadersList>() {}));

		// Broadcast the result of ClientProofreaders
		BroadcastStream<ClientProofreadersList> clientProofreadersListBroadcastStream = cp.getResultStream()
				.broadcast(clientProofreadersStateDescriptor);

		// Invitation job. This will fork our default source and also consume broadcasted state clientProofreaders
		Invitation invitation = new Invitation(defaultSourceStream, clientProofreadersListBroadcastStream);
		invitation.run();

		// let's get the resulting stream of proofreaders who we have invited to the project
		DataStream<ClientProofreader> invitedProofreaders = invitation.getResultStream();
		invitedProofreaders.print("invitedProofreaders");

		// execute program
		System.out.println(env.getExecutionPlan());
		env.execute("step5_broadcast_leaky_improved");
	}
}
