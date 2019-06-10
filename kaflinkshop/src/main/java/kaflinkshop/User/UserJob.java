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

package kaflinkshop.User;

import kaflinkshop.CommunicationFactory;
import kaflinkshop.JobParams;
import kaflinkshop.SimpleJob;
import kaflinkshop.SimpleMessageKeyExtractor;

import java.io.IOException;

public class UserJob {
	public static void main(String[] args) throws IOException {
		JobParams params = new JobParams();
		params.kafkaAddress = CommunicationFactory.KAFKA_DEFAULT_ADDRESS;
		params.inputTopic = CommunicationFactory.USER_IN_TOPIC;
		params.defaultOutputTopic = CommunicationFactory.USER_OUT_TOPIC;
		params.keyExtractor = new SimpleMessageKeyExtractor(CommunicationFactory.PARAM_USER_ID);
		params.processFunction = new UserQueryProcess();
		params.attachDefaultProperties(CommunicationFactory.ZOOKEEPER_DEFAULT_ADDRESS);

		SimpleJob job = new SimpleJob(params);
		job.execute();
	}
}
