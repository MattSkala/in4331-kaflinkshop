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

package kaflinkshop.Order;

import kaflinkshop.CommunicationFactory;
import kaflinkshop.JobParams;
import kaflinkshop.MessageKeySelector;
import kaflinkshop.SimpleJob;

public class OrderJob {
	public static void main(String[] args) throws Exception {
		JobParams<String> params = new JobParams<>();
		params.kafkaAddress = CommunicationFactory.KAFKA_DEFAULT_ADDRESS;
		params.inputTopic = CommunicationFactory.ORDER_IN_TOPIC;
		params.defaultOutputTopic = CommunicationFactory.ORDER_OUT_TOPIC;
		params.keySelector = new MessageKeySelector("order_id");
		params.processFunction = new OrderQueryProcess();
		params.attachDefaultProperties(CommunicationFactory.ZOOKEEPER_DEFAULT_ADDRESS);

		SimpleJob<String> job = new SimpleJob<>(params);
		job.execute();
	}
}


