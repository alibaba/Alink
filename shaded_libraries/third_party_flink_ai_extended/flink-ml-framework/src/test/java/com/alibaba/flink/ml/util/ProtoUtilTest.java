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

package com.alibaba.flink.ml.util;


import com.alibaba.flink.ml.proto.NodeSpec;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ProtoUtilTest {
	@Test
	public void protoToJson() throws Exception {
		NodeSpec nodeSpec = NodeSpec.newBuilder().setClientPort(1).setIndex(2).setIp("127.0.0.1").build();
		String json = ProtoUtil.protoToJson(nodeSpec);
		NodeSpec.Builder builder = NodeSpec.newBuilder();
		ProtoUtil.jsonToProto(json, builder);
		assertEquals(nodeSpec, builder.build());
	}
}