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

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.util.JsonFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * proto class helper function
 */
public class ProtoUtil {

	private static final Logger LOG = LoggerFactory.getLogger(PythonUtil.class);

	/**
	 * convert proto message to string
	 * @param builder proto message
	 * @return proto message json string
	 */
	public static String protoToJson(MessageOrBuilder builder) {
		String json;
		try {
			json = JsonFormat.printer().print(builder);
		} catch (InvalidProtocolBufferException e) {
			LOG.error("Cannot convert proto to json", e);
			return "";
		}
		return json;
	}

	/**
	 * convert json string to proto message object
	 * @param json a proto message json string
	 * @param builder proto message builder
	 */
	public static void jsonToProto(String json, Message.Builder builder) {
		try {
			JsonFormat.parser().merge(json, builder);
		} catch (InvalidProtocolBufferException e) {
			LOG.error("Cannot convert json to proto", e);
		}
	}
}
