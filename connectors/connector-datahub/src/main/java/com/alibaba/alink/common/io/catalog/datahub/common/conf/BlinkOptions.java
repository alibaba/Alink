/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.alink.common.io.catalog.datahub.common.conf;

import org.apache.flink.configuration.ConfigOption;

import java.util.Arrays;
import java.util.List;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * BlinkOptions.
 */
public class BlinkOptions {
	/**
	 * Aliyun STS options.
	 */
	public static class STS {
		// for sts endpoint, used for validation for all the connectors.
		public static final String INNER_STS_ENDPOINT = "__inner__blink_sts_endpoints__";

		public static final ConfigOption<String> STS_REGION_ID = key("stsRegionId".toLowerCase())
				.defaultValue("cn-shanghai");

		/** 当前 STS API 版本. */
		public static final ConfigOption<String> STS_API_VERSION = key("stsApiVersion".toLowerCase())
				.defaultValue("2015-04-01");

		public static final ConfigOption<String> STS_ACCESS_ID = key("stsAccessId".toLowerCase())
				.noDefaultValue();

		public static final ConfigOption<String> STS_ACCESS_KEY = key("stsAccesskey".toLowerCase())
				.noDefaultValue();

		public static final ConfigOption<String> STS_ROLE_ARN = key("roleArn".toLowerCase())
				.noDefaultValue();

		public static final ConfigOption<String> STS_UID = key("stsUid".toLowerCase())
				.noDefaultValue();
		public static final ConfigOption<String> BRS_ENDPOINT = key("brsEndPoint".toLowerCase())
				.noDefaultValue();

		/** 默认86400s. */
		public static final ConfigOption<Integer> STS_ROLEARN_UPDATE_SECONDS = key("stsUpdateSeconds".toLowerCase())
				.defaultValue(86400);

		public static final String STS_PARAMS_HELP_MSG = String.format(
				"required params:%s,%s,%s,%s\n\toptional params:%s,%s,%s",
				STS_ACCESS_ID,
				STS_ACCESS_KEY,
				STS_ROLE_ARN,
				STS_UID,
				STS_REGION_ID,
				STS_API_VERSION,
				STS_ROLEARN_UPDATE_SECONDS);

		public static final List<String> SUPPORTED_KEYS = Arrays.asList(
				STS_REGION_ID.key(),
				STS_API_VERSION.key(),
				STS_ACCESS_ID.key(),
				STS_ACCESS_KEY.key(),
				STS_ROLE_ARN.key(),
				STS_UID.key(),
				BRS_ENDPOINT.key(),
				STS_ROLEARN_UPDATE_SECONDS.key(),
				INNER_STS_ENDPOINT);
	}
}
