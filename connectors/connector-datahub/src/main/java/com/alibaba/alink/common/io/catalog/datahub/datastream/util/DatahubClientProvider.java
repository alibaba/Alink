/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.alink.common.io.catalog.datahub.datastream.util;

import org.apache.flink.configuration.Configuration;

import com.alibaba.alink.common.io.catalog.datahub.common.sts.AbstractClientProvider;
import com.aliyun.datahub.client.DatahubClient;
import com.aliyun.datahub.client.DatahubClientBuilder;
import com.aliyun.datahub.client.auth.AliyunAccount;
import com.aliyun.datahub.client.common.DatahubConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DatahubClientProvider.
 */
public class DatahubClientProvider extends AbstractClientProvider <DatahubClient> {
	private static Logger logger = LoggerFactory.getLogger(DatahubClientProvider.class);
	private String endpoint;
	private boolean enablePb = false;

	public DatahubClientProvider(String endpoint, String accessId, String accessKey){
		super(accessId, accessKey);
		this.endpoint = endpoint;
	}

	public DatahubClientProvider(String endpoint,
								Configuration properties){
		super(properties);
		this.endpoint = endpoint;
	}

	@Override
	protected DatahubClient produceNormalClient(String accessId, String accessKey) {
		AliyunAccount account = new AliyunAccount(accessId, accessKey);
		DatahubConfig conf = new DatahubConfig(endpoint, account, enablePb);
		return DatahubClientBuilder.newBuilder().setDatahubConfig(conf).build();
	}

	@Override
	protected DatahubClient produceStsClient(String accessId, String accessKey, String securityToken) {
		AliyunAccount account = new AliyunAccount(accessId, accessKey, securityToken);
		DatahubConfig conf = new DatahubConfig(endpoint, account, enablePb);
		return DatahubClientBuilder.newBuilder().setDatahubConfig(conf).build();
	}

	@Override
	protected void closeClient() { }
}
