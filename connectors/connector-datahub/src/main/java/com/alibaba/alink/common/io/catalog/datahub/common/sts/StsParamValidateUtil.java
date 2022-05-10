/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.alink.common.io.catalog.datahub.common.sts;

import org.apache.flink.configuration.Configuration;

import com.alibaba.alink.common.io.catalog.datahub.common.conf.BlinkOptions;
import com.alibaba.alink.common.io.catalog.datahub.common.exception.NotEnoughParamsException;
import com.alibaba.alink.common.io.catalog.datahub.common.util.BlinkStringUtil;

/**
 * StsParamValidateUtil.
 */
public class StsParamValidateUtil {

	public static String stsValidate(String accessId, String accessKey, String localErrorMsg, Configuration properties) {
		String stsRoleArn = properties.getString(BlinkOptions.STS.STS_ROLE_ARN);
		String stsAccessId = properties.getString(BlinkOptions.STS.STS_ACCESS_ID);
		String stsAccessKey = properties.getString(BlinkOptions.STS.STS_ACCESS_KEY);
		String stsUid = properties.getString(BlinkOptions.STS.STS_UID);
		if (BlinkStringUtil.isNotEmpty(accessId, accessKey) || BlinkStringUtil.isNotEmpty(stsRoleArn, stsAccessId, stsAccessKey, stsUid)){
			return null;
		} else if (properties.containsKey(BlinkOptions.STS.STS_ROLE_ARN.key())){
			throw new NotEnoughParamsException(String.format("Lack necessary arguments: {0}", BlinkOptions.STS.STS_PARAMS_HELP_MSG));
		} else {
			throw new NotEnoughParamsException(localErrorMsg);
		}
	}
}
