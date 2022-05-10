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

import com.aliyuncs.http.ProtocolType;

/**
 * StsConstants.
 */
public class StsConstants {
	// https
	public static final ProtocolType PROTOCOL_TYPE = ProtocolType.HTTPS;
	// duration
	public static final Long DURATION = 129600L;

	public static final String STS_SECRET_KEY = "xxx";

	public static final String STS_ROLE_RESPONSE_KEY = "roleResponseKey";
}
