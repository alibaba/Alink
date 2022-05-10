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

package com.alibaba.alink.common.io.catalog.datahub.common.sts;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheLoader;
import org.apache.flink.shaded.guava18.com.google.common.cache.LoadingCache;

import com.alibaba.alink.common.io.catalog.datahub.common.conf.BlinkOptions;
import com.aliyuncs.sts.model.v20150401.AssumeRoleResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * A provider class that auto refresh STS temporary account.
 * You need implement produceNormalClient which connect remote server with normal aliyun account, and
 * implement produceStsClient which connect remote server with STS temporary account.
 */
public abstract class AbstractClientProvider<T> {
	private static Logger logger = LoggerFactory.getLogger(AbstractClientProvider.class);
	private String accessId = null;
	private String accessKey = null;
	private String stsRoleArn = null;
	private String stsAccessId = null;
	private String stsAccessKey = null;
	private String stsSessionName = null;
	private String stsAssumeRoleFor = null;
	private int stsExpireSeconds = 86400;
	private boolean useSts = false;
	private static LoadingCache<String, InnerStsIdentity> cacheArnResponse = null;
	private static long lastCertificationUpdateTime = 0;
	private static final Object mutex = new Object();
	protected T client;
	private long lastUpdateTime = 0;

	public AbstractClientProvider(String accessId, String accessKey) {
		this.accessId = accessId;
		this.accessKey = accessKey;
		useSts = false;
	}

	/**
	 * Internal class for sts identity loader.
	 */
	public static class StsIdentityLoader extends CacheLoader<String, InnerStsIdentity> {

		private String stsAccessId;
		private String stsAccessKey;
		private String stsRoleArn;
		private String stsSessionName;
		private String stsAssumeRoleFor;
		private Configuration properties;

		public StsIdentityLoader(AbstractClientProvider provider, Configuration properties) {
			this.stsAccessId = provider.stsAccessId;
			this.stsAccessKey = provider.stsAccessKey;
			this.stsRoleArn = provider.stsRoleArn;
			this.stsSessionName = provider.stsSessionName;
			this.stsAssumeRoleFor = provider.stsAssumeRoleFor;
			this.properties = new Configuration();
			this.properties.addAll(properties);
		}

		@Override
		public InnerStsIdentity load(String key) throws Exception {
			logger.info("getAssumeRole with para accessId " + stsAccessId + ", secretKey " +
					stsAccessKey + ", roleArn " + stsRoleArn +
					", stsSessionName " + stsSessionName);
			AssumeRoleResponse role = StsServiceRequest.assumeRoleWithServiceIdentity(
					stsAccessId, stsAccessKey, stsRoleArn, stsSessionName, stsAssumeRoleFor, properties);
				return new InnerStsIdentity(role.getCredentials().getAccessKeyId(),
						role.getCredentials().getAccessKeySecret(),
						role.getCredentials().getSecurityToken());
		}

	}

	public AbstractClientProvider(Configuration properties) {
		this.stsRoleArn = properties.getString(BlinkOptions.STS.STS_ROLE_ARN);
		this.stsAccessId = properties.getString(BlinkOptions.STS.STS_ACCESS_ID);
		this.stsAccessKey = properties.getString(BlinkOptions.STS.STS_ACCESS_KEY);
		this.stsAssumeRoleFor = properties.getString(BlinkOptions.STS.STS_UID);
		this.stsSessionName = String.valueOf(System.currentTimeMillis());
		this.stsExpireSeconds = properties.getInteger(BlinkOptions.STS.STS_ROLEARN_UPDATE_SECONDS);
		useSts = true;
		synchronized (mutex) {
			if (this.cacheArnResponse == null) {
				this.cacheArnResponse = CacheBuilder.newBuilder().concurrencyLevel(5).initialCapacity(1).maximumSize(3)
													.expireAfterWrite(stsExpireSeconds, TimeUnit.SECONDS).build(
								new StsIdentityLoader(this, properties));
			}
		}
	}

	protected T produceClient() {
		T client;
		if (useSts) {
			InnerStsIdentity role = null;
			try {
				lastUpdateTime = System.currentTimeMillis();
				role = cacheArnResponse.get(StsConstants.STS_ROLE_RESPONSE_KEY);
			} catch (ExecutionException e) {
				logger.info("catched ExecutionException, maybe too much call", e);
				throw new RuntimeException(e);
			}
			if (null == role) {
				throw new RuntimeException("failed to  get sts identify!");
			}
			client = produceStsClient(
					role.getAccessKeyId(),
					role.getAccessKeySecret(),
					role.getSecurityToken());
		} else {
			client = produceNormalClient(accessId, accessKey);
		}

		return client;
	}

	/**
	 * getClient with some option.
	 *
	 * @param forceReconnect If this param is true, provider will produce new client and aliyun account will not change.
	 * @param forceRefresh   If this param is true and current account is generated by STS, provider will request new STS account and produce
	 *                       new client. If this param is true and current account is normal account, provider will just produce new client.
	 * @return return client.
	 */
	public T getClient(boolean forceReconnect, boolean forceRefresh) {
		long nowTime = System.currentTimeMillis();
		if (useSts) {
				if (forceRefresh || lastCertificationUpdateTime == 0
						|| ((nowTime - lastCertificationUpdateTime) > (stsExpireSeconds * 1000))) {
					synchronized (mutex){
						if (forceRefresh || lastCertificationUpdateTime == 0
								|| ((nowTime - lastCertificationUpdateTime) > (stsExpireSeconds * 1000))) {
							lastCertificationUpdateTime = nowTime;
							cacheArnResponse.invalidate(StsConstants.STS_ROLE_RESPONSE_KEY);
						}
					}
					closeClient();
					client = produceClient();
					return client;
				} else if (forceReconnect || lastUpdateTime < lastCertificationUpdateTime ||
						client == null) {
					closeClient();
					client = produceClient();
					return client;
				} else {
					return client;
				}
		} else {
			if (forceRefresh || forceReconnect || client == null) {
				closeClient();
				client = produceClient();
				return client;
			} else {
				return client;
			}
		}
	}

	public T getClient(boolean forceReconnect) {
		return getClient(forceReconnect, false);
	}

	public T getClient() {
		return getClient(false, false);
	}

	protected void setClientNull() {
		client = null;
	}

	protected abstract void closeClient();

	protected abstract T produceNormalClient(String accessId, String accessKey);

	protected abstract T produceStsClient(String accessId, String accessKey, String securityToken);

	/**
	 * InnerStsIdentity.
	 */
	public static class InnerStsIdentity {
		public String accessKeyId;
		public String accessKeySecret;
		public String securityToken;
		public String expiration;
		public String roleArn;

		public InnerStsIdentity() {
		}

		public InnerStsIdentity(String accessKeyId, String accessKeySecret, String securityToken) {
			this.accessKeyId = accessKeyId;
			this.accessKeySecret = accessKeySecret;
			this.securityToken = securityToken;
		}

		public InnerStsIdentity setAccessKeyId(String accessKeyId) {
			this.accessKeyId = accessKeyId;
			return this;
		}

		public InnerStsIdentity setAccessKeySecret(String accessKeySecret) {
			this.accessKeySecret = accessKeySecret;
			return this;
		}

		public InnerStsIdentity setSecurityToken(String securityToken) {
			this.securityToken = securityToken;
			return this;
		}

		public InnerStsIdentity setExpiration(String expiration) {
			this.expiration = expiration;
			return this;
		}

		public InnerStsIdentity setRoleArn(String roleArn) {
			this.roleArn = roleArn;
			return this;
		}

		public String getAccessKeyId() {
			return accessKeyId;
		}

		public String getAccessKeySecret() {
			return accessKeySecret;
		}

		public String getSecurityToken() {
			return securityToken;
		}

		public String getExpiration() {
			return expiration;
		}

		public String getRoleArn() {
			return roleArn;
		}
	}
}
