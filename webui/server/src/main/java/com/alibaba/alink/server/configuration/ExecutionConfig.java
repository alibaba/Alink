package com.alibaba.alink.server.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "alink.execution")
public class ExecutionConfig {

	private String remoteClusterHost;

	private String remoteClusterPort;

	private Integer localParallelism = 1;

	public String getRemoteClusterHost() {
		return remoteClusterHost;
	}

	public ExecutionConfig setRemoteClusterHost(String remoteClusterHost) {
		this.remoteClusterHost = remoteClusterHost;
		return this;
	}

	public String getRemoteClusterPort() {
		return remoteClusterPort;
	}

	public ExecutionConfig setRemoteClusterPort(String remoteClusterPort) {
		this.remoteClusterPort = remoteClusterPort;
		return this;
	}

	public Integer getLocalParallelism() {
		return localParallelism;
	}

	public ExecutionConfig setLocalParallelism(Integer localParallelism) {
		this.localParallelism = localParallelism;
		return this;
	}
}
