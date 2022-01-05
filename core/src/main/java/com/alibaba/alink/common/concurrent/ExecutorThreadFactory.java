package com.alibaba.alink.common.concurrent;

import java.lang.Thread.UncaughtExceptionHandler;

public class ExecutorThreadFactory extends org.apache.flink.runtime.util.ExecutorThreadFactory {

	private static final String DEFAULT_POOL_NAME = "alink-executor-pool";

	public ExecutorThreadFactory() {
		this(DEFAULT_POOL_NAME);
	}

	public ExecutorThreadFactory(String poolName) {
		super(poolName);
	}

	public ExecutorThreadFactory(String poolName, UncaughtExceptionHandler exceptionHandler) {
		super(poolName, exceptionHandler);
	}
}
